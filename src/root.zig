//! zimit — GCRA-based rate limiter for Zig.
//!
//! Quick start:
//!
//!     const std = @import("std");
//!     const zimit = @import("zimit");
//!
//! pub fn main(init: std.process.Init) !void {
//!     var gpa = std.heap.GeneralPurposeAllocator(.{}){};
//!     defer _ = gpa.deinit();
//!
//!     const sys_clock = zimit.SystemClock.init(init.io);
//!     var limiter = try zimit.RateLimiter([]const u8, zimit.SystemClock).init(.{
//!         .allocator  = gpa.allocator(),
//!         .rate       = 100,          // 100 requests …
//!         .per        = .second,      // … per second
//!         .burst      = 20,           // allow up to 20 extra in a burst
//!         .clock      = sys_clock,
//!     });
//!     defer limiter.deinit();
//!
//!     switch (try limiter.allow("192.168.1.1")) {
//!         .allowed => handleRequest(),
//!         .denied  => |d| return error429(d.retry_after_ms_ceil()),
//!     }

const std = @import("std");
const gcra = @import("gcra.zig");
const types = @import("types.zig");

// ── Re-exports (callers only need to import "zimit") ─────────────────────────

const Limit = types.Limit;
pub const SystemClock = types.SystemClock;
pub const ManualClock = types.ManualClock;
pub const ZimitError = types.ZimitError;

// ── Period enum ───────────────────────────────────────────────────────────────

/// Human-readable time unit for `RateLimiterConfig`.
pub const Period = enum {
    second,
    minute,
    hour,

    /// Converts the period to nanoseconds.
    pub fn to_ns(self: Period) i64 {
        return switch (self) {
            .second => std.time.ns_per_s,
            .minute => 60 * std.time.ns_per_s,
            .hour => 3600 * std.time.ns_per_s,
        };
    }
};

// ── Config ────────────────────────────────────────────────────────────────────

/// Configuration for `RateLimiter.init`.
pub fn RateLimiterConfig(comptime K: type, comptime ClockType: type) type {
    _ = K; // keeps the type parameter meaningful for future fields
    return struct {
        allocator: std.mem.Allocator,
        /// How many requests are allowed per `per`.
        rate: u32,
        /// The time window.
        per: Period,
        /// Extra requests allowed in a burst on top of the base rate.
        /// 0 means no burst — every request must wait its full slot.
        burst: u32 = 0,
        /// Time source. Use `SystemClock` in production, `ManualClock` in tests.
        clock: ClockType,
    };
}

// ── Outcome ───────────────────────────────────────────────────────────────────

/// What the caller receives from `allow` / `allow_n`.
/// Richer than a plain bool — carries the wait time so callers
/// can implement backoff, 429 headers, or fiber suspension.
pub const Outcome = union(enum) {
    allowed,
    denied: struct {
        /// How long to wait before retrying, in nanoseconds.
        retry_after_ns: i64,

        /// Convenience: retry delay in whole milliseconds (rounded up).
        pub fn retry_after_ms_ceil(self: @This()) i64 {
            return @divTrunc(self.retry_after_ns + 999_999, 1_000_000);
        }
    },

    /// Returns true if the outcome was allowed.
    pub fn is_allowed(self: Outcome) bool {
        return self == .allowed;
    }
};

// ── GlobalLimiter ─────────────────────────────────────────────────────────────

/// A lock-free single-key rate limiter with a token-bucket-flavored API.
///
/// Use this for process-wide or service-wide limits that are shared across
/// threads — for example, "this service may make at most N outbound calls/s".
///
/// For per-key limits use `RateLimiter(K)`.
pub fn GlobalLimiter(comptime ClockType: type) type {
    return struct {
        const Self = @This();
        const Inner = gcra.AtomicLimiter(ClockType);

        inner: Inner,

        /// Initialise a global limiter.
        pub fn init(cfg: struct {
            rate: u32,
            per: Period,
            burst: u32 = 0,
            clock: ClockType,
        }) ZimitError!Self {
            const limit = Limit{
                .count = cfg.rate,
                .period_ns = cfg.per.to_ns(),
            };
            return .{ .inner = try Inner.init(limit, cfg.burst, cfg.clock) };
        }

        /// Convenience for `allow_n(1)`.
        pub fn allow(self: *Self) Outcome {
            return self.allow_n(1);
        }

        /// Atomically consume `n` slots.
        pub fn allow_n(self: *Self, n: u32) Outcome {
            return switch (self.inner.allow_n(n)) {
                .allowed => .allowed,
                .denied => |d| .{ .denied = .{ .retry_after_ns = d.retry_after_ns } },
            };
        }

        /// Block the calling thread until allowed.
        /// Same design seam as `RateLimiter.wait` — replace with fiber suspension
        /// once Zig 0.16.0 async lands.
        pub fn wait(self: *Self, io: std.Io) !void {
            while (true) {
                switch (self.allow()) {
                    .allowed => return,
                    .denied => |d| try std.Io.sleep(io, std.Io.Duration.fromNanoseconds(d.retry_after_ns), .awake),
                }
            }
        }

        /// Resets the limiter to its initial state.
        pub fn reset(self: *Self) void {
            self.inner.reset();
        }
    };
}

// ── RateLimiter ───────────────────────────────────────────────────────────────

/// A token-bucket-flavored rate limiter backed by a GCRA engine.
///
/// `K` is the key type — typically `[]const u8` (IP, username) or an integer
/// (user ID, session ID). All keys are isolated; one key's limit does not
/// affect another's.
///
/// Thread safety: none. Wrap with a mutex if shared across threads.
pub fn RateLimiter(comptime K: type, comptime ClockType: type) type {
    return struct {
        const Self = @This();
        const Inner = gcra.Limiter(K, ClockType);

        inner: Inner,

        /// Create a new limiter from a `RateLimiterConfig`.
        pub fn init(cfg: RateLimiterConfig(K, ClockType)) ZimitError!Self {
            const limit = Limit{
                .count = cfg.rate,
                .period_ns = cfg.per.to_ns(),
            };
            return .{
                .inner = try Inner.init(cfg.allocator, limit, cfg.burst, cfg.clock),
            };
        }

        /// Releases all memory owned by the limiter.
        pub fn deinit(self: *Self) void {
            self.inner.deinit();
        }

        /// Check whether `key` may make one request right now.
        ///
        /// On `.allowed` the internal state is updated immediately.
        /// On `.denied` no state changes — the caller should wait
        /// `retry_after_ns` before calling again.
        pub fn allow(self: *Self, key: K) ZimitError!Outcome {
            return self.allow_n(key, 1);
        }

        /// Check whether `key` may make `n` requests atomically.
        ///
        /// All `n` slots are consumed together or none are — there is no
        /// partial allowance. Useful for batch jobs or chunked uploads.
        pub fn allow_n(self: *Self, key: K, n: u32) ZimitError!Outcome {
            return switch (try self.inner.check_key_n(key, n)) {
                .allowed => .allowed,
                .denied => |d| .{ .denied = .{ .retry_after_ns = d.retry_after_ns } },
            };
        }

        /// Block the calling thread until `key` is allowed.
        ///
        /// This is the simple synchronous wait. For async contexts, use
        /// `allow` and handle the `retry_after_ns` yourself.
        pub fn wait(self: *Self, io: std.Io, key: K) !void {
            while (true) {
                const outcome = try self.allow(key);
                switch (outcome) {
                    .allowed => return,
                    .denied => |d| try std.Io.sleep(io, std.Io.Duration.fromNanoseconds(d.retry_after_ns), .awake),
                }
            }
        }

        /// Remove a key from the store — useful when a session ends and you
        /// want to reclaim memory rather than wait for the TAT to age out.
        pub fn remove(self: *Self, key: K) void {
            self.inner.remove(key);
        }

        /// Number of keys currently tracked.
        pub fn key_count(self: *const Self) usize {
            return self.inner.key_count();
        }
    };
}

/// Convenience alias — the overwhelmingly common case.
pub fn StringRateLimiter(comptime ClockType: type) type {
    return RateLimiter([]const u8, ClockType);
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

fn makeLimiter(rate: u32, per: Period, burst: u32, mc: *ManualClock) !StringRateLimiter(*ManualClock) {
    return StringRateLimiter(*ManualClock).init(.{
        .allocator = std.testing.allocator,
        .rate = rate,
        .per = per,
        .burst = burst,
        .clock = mc,
    });
}

test "Period.to_ns: values are correct" {
    try std.testing.expectEqual(std.time.ns_per_s, Period.second.to_ns());
    try std.testing.expectEqual(60 * std.time.ns_per_s, Period.minute.to_ns());
    try std.testing.expectEqual(3600 * std.time.ns_per_s, Period.hour.to_ns());
}

test "RateLimiter: allow — fresh key passes" {
    var mc = ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try makeLimiter(10, .second, 0, &mc);
    defer lim.deinit();

    const out = try lim.allow("alice");
    try std.testing.expect(out.is_allowed());
}

test "RateLimiter: allow — exhausted key is denied" {
    var mc = ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try makeLimiter(3, .second, 0, &mc);
    defer lim.deinit();

    _ = try lim.allow("u");
    _ = try lim.allow("u");
    _ = try lim.allow("u");
    const out = try lim.allow("u");
    try std.testing.expect(!out.is_allowed());
}

test "RateLimiter: allow — retry_after_ms_ceil rounds up" {
    var mc = ManualClock{};
    mc.set(std.time.ns_per_s);
    // 1 req/s → emission interval = 1 000 000 000 ns = 1000 ms
    var lim = try makeLimiter(1, .second, 0, &mc);
    defer lim.deinit();

    _ = try lim.allow("u");
    const out = try lim.allow("u");
    switch (out) {
        .denied => |d| {
            // retry_after_ns should be ~1s; ms should round up to 1000
            try std.testing.expect(d.retry_after_ns > 0);
            try std.testing.expectEqual(@as(i64, 1000), d.retry_after_ms_ceil());
        },
        .allowed => return error.TestUnexpectedResult,
    }
}

test "RateLimiter: allow — keys are isolated" {
    var mc = ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try makeLimiter(1, .second, 0, &mc);
    defer lim.deinit();

    _ = try lim.allow("alice");
    const bob = try lim.allow("bob");
    try std.testing.expect(bob.is_allowed());
}

test "RateLimiter: allow — time advance unblocks key" {
    var mc = ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try makeLimiter(1, .second, 0, &mc);
    defer lim.deinit();

    _ = try lim.allow("u");
    try std.testing.expect(!(try lim.allow("u")).is_allowed());

    mc.tick(std.time.ns_per_s);
    try std.testing.expect((try lim.allow("u")).is_allowed());
}

test "RateLimiter: burst — allows base+burst requests at t=0" {
    var mc = ManualClock{};
    mc.set(std.time.ns_per_s);
    // rate=5/s, burst=3 → 1 base + 3 burst = 4 requests immediately
    var lim = try makeLimiter(5, .second, 3, &mc);
    defer lim.deinit();

    var i: usize = 0;
    while (i < 4) : (i += 1) {
        const out = try lim.allow("u");
        try std.testing.expectEqual(true, out.is_allowed());
    }
    try std.testing.expectEqual(false, (try lim.allow("u")).is_allowed());
}

test "RateLimiter: burst — replenishes after delay" {
    var mc = ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try makeLimiter(1, .second, 1, &mc);
    defer lim.deinit();

    // Consume both base + burst
    _ = try lim.allow("u");
    _ = try lim.allow("u");
    try std.testing.expect(!(try lim.allow("u")).is_allowed());

    // One second later, one slot has replenished
    mc.tick(std.time.ns_per_s);
    try std.testing.expect((try lim.allow("u")).is_allowed());
}

test "RateLimiter: allow_n — consume multiple slots atomically" {
    var mc = ManualClock{};
    mc.set(std.time.ns_per_s);
    // rate=10/s, no burst → 10 slots available at t=0
    var lim = try makeLimiter(10, .second, 0, &mc);
    defer lim.deinit();

    // Consume 7 — succeeds
    try std.testing.expectEqual(true, (try lim.allow_n("u", 7)).is_allowed());
    // Only 3 remain — requesting 4 fails
    try std.testing.expectEqual(false, (try lim.allow_n("u", 4)).is_allowed());
    // 3 remaining still there — fails too, TAT is already 700ms out
    // and no burst to cover the gap for a further n=3 at t=0
    try std.testing.expectEqual(false, (try lim.allow_n("u", 3)).is_allowed());
}

test "RateLimiter: allow_n — n=0 always allowed without state change" {
    var mc = ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try makeLimiter(1, .second, 0, &mc);
    defer lim.deinit();

    // Exhaust the key
    _ = try lim.allow("u");
    try std.testing.expect(!(try lim.allow("u")).is_allowed());

    // n=0 should still return allowed and not mutate state
    try std.testing.expect((try lim.allow_n("u", 0)).is_allowed());
    try std.testing.expect(!(try lim.allow("u")).is_allowed());
}

test "RateLimiter: allow_n — partial batch is never granted" {
    var mc = ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try makeLimiter(5, .second, 0, &mc);
    defer lim.deinit();

    // Consume 3 slots atomically — succeeds, TAT now 600ms out
    try std.testing.expectEqual(true, (try lim.allow_n("u", 3)).is_allowed());

    // Request 10 more — way over limit, must fail
    try std.testing.expectEqual(false, (try lim.allow_n("u", 10)).is_allowed());

    // Advance time by 600ms — exactly the 3 slots we consumed
    mc.tick(600 * std.time.ns_per_ms);

    // TAT is now at 1600ms, time is at 1600ms — key is fresh again.
    // If allow_n had partially mutated state on the failed n=10 attempt,
    // the TAT would be further ahead and this would fail.
    try std.testing.expectEqual(true, (try lim.allow_n("u", 5)).is_allowed());
}

test "RateLimiter: remove — resets key to fresh" {
    var mc = ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try makeLimiter(1, .second, 0, &mc);
    defer lim.deinit();

    _ = try lim.allow("u");
    try std.testing.expect(!(try lim.allow("u")).is_allowed());

    lim.remove("u");
    try std.testing.expectEqual(@as(usize, 0), lim.key_count());
    try std.testing.expect((try lim.allow("u")).is_allowed());
}

test "RateLimiter: per minute config" {
    var mc = ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try makeLimiter(60, .minute, 0, &mc);
    defer lim.deinit();

    // 60/min = 1/s — second request at same instant denied
    _ = try lim.allow("u");
    try std.testing.expect(!(try lim.allow("u")).is_allowed());

    // Advance 1 second → allowed again
    mc.tick(std.time.ns_per_s);
    try std.testing.expect((try lim.allow("u")).is_allowed());
}

test "RateLimiter: integer key type (u64)" {
    var mc = ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try RateLimiter(u64, ManualClock).init(.{
        .allocator = std.testing.allocator,
        .rate = 5,
        .per = .second,
        .burst = 0,
        .clock = mc,
    });
    defer lim.deinit();

    try std.testing.expect((try lim.allow(1001)).is_allowed());
    try std.testing.expect((try lim.allow(1002)).is_allowed());
    // Same key, second request — denied
    try std.testing.expect(!(try lim.allow_n(1001, 5)).is_allowed());
}

test "RateLimiter: sustained throughput over simulated minute" {
    var mc = ManualClock{};
    var lim = try makeLimiter(100, .second, 0, &mc);
    defer lim.deinit();

    var allowed: usize = 0;
    var t: i64 = 0;
    // 60 seconds, one attempt every 5ms (12 000 attempts total)
    while (t < 60 * std.time.ns_per_s) : (t += 5_000_000) {
        mc.set(t);
        if ((try lim.allow("u")).is_allowed()) allowed += 1;
    }
    // Expect exactly 6 000 allowed (100/s × 60s)
    try std.testing.expectEqual(@as(usize, 6_000), allowed);
}

test "RateLimiter: allow_n overflow guard denies without panic" {
    var mc = ManualClock{};
    mc.set(std.time.ns_per_s);
    // per=minute, rate=1 → interval=60_000_000_000 ns → max_batch=153
    // maxInt(u32)=4_294_967_295 >> 153, so guard fires
    var lim = try makeLimiter(1, .minute, 0, &mc);
    defer lim.deinit();

    const out = try lim.allow_n("u", std.math.maxInt(u32));
    try std.testing.expect(!out.is_allowed());
}

test "RateLimiter: allow_n overflow guard returns maxInt retry_after" {
    var mc = ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try makeLimiter(1, .minute, 0, &mc);
    defer lim.deinit();

    const out = try lim.allow_n("u", std.math.maxInt(u32));
    switch (out) {
        .denied => |d| try std.testing.expectEqual(
            @as(i64, std.math.maxInt(i64)),
            d.retry_after_ns,
        ),
        .allowed => return error.TestUnexpectedResult,
    }
}

test "RateLimiter: allow_n overflow guard does not mutate state" {
    var mc = ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try makeLimiter(1, .minute, 0, &mc);
    defer lim.deinit();

    _ = try lim.allow_n("u", std.math.maxInt(u32));

    const out = try lim.allow("u");
    try std.testing.expect(out.is_allowed());
}

test "RateLimiter: allow_n large but valid n is evaluated normally" {
    var mc = ManualClock{};
    mc.set(std.time.ns_per_s);
    // rate=10/s → interval=100ms → max_batch=92, so n=5 is valid
    var lim = try makeLimiter(10, .second, 0, &mc);
    defer lim.deinit();

    const out = try lim.allow_n("u", 5);
    switch (out) {
        .allowed => {},
        .denied => |d| try std.testing.expect(d.retry_after_ns < std.math.maxInt(i64)),
    }
}

test "RateLimiter: allow_n — n=1 and allow are equivalent" {
    var mc = ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim_a = try makeLimiter(5, .second, 0, &mc);
    defer lim_a.deinit();
    var lim_b = try makeLimiter(5, .second, 0, &mc);
    defer lim_b.deinit();

    var i: usize = 0;
    while (i < 6) : (i += 1) {
        const a = try lim_a.allow("u");
        const b = try lim_b.allow_n("u", 1);
        try std.testing.expectEqual(a.is_allowed(), b.is_allowed());
    }
}

test "RateLimiter: remove on absent key is safe" {
    var mc = ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try makeLimiter(5, .second, 0, &mc);
    defer lim.deinit();

    lim.remove("ghost");
    try std.testing.expectEqual(@as(usize, 0), lim.key_count());
}

test "RateLimiter: key_count after mixed allow and remove" {
    var mc = ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try makeLimiter(10, .second, 0, &mc);
    defer lim.deinit();

    _ = try lim.allow("a");
    _ = try lim.allow("b");
    _ = try lim.allow("c");
    try std.testing.expectEqual(@as(usize, 3), lim.key_count());

    lim.remove("b");
    try std.testing.expectEqual(@as(usize, 2), lim.key_count());

    lim.remove("b"); // second remove is safe
    try std.testing.expectEqual(@as(usize, 2), lim.key_count());
}

test "RateLimiter: retry_after_ns is positive on denial" {
    var mc = ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try makeLimiter(1, .second, 0, &mc);
    defer lim.deinit();

    _ = try lim.allow("u");
    const out = try lim.allow("u");
    switch (out) {
        .denied => |d| try std.testing.expect(d.retry_after_ns > 0),
        .allowed => return error.TestUnexpectedResult,
    }
}

test "RateLimiter: retry_after_ns decreases as time advances" {
    var mc = ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try makeLimiter(1, .second, 0, &mc);
    defer lim.deinit();

    _ = try lim.allow("u");

    const out1 = try lim.allow("u");
    const wait1 = switch (out1) {
        .denied => |d| d.retry_after_ns,
        .allowed => return error.TestUnexpectedResult,
    };

    mc.tick(std.time.ns_per_s / 2);

    const out2 = try lim.allow("u");
    const wait2 = switch (out2) {
        .denied => |d| d.retry_after_ns,
        .allowed => return error.TestUnexpectedResult,
    };

    try std.testing.expect(wait2 < wait1);
}

test "RateLimiter: wait blocks and succeeds" {
    const sys = SystemClock.init(std.testing.io);
    var lim = try RateLimiter(u32, SystemClock).init(.{
        .allocator = std.testing.allocator,
        .rate = 10,
        .per = .second, // 100ms per slot
        .clock = sys,
    });
    defer lim.deinit();

    // Exhaust key 42
    var i: usize = 0;
    while (i < 10) : (i += 1) {
        _ = try lim.allow(42);
    }

    const start = std.Io.Timestamp.now(std.testing.io, .real).toMilliseconds();
    try lim.wait(std.testing.io, 42);
    const end = std.Io.Timestamp.now(std.testing.io, .real).toMilliseconds();

    try std.testing.expect(end - start >= 50);
}

test "RateLimiter: stress — 10k unique keys" {
    const mc = ManualClock{};
    var lim = try RateLimiter(u32, ManualClock).init(.{
        .allocator = std.testing.allocator,
        .rate = 1,
        .per = .hour,
        .clock = mc,
    });
    defer lim.deinit();

    var i: u32 = 0;
    while (i < 10_000) : (i += 1) {
        try std.testing.expect((try lim.allow(i)).is_allowed());
    }
    try std.testing.expectEqual(@as(usize, 10_000), lim.key_count());

    // Second pass — all must be denied (rate is 1/hour)
    i = 0;
    while (i < 10_000) : (i += 1) {
        try std.testing.expect(!(try lim.allow(i)).is_allowed());
    }
}

test "RateLimiter: init rejects zero rate" {
    const mc = ManualClock{};
    const result = StringRateLimiter(ManualClock).init(.{
        .allocator = std.testing.allocator,
        .rate = 0,
        .per = .second,
        .burst = 0,
        .clock = mc,
    });
    try std.testing.expectError(error.InvalidLimit, result);
}

test "RateLimiter: init rejects rate > 1 req/ns" {
    const mc = ManualClock{};
    // per = .second (= 1_000_000_000 ns)
    // rate = 2_000_000_000 > 1_000_000_000
    const result = StringRateLimiter(ManualClock).init(.{
        .allocator = std.testing.allocator,
        .rate = 2_000_000_000,
        .per = .second,
        .burst = 0,
        .clock = mc,
    });
    try std.testing.expectError(error.RateExceedsRes, result);
}

test "RateLimiter: per-hour config with burst and time advance" {
    var mc = ManualClock{};
    mc.set(std.time.ns_per_s);
    // 1 req/hour, burst=1 → 2 immediate requests
    var lim = try StringRateLimiter(*ManualClock).init(.{
        .allocator = std.testing.allocator,
        .rate = 1,
        .per = .hour,
        .burst = 1,
        .clock = &mc,
    });
    defer lim.deinit();

    // 2 requests pass (1 base + 1 burst)
    try std.testing.expect((try lim.allow("u")).is_allowed());
    try std.testing.expect((try lim.allow("u")).is_allowed());
    // Third denied
    try std.testing.expect(!(try lim.allow("u")).is_allowed());

    // Advance 1 hour → 1 slot replenished
    mc.tick(3600 * std.time.ns_per_s);
    try std.testing.expect((try lim.allow("u")).is_allowed());
}

test "RateLimiter: allow_n with n=0 on exhausted key returns allowed" {
    var mc = ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try makeLimiter(1, .second, 0, &mc);
    defer lim.deinit();

    _ = try lim.allow("u"); // exhaust
    try std.testing.expect(!(try lim.allow("u")).is_allowed()); // confirm exhausted
    try std.testing.expect((try lim.allow_n("u", 0)).is_allowed()); // n=0 still allowed
}

test "RateLimiter: multiple keys with different burst behavior" {
    var mc = ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try makeLimiter(1, .second, 2, &mc);
    defer lim.deinit();

    // key "a" uses all burst: 3 requests (1 base + 2 burst)
    try std.testing.expect((try lim.allow("a")).is_allowed());
    try std.testing.expect((try lim.allow("a")).is_allowed());
    try std.testing.expect((try lim.allow("a")).is_allowed());
    try std.testing.expect(!(try lim.allow("a")).is_allowed());

    // key "b" is independent — still has full burst capacity
    try std.testing.expect((try lim.allow("b")).is_allowed());
    try std.testing.expect((try lim.allow("b")).is_allowed());
    try std.testing.expect((try lim.allow("b")).is_allowed());
    try std.testing.expect(!(try lim.allow("b")).is_allowed());
}

test "RateLimiter: StringRateLimiter type alias works" {
    var mc = ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try StringRateLimiter(ManualClock).init(.{
        .allocator = std.testing.allocator,
        .rate = 5,
        .per = .second,
        .burst = 0,
        .clock = mc,
    });
    defer lim.deinit();

    try std.testing.expect((try lim.allow("test")).is_allowed());
}

test "GlobalLimiter: concurrent contention" {
    const num_threads = 4;
    const total_slots = 1000;

    const sys = SystemClock.init(std.testing.io);
    var lim = try GlobalLimiter(SystemClock).init(.{
        .rate = total_slots,
        .per = .hour,
        .burst = total_slots - 1,
        .clock = sys,
    });

    const Ctx = struct {
        limiter: *GlobalLimiter(SystemClock),
        allowed: std.atomic.Value(usize),

        fn run(ctx: *@This()) void {
            while (true) {
                if (ctx.limiter.allow().is_allowed()) {
                    _ = ctx.allowed.fetchAdd(1, .monotonic);
                } else break;
            }
        }
    };

    var ctx = Ctx{
        .limiter = &lim,
        .allowed = std.atomic.Value(usize).init(0),
    };

    var threads: [num_threads]std.Thread = undefined;
    for (&threads) |*t| {
        t.* = try std.Thread.spawn(.{}, Ctx.run, .{&ctx});
    }
    for (&threads) |*t| t.join();

    try std.testing.expectEqual(@as(usize, total_slots), ctx.allowed.load(.monotonic));
}

test "GlobalLimiter: basic allow and deny" {
    var mc = ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try GlobalLimiter(ManualClock).init(.{
        .rate = 5,
        .per = .second,
        .burst = 4, // 1 base + 4 burst = 5
        .clock = mc,
    });

    var i: usize = 0;
    while (i < 5) : (i += 1) {
        try std.testing.expectEqual(true, lim.allow().is_allowed());
    }
    try std.testing.expectEqual(false, lim.allow().is_allowed());
}

test "GlobalLimiter: reset restores capacity" {
    var mc = ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try GlobalLimiter(ManualClock).init(.{
        .rate = 1,
        .per = .second,
        .burst = 0,
        .clock = mc,
    });

    _ = lim.allow();
    try std.testing.expectEqual(false, lim.allow().is_allowed());
    lim.reset();
    try std.testing.expectEqual(true, lim.allow().is_allowed());
}

test "GlobalLimiter: wait blocks and eventually succeeds" {
    const sys = SystemClock.init(std.testing.io);
    var lim = try GlobalLimiter(SystemClock).init(.{
        .rate = 10,
        .per = .second, // 100ms per slot
        .burst = 0,
        .clock = sys,
    });

    // Exhaust immediately
    var i: usize = 0;
    while (i < 10) : (i += 1) {
        _ = lim.allow();
    }
    try std.testing.expectEqual(false, lim.allow().is_allowed());

    const start = std.Io.Timestamp.now(std.testing.io, .real).toMilliseconds();
    try lim.wait(std.testing.io); // should block for roughly 100ms
    const end = std.Io.Timestamp.now(std.testing.io, .real).toMilliseconds();

    try std.testing.expect(end - start >= 50); // allow some slack
}

test "GlobalLimiter: allow_n batch" {
    var mc = ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try GlobalLimiter(ManualClock).init(.{
        .rate = 10,
        .per = .second,
        .burst = 0,
        .clock = mc,
    });

    try std.testing.expectEqual(true, lim.allow_n(8).is_allowed());
    try std.testing.expectEqual(false, lim.allow_n(4).is_allowed());
}

test "GlobalLimiter: retry_after_ms_ceil is non-zero on denial" {
    var mc = ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try GlobalLimiter(ManualClock).init(.{
        .rate = 1,
        .per = .second,
        .burst = 0,
        .clock = mc,
    });

    _ = lim.allow();
    switch (lim.allow()) {
        .denied => |d| try std.testing.expect(d.retry_after_ms_ceil() > 0),
        .allowed => return error.TestUnexpectedResult,
    }
}

test "GlobalLimiter: allow_n overflow guard" {
    var mc = ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try GlobalLimiter(ManualClock).init(.{
        .rate = 1,
        .per = .minute,
        .burst = 0,
        .clock = mc,
    });

    const out = lim.allow_n(std.math.maxInt(u32));
    try std.testing.expect(!out.is_allowed());
    switch (out) {
        .denied => |d| try std.testing.expectEqual(
            @as(i64, std.math.maxInt(i64)),
            d.retry_after_ns,
        ),
        .allowed => return error.TestUnexpectedResult,
    }
}

test "GlobalLimiter: allow_n overflow guard does not mutate state" {
    var mc = ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try GlobalLimiter(ManualClock).init(.{
        .rate = 1,
        .per = .minute,
        .burst = 0,
        .clock = mc,
    });

    _ = lim.allow_n(std.math.maxInt(u32));
    // Normal request should still work
    try std.testing.expect(lim.allow().is_allowed());
}

test "Outcome: retry_after_ms_ceil rounds up from 1ns" {
    // 1 ns → ceil(1 / 1_000_000) = 1 ms
    const outcome = Outcome{ .denied = .{ .retry_after_ns = 1 } };
    switch (outcome) {
        .denied => |d| try std.testing.expectEqual(@as(i64, 1), d.retry_after_ms_ceil()),
        .allowed => return error.TestUnexpectedResult,
    }
}

test "Outcome: retry_after_ms_ceil exact millisecond boundary" {
    // Exactly 5ms = 5_000_000 ns → should be 5ms, not 6ms
    const outcome = Outcome{ .denied = .{ .retry_after_ns = 5_000_000 } };
    switch (outcome) {
        .denied => |d| try std.testing.expectEqual(@as(i64, 5), d.retry_after_ms_ceil()),
        .allowed => return error.TestUnexpectedResult,
    }
}

test "Outcome: retry_after_ms_ceil just over boundary" {
    // 5_000_001 ns → ceil = 6ms
    const outcome = Outcome{ .denied = .{ .retry_after_ns = 5_000_001 } };
    switch (outcome) {
        .denied => |d| try std.testing.expectEqual(@as(i64, 6), d.retry_after_ms_ceil()),
        .allowed => return error.TestUnexpectedResult,
    }
}

test "Outcome: is_allowed matches tagged union" {
    const allowed = Outcome{ .allowed = {} };
    const denied = Outcome{ .denied = .{ .retry_after_ns = 42 } };
    try std.testing.expect(allowed.is_allowed());
    try std.testing.expect(!denied.is_allowed());
}
