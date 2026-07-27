//! zimit — GCRA-based rate limiter for Zig.
//!
//! Quick start:
//!
//!     const std = @import("std");
//!     const zimit = @import("zimit");
//!
//! pub fn main(init: std.process.Init) !void {
//!     const gpa = init.gpa;
//!     const io = init.io;
//!
//!     var limiter = try zimit.RateLimiter([]const u8).init(io, .{
//!         .allocator = gpa,
//!         .limit = .perSecond(5),
//!         .burst = 2,
//!     });
//!     defer limiter.deinit();
//!
//!     const key = "127.0.0.1";
//!
//!     var i: usize = 0;
//!     while (i < 5) : (i += 1) {
//!         const decision = try limiter.allow(key);
//!         switch (decision) {
//!             .allowed => std.debug.print("allowed\n", .{}),
//!             .denied => {
//!                 std.debug.print("retry in {d}ms\n", .{
//!                     decision.retryAfterMillisecondsCeil().?,
//!                 });
//!             },
//!         }
//!     }
//! }
//!
//! Note: `RateLimiter` is **not** thread-safe. Wrap it in a `std.Io.Mutex` if
//! shared. For a thread-safe global limit, use `GlobalLimiter`.

const std = @import("std");
const gcra = @import("gcra.zig");
const types = @import("types.zig");

// ── Re-exports (callers only need to import "zimit") ─────────────────────────

pub const Limit = types.Limit;
pub const Decision = types.Decision;
pub const Clock = types.Clock;
pub const SystemClock = types.SystemClock;
pub const ManualClock = types.ManualClock;
pub const ZimitError = types.ZimitError;
pub const KeyOwnership = gcra.KeyOwnership;

// ── Config ────────────────────────────────────────────────────────────────────

/// Configuration for `RateLimiter.init`.
pub const RateLimiterConfig = struct {
    allocator: std.mem.Allocator,
    /// Rate to enforce. Use `Limit.perSecond`, `perMinute`, or `perHour` for
    /// common units, or initialize `Limit` directly for an arbitrary period.
    limit: Limit,
    /// Extra requests allowed in a burst on top of the base rate.
    /// 0 means no burst — every request must wait its full slot.
    burst: u32 = 0,
    /// Reserve space for this many keys during initialization.
    initial_capacity: u32 = 0,
    /// Maximum number of keys retained. `null` leaves the store unbounded.
    max_entries: ?usize = null,
    /// Remove fully-drained keys after this much inactivity.
    /// Expired keys are reclaimed when a new key is inserted or by calling
    /// `pruneExpired`.
    idle_timeout: ?std.Io.Duration = null,
};

/// Configuration for `GlobalLimiter.init`.
pub const GlobalLimiterConfig = struct {
    /// Rate to enforce. Use `Limit.perSecond`, `perMinute`, or `perHour` for
    /// common units, or initialize `Limit` directly for an arbitrary period.
    limit: Limit,
    /// Extra requests allowed in a burst on top of the base rate.
    burst: u32 = 0,
};

fn engineConfig(config: anytype, clock: Clock) gcra.Config {
    return .{
        .limit = config.limit,
        .burst = config.burst,
        .clock = clock,
    };
}

fn waitForN(
    io: std.Io,
    n: u32,
    context: anytype,
    comptime allowNFn: anytype,
) !void {
    while (true) {
        switch (try allowNFn(context, n)) {
            .allowed => return,
            .denied => |denied| try io.sleep(denied.retry_after, .awake),
        }
    }
}

// ── GlobalLimiter ─────────────────────────────────────────────────────────────

/// A lock-free single-key rate limiter with a token-bucket-flavored API.
///
/// Use this for process-wide or service-wide limits that are shared across
/// threads — for example, "this service may make at most N outbound calls/s".
///
/// For per-key limits use `RateLimiter(K)`.
///
/// ### Thread Safety
/// This type is **thread-safe**. It can be used across multiple threads without
/// additional synchronization.
pub const GlobalLimiter = struct {
    inner: gcra.AtomicLimiter,

    /// Initialise a production limiter using Zig's monotonic awake clock.
    pub fn init(io: std.Io, cfg: GlobalLimiterConfig) ZimitError!GlobalLimiter {
        return initWithClock(cfg, .{ .system = io });
    }

    /// Initialise with an explicit clock, typically for deterministic tests.
    /// The clock's backing object must outlive the limiter.
    pub fn initWithClock(
        cfg: GlobalLimiterConfig,
        clock: Clock,
    ) ZimitError!GlobalLimiter {
        return .{ .inner = try gcra.AtomicLimiter.initWithConfig(
            engineConfig(cfg, clock),
        ) };
    }

    /// Convenience for `allowN(1)`.
    pub fn allow(self: *GlobalLimiter) ZimitError!Decision {
        return self.allowN(1);
    }

    /// Atomically consume `n` slots.
    /// Returns `error.BatchTooLarge` when `n` exceeds `1 + burst`, and
    /// `error.TimeOverflow` when the resulting time cannot be represented.
    pub fn allowN(self: *GlobalLimiter, n: u32) ZimitError!Decision {
        return self.inner.allowN(n);
    }

    /// Block the calling thread until allowed.
    pub fn wait(self: *GlobalLimiter, io: std.Io) !void {
        return self.waitN(io, 1);
    }

    /// Block until an atomic batch of `n` requests is allowed.
    pub fn waitN(self: *GlobalLimiter, io: std.Io, n: u32) !void {
        try self.inner.validateBatch(n);
        return waitForN(io, n, self, attemptN);
    }

    fn attemptN(self: *GlobalLimiter, n: u32) ZimitError!Decision {
        return self.allowN(n);
    }

    /// Resets the limiter to its initial state.
    pub fn reset(self: *GlobalLimiter) void {
        self.inner.reset();
    }
};

// ── RateLimiter ───────────────────────────────────────────────────────────────

/// A token-bucket-flavored rate limiter backed by a GCRA engine.
///
/// `K` may be `[]const u8`, an integer, enum, or another type supported by
/// `std.hash_map.AutoContext`. String keys are copied; other key values are
/// stored directly and any memory they reference remains caller-owned.
///
/// For custom equality, hashing, or ownership, use `RateLimiterWithContext`.
///
/// ### Thread Safety
/// This type is **not** thread-safe. If you need to use the same `RateLimiter`
/// instance across multiple threads, you must wrap it in a `std.Io.Mutex`.
///
/// For a thread-safe global limiter that doesn't require keys, see `GlobalLimiter`.
pub fn RateLimiter(comptime K: type) type {
    return RateLimiterImpl(K, gcra.DefaultContext(K), .default);
}

/// A keyed limiter using a caller-provided `std.HashMap` context.
///
/// Its nested `Config` requires an explicit context and key-ownership policy.
pub fn RateLimiterWithContext(comptime K: type, comptime Context: type) type {
    return RateLimiterImpl(K, Context, .custom);
}

const KeyConfigKind = enum {
    default,
    custom,
};

fn RateLimiterImpl(
    comptime K: type,
    comptime Context: type,
    comptime config_kind: KeyConfigKind,
) type {
    return struct {
        const Self = @This();
        const Inner = gcra.LimiterWithContext(K, Context);
        const WaitContext = struct {
            limiter: *Self,
            key: K,
        };

        /// Configuration accepted by this limiter's constructors.
        pub const Config = switch (config_kind) {
            .default => RateLimiterConfig,
            .custom => struct {
                allocator: std.mem.Allocator,
                /// Rate to enforce.
                limit: Limit,
                /// Extra requests allowed in a burst on top of the base rate.
                burst: u32 = 0,
                /// Reserve space for this many keys during initialization.
                initial_capacity: u32 = 0,
                /// Maximum number of retained keys, or `null` for no bound.
                max_entries: ?usize = null,
                /// Remove fully-drained keys after this much inactivity.
                idle_timeout: ?std.Io.Duration = null,
                /// Hashing and equality behavior for keys.
                context: Context,
                /// Whether keys are borrowed from the caller or owned.
                ownership: KeyOwnership(K),
            },
        };

        inner: Inner,

        /// Initialise a production limiter using Zig's monotonic awake clock.
        pub fn init(io: std.Io, cfg: Config) ZimitError!Self {
            return initWithClock(cfg, .{ .system = io });
        }

        /// Initialise with an explicit clock, typically for deterministic tests.
        /// The clock's backing object must outlive the limiter.
        pub fn initWithClock(cfg: Config, clock: Clock) ZimitError!Self {
            const storage: gcra.StorageOptions = .{
                .initial_capacity = cfg.initial_capacity,
                .max_entries = cfg.max_entries,
                .idle_timeout = cfg.idle_timeout,
            };

            return .{
                .inner = switch (config_kind) {
                    .default => try Inner.initWithConfigAndStorage(
                        cfg.allocator,
                        engineConfig(cfg, clock),
                        storage,
                    ),
                    .custom => try Inner.initWithKeyOptions(
                        cfg.allocator,
                        engineConfig(cfg, clock),
                        storage,
                        cfg.context,
                        cfg.ownership,
                    ),
                },
            };
        }

        /// Releases all memory owned by the limiter.
        pub fn deinit(self: *Self) void {
            self.inner.deinit();
        }

        /// Check whether `key` may make one request right now.
        ///
        /// On `.allowed` the internal state is updated immediately.
        /// On `.denied` the rate state is unchanged, but the key's last-seen
        /// time is refreshed when idle expiration is configured.
        pub fn allow(self: *Self, key: K) ZimitError!Decision {
            return self.allowN(key, 1);
        }

        /// Check whether `key` may make `n` requests atomically.
        ///
        /// All `n` slots are consumed together or none are — there is no
        /// partial allowance. A batch can contain at most `1 + burst`
        /// requests; larger batches return `error.BatchTooLarge`. Returns
        /// `error.TimeOverflow` when the resulting time cannot be represented.
        /// Useful for batch jobs or chunked uploads.
        pub fn allowN(self: *Self, key: K, n: u32) ZimitError!Decision {
            return self.inner.checkKeyN(key, n);
        }

        /// Block the calling thread until `key` is allowed.
        ///
        /// This is the simple synchronous wait. For async contexts, use
        /// `allow` and handle the retry duration yourself.
        pub fn wait(self: *Self, io: std.Io, key: K) !void {
            return self.waitN(io, key, 1);
        }

        /// Block until an atomic batch of `n` requests is allowed for `key`.
        pub fn waitN(self: *Self, io: std.Io, key: K, n: u32) !void {
            try self.inner.validateBatch(n);
            return waitForN(io, n, WaitContext{
                .limiter = self,
                .key = key,
            }, attemptN);
        }

        fn attemptN(context: WaitContext, n: u32) ZimitError!Decision {
            return context.limiter.allowN(context.key, n);
        }

        /// Remove a key from the store — useful when a session ends and you
        /// want to reclaim memory rather than wait for the TAT to age out.
        pub fn remove(self: *Self, key: K) void {
            self.inner.remove(key);
        }

        /// Number of keys currently tracked.
        pub fn keyCount(self: *const Self) usize {
            return self.inner.keyCount();
        }

        /// Remove fully-drained keys whose idle timeout has elapsed.
        /// Returns the number of keys removed.
        pub fn pruneExpired(self: *Self) ZimitError!usize {
            return try self.inner.pruneExpired();
        }
    };
}

/// Convenience alias — the overwhelmingly common case.
pub const StringRateLimiter = RateLimiter([]const u8);

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

fn makeLimiter(limit: Limit, burst: u32, mc: *ManualClock) !StringRateLimiter {
    return StringRateLimiter.initWithClock(.{
        .allocator = std.testing.allocator,
        .limit = limit,
        .burst = burst,
    }, mc.clock());
}

fn makeStoredLimiter(
    max_entries: ?usize,
    idle_timeout: ?std.Io.Duration,
    mc: *ManualClock,
) !StringRateLimiter {
    return StringRateLimiter.initWithClock(.{
        .allocator = std.testing.allocator,
        .limit = .perSecond(1),
        .max_entries = max_entries,
        .idle_timeout = idle_timeout,
    }, mc.clock());
}

fn makeProductionRateLimiter() !StringRateLimiter {
    return StringRateLimiter.init(std.testing.io, .{
        .allocator = std.testing.allocator,
        .limit = .perSecond(10),
    });
}

fn makeProductionGlobalLimiter() !GlobalLimiter {
    return GlobalLimiter.init(std.testing.io, .{
        .limit = .perSecond(10),
    });
}

const TestKey = struct {
    tenant: u32,
    name: []const u8,
};

const TestKeyContext = struct {
    seed: u64,

    pub fn hash(self: TestKeyContext, key: TestKey) u64 {
        var value = self.seed ^ key.tenant;
        for (key.name) |byte| {
            value = (value ^ std.ascii.toLower(byte)) *% 0x100000001b3;
        }
        return value;
    }

    pub fn eql(_: TestKeyContext, a: TestKey, b: TestKey) bool {
        return a.tenant == b.tenant and
            std.ascii.eqlIgnoreCase(a.name, b.name);
    }
};

fn cloneTestKey(
    allocator: std.mem.Allocator,
    key: TestKey,
) std.mem.Allocator.Error!TestKey {
    return .{
        .tenant = key.tenant,
        .name = try allocator.dupe(u8, key.name),
    };
}

fn deinitTestKey(allocator: std.mem.Allocator, key: TestKey) void {
    allocator.free(key.name);
}

test "production constructors remain valid after return by value" {
    var rate_limiter = try makeProductionRateLimiter();
    defer rate_limiter.deinit();
    var global_limiter = try makeProductionGlobalLimiter();

    try std.testing.expect((try rate_limiter.allow("u")).isAllowed());
    try std.testing.expect((try global_limiter.allow()).isAllowed());
}

test "RateLimiterWithContext supports borrowed keys and custom equality" {
    var mc = ManualClock{};
    const CustomLimiter = RateLimiterWithContext(TestKey, TestKeyContext);
    var limiter = try CustomLimiter.initWithClock(.{
        .allocator = std.testing.allocator,
        .limit = .perSecond(1),
        .context = .{ .seed = 42 },
        .ownership = .borrowed,
    }, mc.clock());
    defer limiter.deinit();

    try std.testing.expect((try limiter.allow(.{
        .tenant = 7,
        .name = "Alice",
    })).isAllowed());
    try std.testing.expect(!(try limiter.allow(.{
        .tenant = 7,
        .name = "alice",
    })).isAllowed());

    limiter.remove(.{ .tenant = 7, .name = "ALICE" });
    try std.testing.expectEqual(@as(usize, 0), limiter.keyCount());
}

test "RateLimiterWithContext production constructor uses custom Config" {
    const CustomLimiter = RateLimiterWithContext(TestKey, TestKeyContext);
    var limiter = try CustomLimiter.init(std.testing.io, .{
        .allocator = std.testing.allocator,
        .limit = .perSecond(1),
        .context = .{ .seed = 42 },
        .ownership = .borrowed,
    });
    defer limiter.deinit();

    try std.testing.expect((try limiter.allow(.{
        .tenant = 7,
        .name = "Alice",
    })).isAllowed());
}

test "custom and default limiter types expose only their valid configuration" {
    const DefaultLimiter = RateLimiter(TestKey);
    const CustomLimiter = RateLimiterWithContext(TestKey, TestKeyContext);

    try std.testing.expect(!@hasField(DefaultLimiter.Config, "context"));
    try std.testing.expect(!@hasField(DefaultLimiter.Config, "ownership"));
    try std.testing.expect(@hasField(CustomLimiter.Config, "context"));
    try std.testing.expect(@hasField(CustomLimiter.Config, "ownership"));
    try std.testing.expect(!@hasDecl(CustomLimiter, "initWithKeyOptions"));
    try std.testing.expect(!@hasDecl(
        CustomLimiter,
        "initWithClockAndKeyOptions",
    ));
}

test "RateLimiterWithContext can own deeply copied keys" {
    var mc = ManualClock{};
    const CustomLimiter = RateLimiterWithContext(TestKey, TestKeyContext);
    var limiter = try CustomLimiter.initWithClock(.{
        .allocator = std.testing.allocator,
        .limit = .perSecond(1),
        .context = .{ .seed = 42 },
        .ownership = .{ .owned = .{
            .clone = cloneTestKey,
            .deinit = deinitTestKey,
        } },
    }, mc.clock());
    defer limiter.deinit();

    var original: ?[]u8 = try std.testing.allocator.dupe(u8, "Alice");
    defer if (original) |name| std.testing.allocator.free(name);

    try std.testing.expect((try limiter.allow(.{
        .tenant = 7,
        .name = original.?,
    })).isAllowed());
    std.testing.allocator.free(original.?);
    original = null;

    try std.testing.expect(!(try limiter.allow(.{
        .tenant = 7,
        .name = "alice",
    })).isAllowed());

    limiter.remove(.{ .tenant = 7, .name = "ALICE" });
    try std.testing.expectEqual(@as(usize, 0), limiter.keyCount());
}

test "RateLimiter: allow — fresh key passes" {
    var mc = ManualClock{};
    mc.set(.fromNanoseconds(std.time.ns_per_s));
    var lim = try makeLimiter(.perSecond(10), 0, &mc);
    defer lim.deinit();

    const out = try lim.allow("alice");
    try std.testing.expect(out.isAllowed());
}

test "RateLimiter: allow — exhausted key is denied" {
    var mc = ManualClock{};
    mc.set(.fromNanoseconds(std.time.ns_per_s));
    var lim = try makeLimiter(.perSecond(3), 0, &mc);
    defer lim.deinit();

    _ = try lim.allow("u");
    _ = try lim.allow("u");
    _ = try lim.allow("u");
    const out = try lim.allow("u");
    try std.testing.expect(!out.isAllowed());
}

test "RateLimiter: retryAfterMillisecondsCeil rounds up" {
    var mc = ManualClock{};
    mc.set(.fromNanoseconds(std.time.ns_per_s));
    // 1 req/s → emission interval = 1 000 000 000 ns = 1000 ms
    var lim = try makeLimiter(.perSecond(1), 0, &mc);
    defer lim.deinit();

    _ = try lim.allow("u");
    const out = try lim.allow("u");
    switch (out) {
        .denied => |d| {
            // The retry duration should be ~1s and round up to 1000ms.
            try std.testing.expect(d.retry_after.toNanoseconds() > 0);
            try std.testing.expectEqual(@as(i64, 1000), out.retryAfterMillisecondsCeil().?);
        },
        .allowed => return error.TestUnexpectedResult,
    }
}

test "RateLimiter: allow — keys are isolated" {
    var mc = ManualClock{};
    mc.set(.fromNanoseconds(std.time.ns_per_s));
    var lim = try makeLimiter(.perSecond(1), 0, &mc);
    defer lim.deinit();

    _ = try lim.allow("alice");
    const bob = try lim.allow("bob");
    try std.testing.expect(bob.isAllowed());
}

test "RateLimiter: allow — time advance unblocks key" {
    var mc = ManualClock{};
    mc.set(.fromNanoseconds(std.time.ns_per_s));
    var lim = try makeLimiter(.perSecond(1), 0, &mc);
    defer lim.deinit();

    _ = try lim.allow("u");
    try std.testing.expect(!(try lim.allow("u")).isAllowed());

    mc.tick(.fromNanoseconds(std.time.ns_per_s));
    try std.testing.expect((try lim.allow("u")).isAllowed());
}

test "RateLimiter: burst — allows base+burst requests at t=0" {
    var mc = ManualClock{};
    mc.set(.fromNanoseconds(std.time.ns_per_s));
    // rate=5/s, burst=3 → 1 base + 3 burst = 4 requests immediately
    var lim = try makeLimiter(.perSecond(5), 3, &mc);
    defer lim.deinit();

    var i: usize = 0;
    while (i < 4) : (i += 1) {
        const out = try lim.allow("u");
        try std.testing.expectEqual(true, out.isAllowed());
    }
    try std.testing.expectEqual(false, (try lim.allow("u")).isAllowed());
}

test "RateLimiter: burst — replenishes after delay" {
    var mc = ManualClock{};
    mc.set(.fromNanoseconds(std.time.ns_per_s));
    var lim = try makeLimiter(.perSecond(1), 1, &mc);
    defer lim.deinit();

    // Consume both base + burst
    _ = try lim.allow("u");
    _ = try lim.allow("u");
    try std.testing.expect(!(try lim.allow("u")).isAllowed());

    // One second later, one slot has replenished
    mc.tick(.fromNanoseconds(std.time.ns_per_s));
    try std.testing.expect((try lim.allow("u")).isAllowed());
}

test "RateLimiter: allowN — consume multiple slots atomically" {
    var mc = ManualClock{};
    mc.set(.fromNanoseconds(std.time.ns_per_s));
    // burst=6 → one base request plus six extra requests at once
    var lim = try makeLimiter(.perSecond(10), 6, &mc);
    defer lim.deinit();

    // Consume 7 — succeeds
    try std.testing.expectEqual(true, (try lim.allowN("u", 7)).isAllowed());
    // The instantaneous capacity is exhausted — requesting 4 fails
    try std.testing.expectEqual(false, (try lim.allowN("u", 4)).isAllowed());
    // No partial capacity is available for a further batch of 3
    try std.testing.expectEqual(false, (try lim.allowN("u", 3)).isAllowed());
}

test "RateLimiter: allowN — fresh limiter without burst rejects batch" {
    var mc = ManualClock{};
    mc.set(.fromNanoseconds(std.time.ns_per_s));
    var lim = try makeLimiter(.perSecond(10), 0, &mc);
    defer lim.deinit();

    try std.testing.expectError(error.BatchTooLarge, lim.allowN("u", 5));
    try std.testing.expectEqual(@as(usize, 0), lim.keyCount());
    try std.testing.expect((try lim.allow("u")).isAllowed());
}

test "RateLimiter: allowN — n=0 always allowed without state change" {
    var mc = ManualClock{};
    mc.set(.fromNanoseconds(std.time.ns_per_s));
    var lim = try makeLimiter(.perSecond(1), 0, &mc);
    defer lim.deinit();

    // Exhaust the key
    _ = try lim.allow("u");
    try std.testing.expect(!(try lim.allow("u")).isAllowed());

    // n=0 should still return allowed and not mutate state
    try std.testing.expect((try lim.allowN("u", 0)).isAllowed());
    try std.testing.expect(!(try lim.allow("u")).isAllowed());
}

test "RateLimiter: allowN — partial batch is never granted" {
    var mc = ManualClock{};
    mc.set(.fromNanoseconds(std.time.ns_per_s));
    var lim = try makeLimiter(.perSecond(5), 4, &mc);
    defer lim.deinit();

    // Consume 3 slots atomically — succeeds, TAT now 600ms out
    try std.testing.expectEqual(true, (try lim.allowN("u", 3)).isAllowed());

    // Request 10 more — impossible for this configuration, must fail
    try std.testing.expectError(error.BatchTooLarge, lim.allowN("u", 10));

    // Advance time by 600ms — exactly the 3 slots we consumed
    mc.tick(.fromNanoseconds(600 * std.time.ns_per_ms));

    // TAT is now at 1600ms, time is at 1600ms — key is fresh again.
    // If allowN had partially mutated state on the failed n=10 attempt,
    // the TAT would be further ahead and this would fail.
    try std.testing.expectEqual(true, (try lim.allowN("u", 5)).isAllowed());
}

test "RateLimiter: remove — resets key to fresh" {
    var mc = ManualClock{};
    mc.set(.fromNanoseconds(std.time.ns_per_s));
    var lim = try makeLimiter(.perSecond(1), 0, &mc);
    defer lim.deinit();

    _ = try lim.allow("u");
    try std.testing.expect(!(try lim.allow("u")).isAllowed());

    lim.remove("u");
    try std.testing.expectEqual(@as(usize, 0), lim.keyCount());
    try std.testing.expect((try lim.allow("u")).isAllowed());
}

test "RateLimiter: per minute config" {
    var mc = ManualClock{};
    mc.set(.fromNanoseconds(std.time.ns_per_s));
    var lim = try makeLimiter(.perMinute(60), 0, &mc);
    defer lim.deinit();

    // 60/min = 1/s — second request at same instant denied
    _ = try lim.allow("u");
    try std.testing.expect(!(try lim.allow("u")).isAllowed());

    // Advance 1 second → allowed again
    mc.tick(.fromNanoseconds(std.time.ns_per_s));
    try std.testing.expect((try lim.allow("u")).isAllowed());
}

test "RateLimiter: arbitrary Limit period" {
    var mc = ManualClock{};
    var lim = try makeLimiter(.{
        .count = 2,
        .period = .fromNanoseconds(std.time.ns_per_s / 2),
    }, 0, &mc);
    defer lim.deinit();

    try std.testing.expect((try lim.allow("u")).isAllowed());
    try std.testing.expect(!(try lim.allow("u")).isAllowed());

    mc.tick(.fromNanoseconds(std.time.ns_per_s / 4));
    try std.testing.expect((try lim.allow("u")).isAllowed());
}

test "RateLimiter: period outside internal range returns TimeOverflow" {
    var mc = ManualClock{};
    try std.testing.expectError(
        error.TimeOverflow,
        makeLimiter(.{
            .count = 1,
            .period = .max,
        }, 0, &mc),
    );
}

test "RateLimiter: integer key type (u64)" {
    var mc = ManualClock{};
    mc.set(.fromNanoseconds(std.time.ns_per_s));
    var lim = try RateLimiter(u64).initWithClock(.{
        .allocator = std.testing.allocator,
        .limit = .perSecond(5),
        .burst = 0,
    }, mc.clock());
    defer lim.deinit();

    try std.testing.expect((try lim.allow(1001)).isAllowed());
    try std.testing.expect((try lim.allow(1002)).isAllowed());
    // Same key, second request — denied
    try std.testing.expectError(error.BatchTooLarge, lim.allowN(1001, 5));
}

test "RateLimiter: sustained throughput over simulated minute" {
    var mc = ManualClock{};
    var lim = try makeLimiter(.perSecond(100), 0, &mc);
    defer lim.deinit();

    var allowed: usize = 0;
    var t: i64 = 0;
    // 60 seconds, one attempt every 5ms (12 000 attempts total)
    while (t < 60 * std.time.ns_per_s) : (t += 5_000_000) {
        mc.set(.fromNanoseconds(t));
        if ((try lim.allow("u")).isAllowed()) allowed += 1;
    }
    // Expect exactly 6 000 allowed (100/s × 60s)
    try std.testing.expectEqual(@as(usize, 6_000), allowed);
}

test "RateLimiter: allowN rejects an unrepresentable batch" {
    var mc = ManualClock{};
    mc.set(.fromNanoseconds(std.time.ns_per_s));
    // per=minute, rate=1 → interval=60_000_000_000 ns → max_batch=153
    // maxInt(u32)=4_294_967_295 >> 153, so guard fires
    var lim = try makeLimiter(.perMinute(1), 0, &mc);
    defer lim.deinit();

    try std.testing.expectError(
        error.BatchTooLarge,
        lim.allowN("u", std.math.maxInt(u32)),
    );
}

test "RateLimiter: rejected allowN does not mutate state" {
    var mc = ManualClock{};
    mc.set(.fromNanoseconds(std.time.ns_per_s));
    var lim = try makeLimiter(.perMinute(1), 0, &mc);
    defer lim.deinit();

    try std.testing.expectError(
        error.BatchTooLarge,
        lim.allowN("u", std.math.maxInt(u32)),
    );

    const out = try lim.allow("u");
    try std.testing.expect(out.isAllowed());
}

test "RateLimiter: unrepresentable admission returns TimeOverflow" {
    var mc = ManualClock{};
    mc.set(.fromNanoseconds(std.math.maxInt(i64) - 5));
    var lim = try makeLimiter(.{
        .count = 1,
        .period = .fromNanoseconds(10),
    }, 0, &mc);
    defer lim.deinit();

    try std.testing.expectError(error.TimeOverflow, lim.allow("u"));
    try std.testing.expectEqual(@as(usize, 0), lim.keyCount());
}

test "RateLimiter: allowN large but valid n is evaluated normally" {
    var mc = ManualClock{};
    mc.set(.fromNanoseconds(std.time.ns_per_s));
    // burst=4 makes a batch of five admissible.
    var lim = try makeLimiter(.perSecond(10), 4, &mc);
    defer lim.deinit();

    const out = try lim.allowN("u", 5);
    switch (out) {
        .allowed => {},
        .denied => |d| try std.testing.expect(d.retry_after.toNanoseconds() < std.math.maxInt(i64)),
    }
}

test "RateLimiter: allowN — n=1 and allow are equivalent" {
    var mc = ManualClock{};
    mc.set(.fromNanoseconds(std.time.ns_per_s));
    var lim_a = try makeLimiter(.perSecond(5), 0, &mc);
    defer lim_a.deinit();
    var lim_b = try makeLimiter(.perSecond(5), 0, &mc);
    defer lim_b.deinit();

    var i: usize = 0;
    while (i < 6) : (i += 1) {
        const a = try lim_a.allow("u");
        const b = try lim_b.allowN("u", 1);
        try std.testing.expectEqual(a.isAllowed(), b.isAllowed());
    }
}

test "RateLimiter: remove on absent key is safe" {
    var mc = ManualClock{};
    mc.set(.fromNanoseconds(std.time.ns_per_s));
    var lim = try makeLimiter(.perSecond(5), 0, &mc);
    defer lim.deinit();

    lim.remove("ghost");
    try std.testing.expectEqual(@as(usize, 0), lim.keyCount());
}

test "RateLimiter: keyCount after mixed allow and remove" {
    var mc = ManualClock{};
    mc.set(.fromNanoseconds(std.time.ns_per_s));
    var lim = try makeLimiter(.perSecond(10), 0, &mc);
    defer lim.deinit();

    _ = try lim.allow("a");
    _ = try lim.allow("b");
    _ = try lim.allow("c");
    try std.testing.expectEqual(@as(usize, 3), lim.keyCount());

    lim.remove("b");
    try std.testing.expectEqual(@as(usize, 2), lim.keyCount());

    lim.remove("b"); // second remove is safe
    try std.testing.expectEqual(@as(usize, 2), lim.keyCount());
}

test "RateLimiter: max_entries rejects a new key at capacity" {
    var mc = ManualClock{};
    var lim = try makeStoredLimiter(2, null, &mc);
    defer lim.deinit();

    _ = try lim.allow("a");
    _ = try lim.allow("b");

    try std.testing.expectError(error.CapacityExceeded, lim.allow("c"));
    try std.testing.expectEqual(@as(usize, 2), lim.keyCount());
}

test "RateLimiter: initial_capacity reserves key storage" {
    var mc = ManualClock{};
    var lim = try StringRateLimiter.initWithClock(.{
        .allocator = std.testing.allocator,
        .limit = .perSecond(1),
        .initial_capacity = 100,
    }, mc.clock());
    defer lim.deinit();

    try std.testing.expect(lim.inner.store.capacity() >= 100);
    try std.testing.expectEqual(@as(usize, 0), lim.keyCount());
}

test "RateLimiter: initial_capacity is limited by max_entries" {
    var mc = ManualClock{};
    var lim = try StringRateLimiter.initWithClock(.{
        .allocator = std.testing.allocator,
        .limit = .perSecond(1),
        .initial_capacity = 100,
        .max_entries = 2,
    }, mc.clock());
    defer lim.deinit();

    try std.testing.expect(lim.inner.store.capacity() >= 2);
    try std.testing.expect(lim.inner.store.capacity() < 100);
}

test "RateLimiter: existing key remains usable at capacity" {
    var mc = ManualClock{};
    var lim = try makeStoredLimiter(1, null, &mc);
    defer lim.deinit();

    _ = try lim.allow("a");
    try std.testing.expect(!(try lim.allow("a")).isAllowed());
    try std.testing.expectEqual(@as(usize, 1), lim.keyCount());
}

test "RateLimiter: max_entries zero rejects every new key" {
    var mc = ManualClock{};
    var lim = try makeStoredLimiter(0, null, &mc);
    defer lim.deinit();

    try std.testing.expectError(error.CapacityExceeded, lim.allow("a"));
    try std.testing.expectEqual(@as(usize, 0), lim.keyCount());
}

test "RateLimiter: capacity automatically reclaims expired key" {
    var mc = ManualClock{};
    var lim = try makeStoredLimiter(1, .fromSeconds(1), &mc);
    defer lim.deinit();

    _ = try lim.allow("a");
    mc.tick(.fromNanoseconds(std.time.ns_per_s));

    try std.testing.expect((try lim.allow("b")).isAllowed());
    try std.testing.expectEqual(@as(usize, 1), lim.keyCount());
    try std.testing.expectError(error.CapacityExceeded, lim.allow("a"));
}

test "RateLimiter: new key opportunistically reclaims expired entries" {
    var mc = ManualClock{};
    var lim = try makeStoredLimiter(3, .fromSeconds(1), &mc);
    defer lim.deinit();

    _ = try lim.allow("a");
    mc.tick(.fromNanoseconds(std.time.ns_per_s));

    try std.testing.expect((try lim.allow("b")).isAllowed());
    try std.testing.expectEqual(@as(usize, 1), lim.keyCount());
}

test "RateLimiter: pruneExpired removes all eligible keys" {
    var mc = ManualClock{};
    var lim = try makeStoredLimiter(5, .fromSeconds(1), &mc);
    defer lim.deinit();

    _ = try lim.allow("a");
    _ = try lim.allow("b");
    _ = try lim.allow("c");
    mc.tick(.fromNanoseconds(std.time.ns_per_s));

    try std.testing.expectEqual(@as(usize, 3), try lim.pruneExpired());
    try std.testing.expectEqual(@as(usize, 0), lim.keyCount());
    try std.testing.expectEqual(@as(usize, 0), try lim.pruneExpired());
}

test "RateLimiter: pruneExpired safely removes many entries" {
    var mc = ManualClock{};
    var lim = try makeStoredLimiter(128, .fromSeconds(1), &mc);
    defer lim.deinit();

    var key_buffer: [32]u8 = undefined;
    for (0..128) |i| {
        const key = try std.fmt.bufPrint(&key_buffer, "key-{d}", .{i});
        try std.testing.expect((try lim.allow(key)).isAllowed());
    }

    mc.tick(.fromNanoseconds(std.time.ns_per_s / 2));
    for (0..128) |i| {
        if (i % 2 != 0) continue;
        const key = try std.fmt.bufPrint(&key_buffer, "key-{d}", .{i});
        _ = try lim.allow(key);
    }

    mc.tick(.fromNanoseconds(std.time.ns_per_s / 2));
    try std.testing.expectEqual(@as(usize, 64), try lim.pruneExpired());
    try std.testing.expectEqual(@as(usize, 64), lim.keyCount());

    mc.tick(.fromNanoseconds(std.time.ns_per_s / 2));
    try std.testing.expectEqual(@as(usize, 64), try lim.pruneExpired());
    try std.testing.expectEqual(@as(usize, 0), lim.keyCount());
}

test "RateLimiter: failed prune leaves entries intact" {
    var mc = ManualClock{};
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var lim = try StringRateLimiter.initWithClock(.{
        .allocator = failing.allocator(),
        .limit = .perSecond(1),
        .max_entries = 2,
        .idle_timeout = .fromNanoseconds(std.time.ns_per_s),
    }, mc.clock());
    defer lim.deinit();

    _ = try lim.allow("a");
    _ = try lim.allow("b");
    mc.tick(.fromNanoseconds(std.time.ns_per_s));

    failing.fail_index = failing.alloc_index;
    try std.testing.expectError(error.OutOfMemory, lim.pruneExpired());
    try std.testing.expectEqual(@as(usize, 2), lim.keyCount());

    failing.fail_index = std.math.maxInt(usize);
    try std.testing.expectEqual(@as(usize, 2), try lim.pruneExpired());
    try std.testing.expectEqual(@as(usize, 0), lim.keyCount());
}

test "RateLimiter: pruneExpired preserves outstanding debt" {
    var mc = ManualClock{};
    var lim = try StringRateLimiter.initWithClock(.{
        .allocator = std.testing.allocator,
        .limit = .perSecond(1),
        .burst = 2,
        .max_entries = 1,
        .idle_timeout = .fromNanoseconds(std.time.ns_per_s),
    }, mc.clock());
    defer lim.deinit();

    try std.testing.expect((try lim.allowN("a", 3)).isAllowed());
    mc.tick(.fromNanoseconds(2 * std.time.ns_per_s));
    try std.testing.expectEqual(@as(usize, 0), try lim.pruneExpired());
    try std.testing.expectEqual(@as(usize, 1), lim.keyCount());

    mc.tick(.fromNanoseconds(std.time.ns_per_s));
    try std.testing.expectEqual(@as(usize, 1), try lim.pruneExpired());
}

test "RateLimiter: denied attempt refreshes idle timeout" {
    var mc = ManualClock{};
    var lim = try makeStoredLimiter(1, .fromSeconds(1), &mc);
    defer lim.deinit();

    _ = try lim.allow("a");
    mc.tick(.fromNanoseconds(std.time.ns_per_s / 2));
    try std.testing.expect(!(try lim.allow("a")).isAllowed());

    mc.tick(.fromNanoseconds(std.time.ns_per_s / 2));
    try std.testing.expectEqual(@as(usize, 0), try lim.pruneExpired());

    mc.tick(.fromNanoseconds(std.time.ns_per_s / 2));
    try std.testing.expectEqual(@as(usize, 1), try lim.pruneExpired());
}

test "RateLimiter: pruneExpired ignores backward clock movement" {
    var mc = ManualClock{};
    mc.set(.fromNanoseconds(10 * std.time.ns_per_s));
    var lim = try makeStoredLimiter(1, .fromSeconds(1), &mc);
    defer lim.deinit();

    _ = try lim.allow("a");
    mc.set(.fromNanoseconds(0));

    try std.testing.expectEqual(@as(usize, 0), try lim.pruneExpired());
    try std.testing.expectEqual(@as(usize, 1), lim.keyCount());
}

test "RateLimiter: init rejects non-positive idle timeout" {
    var mc = ManualClock{};
    try std.testing.expectError(
        error.InvalidIdleTimeout,
        makeStoredLimiter(1, .zero, &mc),
    );
    try std.testing.expectError(
        error.InvalidIdleTimeout,
        makeStoredLimiter(1, .fromNanoseconds(-1), &mc),
    );
}

test "RateLimiter: idle timeout outside internal range returns TimeOverflow" {
    var mc = ManualClock{};
    try std.testing.expectError(
        error.TimeOverflow,
        makeStoredLimiter(1, .max, &mc),
    );
}

test "RateLimiter: retry duration is positive on denial" {
    var mc = ManualClock{};
    mc.set(.fromNanoseconds(std.time.ns_per_s));
    var lim = try makeLimiter(.perSecond(1), 0, &mc);
    defer lim.deinit();

    _ = try lim.allow("u");
    const out = try lim.allow("u");
    switch (out) {
        .denied => |d| try std.testing.expect(d.retry_after.toNanoseconds() > 0),
        .allowed => return error.TestUnexpectedResult,
    }
}

test "RateLimiter: retry duration decreases as time advances" {
    var mc = ManualClock{};
    mc.set(.fromNanoseconds(std.time.ns_per_s));
    var lim = try makeLimiter(.perSecond(1), 0, &mc);
    defer lim.deinit();

    _ = try lim.allow("u");

    const out1 = try lim.allow("u");
    const wait1 = switch (out1) {
        .denied => |d| d.retry_after.toNanoseconds(),
        .allowed => return error.TestUnexpectedResult,
    };

    mc.tick(.fromNanoseconds(std.time.ns_per_s / 2));

    const out2 = try lim.allow("u");
    const wait2 = switch (out2) {
        .denied => |d| d.retry_after.toNanoseconds(),
        .allowed => return error.TestUnexpectedResult,
    };

    try std.testing.expect(wait2 < wait1);
}

test "RateLimiter: wait blocks and succeeds" {
    var lim = try RateLimiter(u32).init(std.testing.io, .{
        .allocator = std.testing.allocator,
        .limit = .perSecond(10), // 100ms per slot
    });
    defer lim.deinit();

    // Exhaust key 42
    var i: usize = 0;
    while (i < 10) : (i += 1) {
        _ = try lim.allow(42);
    }

    const start = std.Io.Clock.awake.now(std.testing.io).toMilliseconds();
    try lim.wait(std.testing.io, 42);
    const end = std.Io.Clock.awake.now(std.testing.io).toMilliseconds();

    try std.testing.expect(end - start >= 50);
}

test "RateLimiter: waitN blocks until the full batch is allowed" {
    var lim = try RateLimiter(u32).init(std.testing.io, .{
        .allocator = std.testing.allocator,
        .limit = .perSecond(10),
        .burst = 1,
    });
    defer lim.deinit();

    try std.testing.expect((try lim.allowN(42, 2)).isAllowed());

    const start = std.Io.Clock.awake.now(std.testing.io).toMilliseconds();
    try lim.waitN(std.testing.io, 42, 2);
    const end = std.Io.Clock.awake.now(std.testing.io).toMilliseconds();

    try std.testing.expect(end - start >= 100);
}

test "RateLimiter: waitN rejects an impossible batch" {
    var mc = ManualClock{};
    var lim = try makeLimiter(.perSecond(10), 0, &mc);
    defer lim.deinit();

    try std.testing.expectError(
        error.BatchTooLarge,
        lim.waitN(std.testing.io, "u", 2),
    );
}

test "RateLimiter: stress — 10k unique keys" {
    var mc = ManualClock{};
    var lim = try RateLimiter(u32).initWithClock(.{
        .allocator = std.testing.allocator,
        .limit = .perHour(1),
    }, mc.clock());
    defer lim.deinit();

    var i: u32 = 0;
    while (i < 10_000) : (i += 1) {
        try std.testing.expect((try lim.allow(i)).isAllowed());
    }
    try std.testing.expectEqual(@as(usize, 10_000), lim.keyCount());

    // Second pass — all must be denied (rate is 1/hour)
    i = 0;
    while (i < 10_000) : (i += 1) {
        try std.testing.expect(!(try lim.allow(i)).isAllowed());
    }
}

test "RateLimiter: init rejects zero rate" {
    var mc = ManualClock{};
    const result = StringRateLimiter.initWithClock(.{
        .allocator = std.testing.allocator,
        .limit = .perSecond(0),
        .burst = 0,
    }, mc.clock());
    try std.testing.expectError(error.InvalidLimit, result);
}

test "RateLimiter: init rejects rate > 1 req/ns" {
    var mc = ManualClock{};
    // per = .second (= 1_000_000_000 ns)
    // rate = 2_000_000_000 > 1_000_000_000
    const result = StringRateLimiter.initWithClock(.{
        .allocator = std.testing.allocator,
        .limit = .perSecond(2_000_000_000),
        .burst = 0,
    }, mc.clock());
    try std.testing.expectError(error.RateExceedsRes, result);
}

test "RateLimiter: per-hour config with burst and time advance" {
    var mc = ManualClock{};
    mc.set(.fromNanoseconds(std.time.ns_per_s));
    // 1 req/hour, burst=1 → 2 immediate requests
    var lim = try StringRateLimiter.initWithClock(.{
        .allocator = std.testing.allocator,
        .limit = .perHour(1),
        .burst = 1,
    }, mc.clock());
    defer lim.deinit();

    // 2 requests pass (1 base + 1 burst)
    try std.testing.expect((try lim.allow("u")).isAllowed());
    try std.testing.expect((try lim.allow("u")).isAllowed());
    // Third denied
    try std.testing.expect(!(try lim.allow("u")).isAllowed());

    // Advance 1 hour → 1 slot replenished
    mc.tick(.fromNanoseconds(3600 * std.time.ns_per_s));
    try std.testing.expect((try lim.allow("u")).isAllowed());
}

test "RateLimiter: allowN with n=0 on exhausted key returns allowed" {
    var mc = ManualClock{};
    mc.set(.fromNanoseconds(std.time.ns_per_s));
    var lim = try makeLimiter(.perSecond(1), 0, &mc);
    defer lim.deinit();

    _ = try lim.allow("u"); // exhaust
    try std.testing.expect(!(try lim.allow("u")).isAllowed()); // confirm exhausted
    try std.testing.expect((try lim.allowN("u", 0)).isAllowed()); // n=0 still allowed
}

test "RateLimiter: multiple keys with different burst behavior" {
    var mc = ManualClock{};
    mc.set(.fromNanoseconds(std.time.ns_per_s));
    var lim = try makeLimiter(.perSecond(1), 2, &mc);
    defer lim.deinit();

    // key "a" uses all burst: 3 requests (1 base + 2 burst)
    try std.testing.expect((try lim.allow("a")).isAllowed());
    try std.testing.expect((try lim.allow("a")).isAllowed());
    try std.testing.expect((try lim.allow("a")).isAllowed());
    try std.testing.expect(!(try lim.allow("a")).isAllowed());

    // key "b" is independent — still has full burst capacity
    try std.testing.expect((try lim.allow("b")).isAllowed());
    try std.testing.expect((try lim.allow("b")).isAllowed());
    try std.testing.expect((try lim.allow("b")).isAllowed());
    try std.testing.expect(!(try lim.allow("b")).isAllowed());
}

test "RateLimiter: StringRateLimiter type alias works" {
    var mc = ManualClock{};
    mc.set(.fromNanoseconds(std.time.ns_per_s));
    var lim = try StringRateLimiter.initWithClock(.{
        .allocator = std.testing.allocator,
        .limit = .perSecond(5),
        .burst = 0,
    }, mc.clock());
    defer lim.deinit();

    try std.testing.expect((try lim.allow("test")).isAllowed());
}

test "GlobalLimiter: concurrent contention" {
    const num_threads = 4;
    const total_slots = 1000;

    var lim = try GlobalLimiter.init(std.testing.io, .{
        .limit = .perHour(total_slots),
        .burst = total_slots - 1,
    });

    const Ctx = struct {
        limiter: *GlobalLimiter,
        allowed: std.atomic.Value(usize),

        fn run(ctx: *@This()) void {
            while (true) {
                if ((ctx.limiter.allow() catch unreachable).isAllowed()) {
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
    mc.set(.fromNanoseconds(std.time.ns_per_s));
    var lim = try GlobalLimiter.initWithClock(.{
        .limit = .perSecond(5),
        .burst = 4, // 1 base + 4 burst = 5
    }, mc.clock());

    var i: usize = 0;
    while (i < 5) : (i += 1) {
        try std.testing.expectEqual(true, (try lim.allow()).isAllowed());
    }
    try std.testing.expectEqual(false, (try lim.allow()).isAllowed());
}

test "GlobalLimiter: arbitrary Limit period" {
    var mc = ManualClock{};
    var lim = try GlobalLimiter.initWithClock(.{
        .limit = .{
            .count = 2,
            .period = .fromNanoseconds(std.time.ns_per_s / 2),
        },
    }, mc.clock());

    try std.testing.expect((try lim.allow()).isAllowed());
    try std.testing.expect(!(try lim.allow()).isAllowed());

    mc.tick(.fromNanoseconds(std.time.ns_per_s / 4));
    try std.testing.expect((try lim.allow()).isAllowed());
}

test "GlobalLimiter: reset restores capacity" {
    var mc = ManualClock{};
    mc.set(.fromNanoseconds(std.time.ns_per_s));
    var lim = try GlobalLimiter.initWithClock(.{
        .limit = .perSecond(1),
        .burst = 0,
    }, mc.clock());

    _ = try lim.allow();
    try std.testing.expectEqual(false, (try lim.allow()).isAllowed());
    lim.reset();
    try std.testing.expectEqual(true, (try lim.allow()).isAllowed());
}

test "GlobalLimiter: wait blocks and eventually succeeds" {
    var lim = try GlobalLimiter.init(std.testing.io, .{
        .limit = .perSecond(10), // 100ms per slot
        .burst = 0,
    });

    // Exhaust immediately
    var i: usize = 0;
    while (i < 10) : (i += 1) {
        _ = try lim.allow();
    }
    try std.testing.expectEqual(false, (try lim.allow()).isAllowed());

    const start = std.Io.Clock.awake.now(std.testing.io).toMilliseconds();
    try lim.wait(std.testing.io); // should block for roughly 100ms
    const end = std.Io.Clock.awake.now(std.testing.io).toMilliseconds();

    try std.testing.expect(end - start >= 50); // allow some slack
}

test "GlobalLimiter: waitN blocks until the full batch is allowed" {
    var lim = try GlobalLimiter.init(std.testing.io, .{
        .limit = .perSecond(10),
        .burst = 1,
    });

    try std.testing.expect((try lim.allowN(2)).isAllowed());

    const start = std.Io.Clock.awake.now(std.testing.io).toMilliseconds();
    try lim.waitN(std.testing.io, 2);
    const end = std.Io.Clock.awake.now(std.testing.io).toMilliseconds();

    try std.testing.expect(end - start >= 100);
}

test "GlobalLimiter: waitN rejects an impossible batch" {
    var mc = ManualClock{};
    var lim = try GlobalLimiter.initWithClock(.{
        .limit = .perSecond(10),
    }, mc.clock());

    try std.testing.expectError(
        error.BatchTooLarge,
        lim.waitN(std.testing.io, 2),
    );
}

test "GlobalLimiter: allowN batch" {
    var mc = ManualClock{};
    mc.set(.fromNanoseconds(std.time.ns_per_s));
    var lim = try GlobalLimiter.initWithClock(.{
        .limit = .perSecond(10),
        .burst = 7,
    }, mc.clock());

    try std.testing.expectEqual(true, (try lim.allowN(8)).isAllowed());
    try std.testing.expectEqual(false, (try lim.allowN(4)).isAllowed());
}

test "GlobalLimiter: allowN fresh limiter without burst rejects batch" {
    var mc = ManualClock{};
    mc.set(.fromNanoseconds(std.time.ns_per_s));
    var lim = try GlobalLimiter.initWithClock(.{
        .limit = .perSecond(10),
        .burst = 0,
    }, mc.clock());

    try std.testing.expectError(error.BatchTooLarge, lim.allowN(5));
    try std.testing.expect((try lim.allow()).isAllowed());
}

test "GlobalLimiter: retryAfterMillisecondsCeil is non-zero on denial" {
    var mc = ManualClock{};
    mc.set(.fromNanoseconds(std.time.ns_per_s));
    var lim = try GlobalLimiter.initWithClock(.{
        .limit = .perSecond(1),
        .burst = 0,
    }, mc.clock());

    _ = try lim.allow();
    const decision = try lim.allow();
    switch (decision) {
        .denied => try std.testing.expect(
            decision.retryAfterMillisecondsCeil().? > 0,
        ),
        .allowed => return error.TestUnexpectedResult,
    }
}

test "GlobalLimiter: allowN rejects an unrepresentable batch" {
    var mc = ManualClock{};
    mc.set(.fromNanoseconds(std.time.ns_per_s));
    var lim = try GlobalLimiter.initWithClock(.{
        .limit = .perMinute(1),
        .burst = 0,
    }, mc.clock());

    try std.testing.expectError(
        error.BatchTooLarge,
        lim.allowN(std.math.maxInt(u32)),
    );
}

test "GlobalLimiter: rejected allowN does not mutate state" {
    var mc = ManualClock{};
    mc.set(.fromNanoseconds(std.time.ns_per_s));
    var lim = try GlobalLimiter.initWithClock(.{
        .limit = .perMinute(1),
        .burst = 0,
    }, mc.clock());

    try std.testing.expectError(
        error.BatchTooLarge,
        lim.allowN(std.math.maxInt(u32)),
    );
    // Normal request should still work
    try std.testing.expect((try lim.allow()).isAllowed());
}

test "GlobalLimiter: unrepresentable admission returns TimeOverflow" {
    var mc = ManualClock{};
    mc.set(.fromNanoseconds(std.math.maxInt(i64) - 5));
    var lim = try GlobalLimiter.initWithClock(.{
        .limit = .{
            .count = 1,
            .period = .fromNanoseconds(10),
        },
    }, mc.clock());

    try std.testing.expectError(error.TimeOverflow, lim.allow());
}

test "GlobalLimiter: init rejects unrepresentable burst duration" {
    var mc = ManualClock{};
    try std.testing.expectError(
        error.TimeOverflow,
        GlobalLimiter.initWithClock(.{
            .limit = .perHour(1),
            .burst = std.math.maxInt(u32),
        }, mc.clock()),
    );
}
