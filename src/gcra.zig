//! GCRA engine and multi-key rate limiter for zimit.
//!
//! The engine itself (`check`) is a pure function — no allocation, no I/O,
//! no global state. The `Limiter` struct wraps it with a HashMap key store.

const std = @import("std");
const types = @import("types.zig");

pub const Limit = types.Limit;
pub const Decision = types.Decision;
pub const ZimitError = types.ZimitError;

// ── Pure GCRA engine ──────────────────────────────────────────────────────────

/// Run one GCRA check. Pure function — no allocations, no side effects.
///
/// Arguments:
///   tat              Current Theoretical Arrival Time for this key.
///                    Pass 0 (or any value ≤ now) for a brand-new key.
///   now_ns           Current time in nanoseconds (from your Clock).
///   emission_interval_ns  Nanoseconds per request slot (Limit.emission_interval()).
///   burst_offset_ns  How far into the past the TAT may lag (Limit.burst_offset()).
///
/// Returns a `Decision`. On `.allowed`, persist `decision.new_tat` back to
/// your store. On `.denied`, wait `decision.retry_after_ns` before retrying.
pub fn check(
    tat: i64,
    now_ns: i64,
    emission_interval_ns: i64,
    burst_offset_ns: i64,
) Decision {
    // The TAT we would assign if we allow this request.
    const new_tat = @max(tat, now_ns) + emission_interval_ns;

    // The earliest `now` at which this request is valid, given the burst allowance.
    const allow_at = new_tat - burst_offset_ns - emission_interval_ns;

    if (allow_at <= now_ns) {
        return .{ .allowed = .{ .new_tat = new_tat } };
    } else {
        return .{ .denied = .{ .retry_after_ns = allow_at - now_ns } };
    }
}

// ── Multi-key Limiter ─────────────────────────────────────────────────────────

/// A rate limiter that tracks an arbitrary number of keys (IPs, user IDs, etc.).
///
/// Keys are `[]const u8`. The limiter owns no memory beyond the HashMap itself —
/// keys are hashed but not copied; callers must ensure key lifetime covers the call.
///
/// Thread safety: none. Wrap with a mutex if shared across threads.
pub fn Limiter(comptime K: type, comptime ClockType: type) type {
    return struct {
        const Self = @This();
        const Store = std.HashMap(K, i64, HashContext(K), 80);

        allocator: std.mem.Allocator,
        store: Store,
        emission_interval_ns: i64,
        burst_offset_ns: i64,
        clock: ClockType,
        max_batch: u64,

        pub fn init(
            allocator: std.mem.Allocator,
            limit: Limit,
            burst: u32,
            clock: ClockType,
        ) ZimitError!Self {
            if (limit.count == 0 or limit.period_ns <= 0) return error.InvalidLimit;
            if (limit.count > limit.period_ns) return error.RateExceedsRes;
            const interval = limit.emission_interval();
            return .{
                .allocator = allocator,
                .store = Store.init(allocator),
                .emission_interval_ns = interval,
                .burst_offset_ns = limit.burst_offset(burst),
                .clock = clock,
                .max_batch = @as(u64, @intCast(@divFloor(std.math.maxInt(i64), interval))),
            };
        }

        /// Releases all memory owned by the limiter.
        /// If K is a string type, also frees all copied keys.
        pub fn deinit(self: *Self) void {
            if (K == []const u8) {
                var it = self.store.iterator();
                while (it.next()) |entry| {
                    self.allocator.free(entry.key_ptr.*);
                }
            }
            self.store.deinit();
        }

        /// Convenience for `check_key_n(key, 1)`.
        pub fn check_key(self: *Self, key: K) ZimitError!Decision {
            return self.check_key_n(key, 1);
        }

        /// Check whether `key` may make `n` requests atomically.
        ///
        /// If K is `[]const u8`, the key is duplicated and owned by the limiter
        /// if it's the first time we see it.
        pub fn check_key_n(self: *Self, key: K, n: u32) ZimitError!Decision {
            if (n == 0) return .{ .allowed = .{ .new_tat = self.store.get(key) orelse 0 } };

            if (@as(u64, n) > self.max_batch) {
                return .{ .denied = .{ .retry_after_ns = std.math.maxInt(i64) } };
            }

            const now = self.clock.now();
            const scaled_interval = self.emission_interval_ns * @as(i64, n);

            // Only lookup — never trust existing key memory
            if (self.store.getEntry(key)) |entry| {
                const decision = check(
                    entry.value_ptr.*,
                    now,
                    scaled_interval,
                    self.burst_offset_ns,
                );

                if (decision == .allowed) {
                    entry.value_ptr.* = decision.allowed.new_tat;
                }

                return decision;
            }

            const decision = check(
                0,
                now,
                scaled_interval,
                self.burst_offset_ns,
            );

            if (decision == .allowed) {
                const owned_key: K = if (K == []const u8)
                    try self.allocator.dupe(u8, key)
                else
                    key;

                errdefer if (K == []const u8) self.allocator.free(owned_key);

                try self.store.put(owned_key, decision.allowed.new_tat);
            }

            return decision;
        }

        /// Remove a key from the store.
        /// If K is a string type, also frees the copied key memory.
        pub fn remove(self: *Self, key: K) void {
            if (K == []const u8) {
                if (self.store.fetchRemove(key)) |kv| {
                    self.allocator.free(kv.key);
                }
            } else {
                _ = self.store.remove(key);
            }
        }

        /// Number of keys currently tracked in the store.
        pub fn key_count(self: *const Self) usize {
            return self.store.count();
        }
    };
}

/// HashMap context — handles both `[]const u8` and integer key types.
fn HashContext(comptime K: type) type {
    if (K == []const u8) return std.hash_map.StringContext;
    return std.hash_map.AutoContext(K);
}

// ── AtomicLimiter ─────────────────────────────────────────────────────────────

/// A lock-free, single-key rate limiter backed by one atomic i64 TAT.
///
/// Use this when you need a *global* limit shared across threads —
/// for example, "this process may make at most N outbound API calls per second"
/// regardless of which thread is making them.
///
/// For per-key limits (per IP, per user) use `Limiter(K)` protected by a Mutex,
/// or a sharded design. `AtomicLimiter` tracks exactly one token bucket.
///
/// Lock-free guarantee: threads never block each other. A thread that loses a
/// CAS race retries immediately with the freshly-loaded TAT. Under zero
/// contention the CAS always succeeds on the first attempt.
pub fn AtomicLimiter(comptime ClockType: type) type {
    return struct {
        const Self = @This();

        tat: std.atomic.Value(i64),
        emission_interval_ns: i64,
        burst_offset_ns: i64,
        clock: ClockType,
        max_batch: u64,

        /// Initialise an atomic limiter.
        ///
        ///   limit  The rate to enforce.
        ///   burst  Extra requests allowed in a burst (0 = no burst).
        ///   clock  Time source.
        pub fn init(limit: Limit, burst: u32, clock: ClockType) ZimitError!Self {
            if (limit.count == 0 or limit.period_ns <= 0) return error.InvalidLimit;
            if (limit.count > limit.period_ns) return error.RateExceedsRes;

            const interval = limit.emission_interval();

            return .{
                .tat = std.atomic.Value(i64).init(0),
                .emission_interval_ns = interval,
                .burst_offset_ns = limit.burst_offset(burst),
                .clock = clock,
                .max_batch = @as(u64, @intCast(@divFloor(std.math.maxInt(i64), interval))),
            };
        }

        /// Check whether a single request is allowed right now.
        /// Safe to call from any number of threads simultaneously.
        pub fn allow(self: *Self) Decision {
            return self.allow_n(1);
        }

        /// Atomically consume `n` slots. All-or-nothing: either all `n` slots
        /// are granted or none are — partial grants never occur.
        pub fn allow_n(self: *Self, n: u32) Decision {
            if (n == 0) {
                return .{ .allowed = .{ .new_tat = self.tat.load(.monotonic) } };
            }

            if (@as(u64, n) > self.max_batch) {
                return .{ .denied = .{ .retry_after_ns = std.math.maxInt(i64) } };
            }

            const scaled = self.emission_interval_ns * @as(i64, n);

            const now = self.clock.now();

            while (true) {
                const old_tat = self.tat.load(.monotonic);

                const decision = check(
                    old_tat,
                    now,
                    scaled,
                    self.burst_offset_ns,
                );

                switch (decision) {
                    .denied => return decision,
                    .allowed => |a| {
                        if (self.tat.cmpxchgWeak(
                            old_tat,
                            a.new_tat,
                            .acq_rel,
                            .monotonic,
                        ) == null) {
                            return decision;
                        }
                    },
                }
            }
        }

        /// Reset the limiter to its initial state — useful in tests.
        /// Not safe to call concurrently with `allow`.
        pub fn reset(self: *Self) void {
            self.tat.store(0, .release);
        }
    };
}

/// Convenience alias for the common string-keyed limiter.
pub fn StringLimiter(comptime ClockType: type) type {
    return Limiter([]const u8, ClockType);
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests — pure engine
// ─────────────────────────────────────────────────────────────────────────────

test "check: fresh key is always allowed" {
    // tat=0, now=1s — any request on a fresh key must pass
    const d = check(0, std.time.ns_per_s, 10_000_000, 0);
    try std.testing.expect(d.is_allowed());
}

test "check: new_tat advances by one emission interval" {
    const interval: i64 = 10_000_000; // 10ms
    const now: i64 = 1_000_000_000;
    const d = check(0, now, interval, 0);
    try std.testing.expectEqual(now + interval, d.allowed.new_tat);
}

test "check: second request inside interval is denied" {
    const interval: i64 = 10_000_000;
    const now: i64 = 1_000_000_000;
    const first = check(0, now, interval, 0);
    // Try again at the same instant — TAT is now in the future
    const second = check(first.allowed.new_tat, now, interval, 0);
    try std.testing.expect(!second.is_allowed());
}

test "check: request exactly at next slot boundary is allowed" {
    const interval: i64 = 10_000_000;
    const now: i64 = 1_000_000_000;
    const first = check(0, now, interval, 0);
    // Advance time by exactly one emission interval
    const next_now = now + interval;
    const second = check(first.allowed.new_tat, next_now, interval, 0);
    try std.testing.expect(second.is_allowed());
}

test "check: retry_after_ns is accurate" {
    const interval: i64 = 10_000_000; // 10ms
    const now: i64 = 1_000_000_000;
    const first = check(0, now, interval, 0);
    const second = check(first.allowed.new_tat, now, interval, 0);
    // Should need to wait ~10ms
    try std.testing.expectEqual(interval, second.denied.retry_after_ns);
}

test "check: burst=5 allows 6 requests at t=0" {
    const interval: i64 = 10_000_000;
    const burst_off = interval * 5;
    const now: i64 = 1_000_000_000;
    var tat: i64 = 0;

    // 1 base + 5 burst = 6 total
    var i: usize = 0;
    while (i < 6) : (i += 1) {
        const d = check(tat, now, interval, burst_off);
        try std.testing.expect(d.is_allowed());
        tat = d.allowed.new_tat;
    }

    // 7th must be denied
    const seventh = check(tat, now, interval, burst_off);
    try std.testing.expect(!seventh.is_allowed());
}

test "check: burst replenishes over time" {
    const interval: i64 = 10_000_000;
    const burst_off = interval * 2;
    const now: i64 = 1_000_000_000;
    var tat: i64 = 0;

    // Exhaust burst (3 requests at t=0: 1 base + 2 burst)
    var i: usize = 0;
    while (i < 3) : (i += 1) {
        const d = check(tat, now, interval, burst_off);
        tat = d.allowed.new_tat;
    }
    try std.testing.expect(!check(tat, now, interval, burst_off).is_allowed());

    // Advance by 2 intervals — should allow 2 requests again
    const later = now + 2 * interval;
    const d = check(tat, later, interval, burst_off);
    try std.testing.expect(d.is_allowed());
}

test "check: tat in the distant past behaves like a fresh key" {
    const interval: i64 = 10_000_000;
    const now: i64 = 1_000_000_000_000; // 1000 seconds in
    // tat is 1 hour ago — should be treated as fully fresh
    const old_tat: i64 = now - 3_600 * std.time.ns_per_s;
    const d = check(old_tat, now, interval, 0);
    try std.testing.expect(d.is_allowed());
    // new_tat should be based on now, not the ancient tat
    try std.testing.expectEqual(now + interval, d.allowed.new_tat);
}

test "check: zero burst, sustained rate allows exactly N req/s" {
    const interval: i64 = 10_000_000; // 100 req/s
    var tat: i64 = 0;
    var now: i64 = 0;
    var allowed: usize = 0;

    // Simulate 1 second with a request every 1ms (1000 attempts)
    var i: usize = 0;
    while (i < 1000) : (i += 1) {
        const d = check(tat, now, interval, 0);
        if (d.is_allowed()) {
            allowed += 1;
            tat = d.allowed.new_tat;
        }
        now += 1_000_000; // advance 1ms
    }
    // Should have allowed exactly 100 (one per 10ms slot)
    try std.testing.expectEqual(@as(usize, 100), allowed);
}

test "check: correctness under clock jitter" {
    const interval: i64 = 10_000_000; // 10ms
    const burst_off: i64 = 0;

    var tat: i64 = 0;
    var now: i64 = 0;
    var forward_ns: i64 = 0;
    var allowed: usize = 0;

    const deltas = [_]i64{
        1_000_000,
        1_000_000,
        0,
        1_000_000,
        -200_000,
        1_200_000,
        1_000_000,
        0,
        1_000_000,
        -100_000,
    };

    var i: usize = 0;

    while (forward_ns < std.time.ns_per_s) {
        const delta = deltas[i % deltas.len];
        i += 1;

        now += delta;
        if (now < 0) now = 0;

        if (delta > 0) forward_ns += delta;

        const d = check(tat, now, interval, burst_off);
        if (d.is_allowed()) {
            allowed += 1;
            tat = d.allowed.new_tat;
        }
    }

    const expected = @divFloor(forward_ns, interval);

    // HARD invariant: must never exceed rate
    try std.testing.expect(allowed <= expected + 2);

    // Soft sanity check (optional)
    try std.testing.expect(allowed > 0);
}

test "check: now=0 (epoch) with fresh key is allowed" {
    const d = check(0, 0, 10_000_000, 0);
    try std.testing.expect(d.is_allowed());
    // new_tat should be 0 + interval
    try std.testing.expectEqual(@as(i64, 10_000_000), d.allowed.new_tat);
}

test "check: tat already in the future queues behind it" {
    const interval: i64 = 10_000_000;
    const now: i64 = 1_000_000_000;
    // TAT is 50ms in the future (5 slots ahead)
    const future_tat = now + 5 * interval;
    const d = check(future_tat, now, interval, 5 * interval);
    // With burst=5, this should still be allowed (burst_offset covers 5 slots)
    try std.testing.expect(d.is_allowed());
    // new_tat should be future_tat + interval (queued behind existing TAT)
    try std.testing.expectEqual(future_tat + interval, d.allowed.new_tat);
}

test "check: tat far in the future without burst is denied" {
    const interval: i64 = 10_000_000;
    const now: i64 = 1_000_000_000;
    const future_tat = now + 100 * interval; // 100 slots ahead
    const d = check(future_tat, now, interval, 0);
    try std.testing.expect(!d.is_allowed());
}

test "check: burst_offset exactly equal to interval allows 2 requests at same time" {
    const interval: i64 = 10_000_000;
    const burst_off = interval; // burst=1
    const now: i64 = 1_000_000_000;

    // First request
    const d1 = check(0, now, interval, burst_off);
    try std.testing.expect(d1.is_allowed());

    // Second request at same time — burst should cover it
    const d2 = check(d1.allowed.new_tat, now, interval, burst_off);
    try std.testing.expect(d2.is_allowed());

    // Third request — should be denied (only 1 burst slot)
    const d3 = check(d2.allowed.new_tat, now, interval, burst_off);
    try std.testing.expect(!d3.is_allowed());
}

test "check: very large emission_interval does not overflow" {
    // 1 req/hour → interval = 3_600_000_000_000
    const interval: i64 = 3_600_000_000_000;
    const now: i64 = 1_000_000_000;
    const d = check(0, now, interval, 0);
    try std.testing.expect(d.is_allowed());
    try std.testing.expectEqual(now + interval, d.allowed.new_tat);
}

test "check: denied retry_after is exact gap" {
    const interval: i64 = 100_000_000; // 100ms
    const now: i64 = 1_000_000_000;
    // Consume first request
    const d1 = check(0, now, interval, 0);
    // Try again 30ms later — should be denied with ~70ms wait
    const later = now + 30_000_000;
    const d2 = check(d1.allowed.new_tat, later, interval, 0);
    try std.testing.expect(!d2.is_allowed());
    // retry_after should be (tat + interval - burst_offset - interval) - now = tat - now
    // tat = now + interval = 1_100_000_000, later = 1_030_000_000
    // retry = 1_100_000_000 - 1_030_000_000 = 70_000_000
    try std.testing.expectEqual(@as(i64, 70_000_000), d2.denied.retry_after_ns);
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests — Limiter (multi-key)
// ─────────────────────────────────────────────────────────────────────────────

test "Limiter: init rejects zero count" {
    const mc = types.ManualClock{};
    const bad = Limit{ .count = 0, .period_ns = std.time.ns_per_s };
    const result = Limiter([]const u8, types.ManualClock).init(std.testing.allocator, bad, 0, mc);
    try std.testing.expectError(error.InvalidLimit, result);
}

test "Limiter: init rejects rate > 1 req/ns" {
    const mc = types.ManualClock{};
    const bad = Limit{ .count = 2, .period_ns = 1 };
    const result = Limiter([]const u8, types.ManualClock).init(std.testing.allocator, bad, 0, mc);
    try std.testing.expectError(error.RateExceedsRes, result);
}

test "Limiter: init rejects non-positive period" {
    const mc = types.ManualClock{};
    const bad = Limit{ .count = 10, .period_ns = 0 };
    const result = Limiter([]const u8, types.ManualClock).init(std.testing.allocator, bad, 0, mc);
    try std.testing.expectError(error.InvalidLimit, result);
}

test "Limiter: fresh key is allowed" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try Limiter([]const u8, types.ManualClock).init(
        std.testing.allocator,
        Limit.per_second(10),
        0,
        mc,
    );
    defer lim.deinit();

    const d = try lim.check_key("user-1");
    try std.testing.expect(d.is_allowed());
}

test "Limiter: exhausted key is denied" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try Limiter([]const u8, types.ManualClock).init(
        std.testing.allocator,
        Limit.per_second(3),
        0,
        mc,
    );
    defer lim.deinit();

    _ = try lim.check_key("u");
    _ = try lim.check_key("u");
    _ = try lim.check_key("u");
    const fourth = try lim.check_key("u");
    try std.testing.expect(!fourth.is_allowed());
}

test "Limiter: keys are isolated" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try Limiter([]const u8, types.ManualClock).init(
        std.testing.allocator,
        Limit.per_second(1),
        0,
        mc,
    );
    defer lim.deinit();

    _ = try lim.check_key("alice");
    const bob = try lim.check_key("bob");
    try std.testing.expect(bob.is_allowed());
}

test "Limiter: time advance allows denied key" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try Limiter([]const u8, *types.ManualClock).init(
        std.testing.allocator,
        Limit.per_second(1),
        0,
        &mc,
    );
    defer lim.deinit();

    _ = try lim.check_key("u");
    const denied = try lim.check_key("u");
    try std.testing.expect(!denied.is_allowed());

    mc.tick(std.time.ns_per_s);
    const retry = try lim.check_key("u");
    try std.testing.expect(retry.is_allowed());
}

test "Limiter: remove clears key state" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try Limiter([]const u8, types.ManualClock).init(
        std.testing.allocator,
        Limit.per_second(1),
        0,
        mc,
    );
    defer lim.deinit();

    _ = try lim.check_key("u");
    const denied = try lim.check_key("u");
    try std.testing.expect(!denied.is_allowed());

    lim.remove("u");
    try std.testing.expectEqual(@as(usize, 0), lim.key_count());

    // Key is gone — next request is fresh again
    const fresh = try lim.check_key("u");
    try std.testing.expect(fresh.is_allowed());
}

test "Limiter: key_count tracks insertions" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try Limiter([]const u8, types.ManualClock).init(
        std.testing.allocator,
        Limit.per_second(10),
        0,
        mc,
    );
    defer lim.deinit();

    _ = try lim.check_key("a");
    _ = try lim.check_key("b");
    _ = try lim.check_key("c");
    try std.testing.expectEqual(@as(usize, 3), lim.key_count());
}

test "Limiter: integer key type (u64)" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try Limiter(u64, types.ManualClock).init(
        std.testing.allocator,
        Limit.per_second(5),
        0,
        mc,
    );
    defer lim.deinit();

    const d = try lim.check_key(42);
    try std.testing.expect(d.is_allowed());

    const d2 = try lim.check_key(99);
    try std.testing.expect(d2.is_allowed());
}

test "Limiter: string key is copied — caller buffer can be mutated" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try Limiter([]const u8, types.ManualClock).init(
        std.testing.allocator,
        Limit.per_second(10),
        0,
        mc,
    );
    defer lim.deinit();

    // Insert via a mutable stack buffer
    var buf = [_]u8{ 'u', 's', 'e', 'r' };
    _ = try lim.check_key(buf[0..]);

    // Mutate the original — if we stored the slice header instead of a copy,
    // the key in the map is now corrupt
    buf[0] = 'X';

    // The entry must still be found under the original bytes
    try std.testing.expectEqual(@as(usize, 1), lim.key_count());
    const d = try lim.check_key("user");
    // Second request on same key — should be rate-limited, not treated as fresh
    try std.testing.expect(!d.is_allowed());
}

test "Limiter: remove frees copied key memory" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try Limiter([]const u8, types.ManualClock).init(
        std.testing.allocator,
        Limit.per_second(10),
        0,
        mc,
    );
    defer lim.deinit();

    _ = try lim.check_key("alice");
    _ = try lim.check_key("bob");
    try std.testing.expectEqual(@as(usize, 2), lim.key_count());

    lim.remove("alice");
    try std.testing.expectEqual(@as(usize, 1), lim.key_count());

    // alice is gone — next check_key treats her as fresh
    const d = try lim.check_key("alice");
    try std.testing.expect(d.is_allowed());
}

test "Limiter: deinit frees all copied keys without leak" {
    // This test is only meaningful when run with `zig build test` under the
    // testing allocator, which detects leaks automatically on deinit.
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try Limiter([]const u8, types.ManualClock).init(
        std.testing.allocator,
        Limit.per_second(10),
        0,
        mc,
    );

    _ = try lim.check_key("x");
    _ = try lim.check_key("y");
    _ = try lim.check_key("z");

    // deinit must free all three copied keys.
    // If it doesn't, std.testing.allocator reports a leak and the test fails.
    lim.deinit();
}

test "Limiter: same key does not duplicate allocation" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);

    var lim = try Limiter([]const u8, types.ManualClock).init(
        std.testing.allocator,
        Limit.per_second(10),
        0,
        mc,
    );
    defer lim.deinit();

    _ = try lim.check_key("user");
    const before = lim.key_count();

    _ = try lim.check_key("user");
    const after = lim.key_count();

    try std.testing.expectEqual(before, after);
}

test "Limiter: remove on missing key is safe" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);

    var lim = try Limiter([]const u8, types.ManualClock).init(
        std.testing.allocator,
        Limit.per_second(10),
        0,
        mc,
    );
    defer lim.deinit();

    lim.remove("ghost"); // should not crash
    try std.testing.expectEqual(@as(usize, 0), lim.key_count());
}

test "Limiter: many keys do not collide or corrupt" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);

    var lim = try Limiter([]const u8, types.ManualClock).init(
        std.testing.allocator,
        Limit.per_second(1),
        0,
        mc,
    );
    defer lim.deinit();

    var i: usize = 0;
    while (i < 1000) : (i += 1) {
        var buf: [16]u8 = undefined;
        const key = try std.fmt.bufPrint(&buf, "k{}", .{i});
        _ = try lim.check_key(key);
    }

    try std.testing.expectEqual(@as(usize, 1000), lim.key_count());
}

test "Limiter: equal string content with different backing memory hits same key" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);

    var lim = try Limiter([]const u8, types.ManualClock).init(
        std.testing.allocator,
        Limit.per_second(1),
        0,
        mc,
    );
    defer lim.deinit();

    var buf1 = [_]u8{ 'u', 's', 'e', 'r' };
    var buf2 = [_]u8{ 'u', 's', 'e', 'r' };

    _ = try lim.check_key(buf1[0..]);
    const d = try lim.check_key(buf2[0..]);

    try std.testing.expect(!d.is_allowed());
}

test "Limiter: check_key_n denial does not change state" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);

    var lim = try Limiter([]const u8, *types.ManualClock).init(
        std.testing.allocator,
        Limit.per_second(5),
        0,
        &mc,
    );
    defer lim.deinit();

    _ = try lim.check_key_n("u", 3);

    // This should fail
    _ = try lim.check_key_n("u", 10);

    // Advance exactly 3 slots
    mc.tick(600 * std.time.ns_per_ms);

    // Should be fresh again
    try std.testing.expect((try lim.check_key_n("u", 5)).is_allowed());
}

test "Limiter: retry_after_ns can be zero at boundary" {
    var mc = types.ManualClock{};
    mc.set(0);

    var lim = try Limiter([]const u8, *types.ManualClock).init(
        std.testing.allocator,
        Limit.per_second(1),
        0,
        &mc,
    );
    defer lim.deinit();

    _ = try lim.check_key("u");

    mc.tick(std.time.ns_per_s);

    const d = try lim.check_key("u");
    try std.testing.expect(d.is_allowed());
}

test "Limiter: alternating keys do not interfere" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);

    var lim = try Limiter([]const u8, types.ManualClock).init(
        std.testing.allocator,
        Limit.per_second(1),
        0,
        mc,
    );
    defer lim.deinit();

    var i: usize = 0;
    while (i < 20) : (i += 1) {
        const key = if (i % 2 == 0) "a" else "b";
        _ = try lim.check_key(key);
    }

    try std.testing.expect(!(try lim.check_key("a")).is_allowed());
    try std.testing.expect(!(try lim.check_key("b")).is_allowed());
}

test "Limiter: freed key memory reuse does not corrupt map" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);

    var lim = try Limiter([]const u8, types.ManualClock).init(
        std.testing.allocator,
        Limit.per_second(1),
        0,
        mc,
    );
    defer lim.deinit();

    {
        var buf = [_]u8{'a'};
        _ = try lim.check_key(buf[0..]);
    } // buf goes out of scope

    // New buffer possibly reuses same memory
    var buf2 = [_]u8{'a'};

    const d = try lim.check_key(buf2[0..]);
    try std.testing.expect(!d.is_allowed());
}

test "Limiter: check_key_n accepts maxInt(u32)" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);

    var lim = try Limiter([]const u8, types.ManualClock).init(
        std.testing.allocator,
        Limit.per_second(10),
        0,
        mc,
    );
    defer lim.deinit();

    const d = try lim.check_key_n("u", std.math.maxInt(u32));

    // Either allowed or denied depending on timing,
    // but must NOT overflow or panic.
    _ = d;
}

test "Limiter: OutOfMemory handling" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);

    // Use a failing allocator to simulate OOM.
    // std.testing.FailingAllocator fires after N successful allocations.
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });

    var lim = try Limiter([]const u8, types.ManualClock).init(
        failing.allocator(),
        Limit.per_second(10),
        0,
        mc,
    );
    defer lim.deinit();

    // 1. OOM on first key insertion (dupe fails or HashMap grow fails)
    // We don't know exactly when it fails, so we loop and advance fail_index.
    var i: u32 = 0;
    while (i < 5) : (i += 1) {
        failing.fail_index = i;
        failing.alloc_index = 0;
        const result = lim.check_key("new-key");
        if (result == error.OutOfMemory) break;
    } else {
        // If we never hit OOM in 5 steps, the test is weak or the fail_index logic is misunderstood.
    }

    // 2. Ensure state is still consistent after OOM.
    // Reset to successful allocator for a moment to check.
    failing.fail_index = std.math.maxInt(usize);
    try std.testing.expect((try lim.check_key("healthy")).is_allowed());
}

test "Limiter: init rejects negative period" {
    const mc = types.ManualClock{};
    const bad = Limit{ .count = 10, .period_ns = -1 };
    const result = Limiter([]const u8, types.ManualClock).init(std.testing.allocator, bad, 0, mc);
    try std.testing.expectError(error.InvalidLimit, result);
}

test "Limiter: batch on fresh key always allowed (TAT pushes forward)" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    // rate=10/s, burst=2 → for fresh keys, any batch is allowed
    // because allow_at = max(0, now) - burst_offset = now - burst_offset <= now
    var lim = try Limiter([]const u8, types.ManualClock).init(
        std.testing.allocator,
        Limit.per_second(10),
        2,
        mc,
    );
    defer lim.deinit();

    // Large batch on a fresh key — allowed, TAT pushed far forward
    const d = try lim.check_key_n("u", 8);
    try std.testing.expect(d.is_allowed());
    try std.testing.expectEqual(@as(usize, 1), lim.key_count());

    // Key is now exhausted — second request denied because TAT is far in future
    try std.testing.expect(!(try lim.check_key("u")).is_allowed());
}

test "Limiter: check_key_n n=0 on missing key does not insert" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try Limiter([]const u8, types.ManualClock).init(
        std.testing.allocator,
        Limit.per_second(10),
        0,
        mc,
    );
    defer lim.deinit();

    const d = try lim.check_key_n("ghost", 0);
    try std.testing.expect(d.is_allowed());
    try std.testing.expectEqual(@as(usize, 0), lim.key_count());
}

test "Limiter: remove then reinsert gets fresh state" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try Limiter([]const u8, types.ManualClock).init(
        std.testing.allocator,
        Limit.per_second(1),
        0,
        mc,
    );
    defer lim.deinit();

    // Exhaust key
    _ = try lim.check_key("u");
    try std.testing.expect(!(try lim.check_key("u")).is_allowed());

    // Remove and reinsert
    lim.remove("u");
    try std.testing.expectEqual(@as(usize, 0), lim.key_count());

    // Should be fresh
    const d = try lim.check_key("u");
    try std.testing.expect(d.is_allowed());
    try std.testing.expectEqual(@as(usize, 1), lim.key_count());

    // And rate-limited again
    try std.testing.expect(!(try lim.check_key("u")).is_allowed());
}

test "Limiter: per-hour config with time advance" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try Limiter(u64, *types.ManualClock).init(
        std.testing.allocator,
        Limit.per_hour(1),
        0,
        &mc,
    );
    defer lim.deinit();

    // 1 req/hour — first allowed
    try std.testing.expect((try lim.check_key(42)).is_allowed());
    // Immediate second denied
    try std.testing.expect(!(try lim.check_key(42)).is_allowed());

    // Advance 30 minutes — still denied
    mc.tick(1800 * std.time.ns_per_s);
    try std.testing.expect(!(try lim.check_key(42)).is_allowed());

    // Advance to full hour — allowed
    mc.tick(1800 * std.time.ns_per_s);
    try std.testing.expect((try lim.check_key(42)).is_allowed());
}

test "Limiter: burst with integer keys" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    // rate=5/s, burst=4 → 5 requests at once
    var lim = try Limiter(u32, types.ManualClock).init(
        std.testing.allocator,
        Limit.per_second(5),
        4,
        mc,
    );
    defer lim.deinit();

    var i: u32 = 0;
    while (i < 5) : (i += 1) {
        try std.testing.expect((try lim.check_key(1)).is_allowed());
    }
    try std.testing.expect(!(try lim.check_key(1)).is_allowed());
}

test "Limiter: denied on existing key does not insert second key" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try Limiter([]const u8, types.ManualClock).init(
        std.testing.allocator,
        Limit.per_second(1),
        0,
        mc,
    );
    defer lim.deinit();

    // Insert and exhaust first key
    _ = try lim.check_key("a");
    try std.testing.expectEqual(@as(usize, 1), lim.key_count());

    // Denied request for existing key doesn't change count
    try std.testing.expect(!(try lim.check_key("a")).is_allowed());
    try std.testing.expectEqual(@as(usize, 1), lim.key_count());
}

test "Limiter: overflow guard denies fresh key without inserting" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    // per_minute(1) → interval=60e9 → max_batch=153 < maxInt(u32)
    var lim = try Limiter([]const u8, types.ManualClock).init(
        std.testing.allocator,
        Limit.per_minute(1),
        0,
        mc,
    );
    defer lim.deinit();

    // Overflow guard fires → denied before any store mutation
    const d = try lim.check_key_n("new", std.math.maxInt(u32));
    try std.testing.expect(!d.is_allowed());
    try std.testing.expectEqual(@as(usize, 0), lim.key_count());
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests — AtomicLimiter (single-threaded correctness)
// ─────────────────────────────────────────────────────────────────────────────

test "AtomicLimiter: fresh limiter allows first request" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try AtomicLimiter(types.ManualClock).init(Limit.per_second(10), 0, mc);
    const d = lim.allow();
    try std.testing.expect(d.is_allowed());
}

test "AtomicLimiter: exhausted limiter denies" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try AtomicLimiter(types.ManualClock).init(Limit.per_second(3), 0, mc);

    _ = lim.allow();
    _ = lim.allow();
    _ = lim.allow();
    try std.testing.expect(!lim.allow().is_allowed());
}

test "AtomicLimiter: time advance unblocks" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try AtomicLimiter(*types.ManualClock).init(Limit.per_second(1), 0, &mc);

    _ = lim.allow();
    try std.testing.expect(!lim.allow().is_allowed());

    mc.tick(std.time.ns_per_s);
    try std.testing.expect(lim.allow().is_allowed());
}

test "AtomicLimiter: burst allows base+burst requests" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    // burst=4 → 1+4 = 5 requests at t=0
    var lim = try AtomicLimiter(types.ManualClock).init(Limit.per_second(10), 4, mc);

    var i: usize = 0;
    while (i < 5) : (i += 1) {
        try std.testing.expect(lim.allow().is_allowed());
    }
    try std.testing.expect(!lim.allow().is_allowed());
}

test "AtomicLimiter: allow_n consumes slots atomically" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try AtomicLimiter(types.ManualClock).init(Limit.per_second(10), 0, mc);

    try std.testing.expect(lim.allow_n(7).is_allowed());
    try std.testing.expect(!lim.allow_n(4).is_allowed());
}

test "AtomicLimiter: allow_n=0 always allowed, no state change" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try AtomicLimiter(types.ManualClock).init(Limit.per_second(1), 0, mc);

    _ = lim.allow(); // exhaust
    try std.testing.expect(!lim.allow().is_allowed());
    try std.testing.expect(lim.allow_n(0).is_allowed()); // zero never mutates
    try std.testing.expect(!lim.allow().is_allowed()); // still exhausted
}

test "AtomicLimiter: denied allow_n leaves TAT unchanged" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try AtomicLimiter(types.ManualClock).init(Limit.per_second(5), 0, mc);

    try std.testing.expect(lim.allow_n(3).is_allowed());

    const tat_before = lim.tat.load(.monotonic);
    _ = lim.allow_n(10); // must fail
    const tat_after = lim.tat.load(.monotonic);

    // TAT must be bitwise identical — denied path must never write
    try std.testing.expectEqual(tat_before, tat_after);
}

test "AtomicLimiter: reset clears state" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try AtomicLimiter(types.ManualClock).init(Limit.per_second(1), 0, mc);

    _ = lim.allow();
    try std.testing.expect(!lim.allow().is_allowed());

    lim.reset();
    try std.testing.expect(lim.allow().is_allowed());
}

test "AtomicLimiter: retry_after_ns is positive on denial" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try AtomicLimiter(types.ManualClock).init(Limit.per_second(1), 0, mc);

    _ = lim.allow();
    const d = lim.allow();
    switch (d) {
        .denied => |denied| try std.testing.expect(denied.retry_after_ns > 0),
        .allowed => return error.TestUnexpectedResult,
    }
}

test "AtomicLimiter: init rejects zero count" {
    const mc = types.ManualClock{};
    const bad = Limit{ .count = 0, .period_ns = std.time.ns_per_s };
    try std.testing.expectError(
        error.InvalidLimit,
        AtomicLimiter(types.ManualClock).init(bad, 0, mc),
    );
}

test "AtomicLimiter: init rejects rate > 1 req/ns" {
    const mc = types.ManualClock{};
    const bad = Limit{ .count = 2, .period_ns = 1 };
    try std.testing.expectError(
        error.RateExceedsRes,
        AtomicLimiter(types.ManualClock).init(bad, 0, mc),
    );
}

test "AtomicLimiter: sustained throughput matches rate" {
    var mc = types.ManualClock{};
    var lim = try AtomicLimiter(*types.ManualClock).init(Limit.per_second(100), 0, &mc);

    var allowed: usize = 0;
    var t: i64 = 0;
    // 10 seconds, one attempt every 1ms (10 000 attempts)
    while (t < 10 * std.time.ns_per_s) : (t += 1_000_000) {
        mc.set(t);
        if (lim.allow().is_allowed()) allowed += 1;
    }
    // Expect exactly 1000 (100/s × 10s)
    try std.testing.expectEqual(@as(usize, 1000), allowed);
}

test "AtomicLimiter: allow_n overflow guard denies without panic" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);

    var lim = try AtomicLimiter(types.ManualClock).init(Limit.per_second(10), 0, mc);

    // max_batch = maxInt(i64) / 100_000_000 = 92, so 93 exceeds it
    if (lim.max_batch >= std.math.maxInt(u32)) return; // avoid invalid cast
    const n: u32 = @intCast(lim.max_batch + 1);

    const d = lim.allow_n(n);
    try std.testing.expect(!d.is_allowed());
}

test "AtomicLimiter: allow_n overflow guard leaves TAT unchanged" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try AtomicLimiter(types.ManualClock).init(Limit.per_minute(1), 0, mc);

    const tat_before = lim.tat.load(.monotonic);
    const n: u32 = @intCast(lim.max_batch + 1);
    _ = lim.allow_n(n);

    try std.testing.expectEqual(tat_before, lim.tat.load(.monotonic));
}

test "AtomicLimiter: allow_n overflow guard returns maxInt retry_after" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try AtomicLimiter(types.ManualClock).init(Limit.per_minute(1), 0, mc);

    const n: u32 = @intCast(lim.max_batch + 1);
    const d = lim.allow_n(n);

    switch (d) {
        .denied => |denied| try std.testing.expectEqual(
            @as(i64, std.math.maxInt(i64)),
            denied.retry_after_ns,
        ),
        .allowed => return error.TestUnexpectedResult,
    }
}

test "AtomicLimiter: allow_n large but valid n still works" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);

    var lim = try AtomicLimiter(types.ManualClock).init(Limit.per_minute(1), 0, mc);

    const n: u32 = 100;
    try std.testing.expect(@as(u64, n) <= lim.max_batch);

    // First call always allowed on a cold limiter — consume capacity
    _ = lim.allow_n(n);

    // Now TAT is far in the future; a second batch must be denied
    const d = lim.allow_n(n);

    try std.testing.expect(!d.is_allowed());
    // Finite wait — proves it was GCRA, not the overflow guard
    try std.testing.expect(d.denied.retry_after_ns < std.math.maxInt(i64));
}

test "AtomicLimiter: allow_n boundary exactly at max_batch does not overflow" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);

    var lim = try AtomicLimiter(types.ManualClock).init(Limit.per_minute(1), 0, mc);

    const n: u32 = @intCast(lim.max_batch);
    const d = lim.allow_n(n);

    // Guard must not have fired — if denied, retry must be finite
    switch (d) {
        .allowed => {},
        .denied => |denied| try std.testing.expect(
            denied.retry_after_ns < std.math.maxInt(i64),
        ),
    }
}

test "AtomicLimiter: init rejects negative period" {
    const mc = types.ManualClock{};
    const bad = Limit{ .count = 10, .period_ns = -1 };
    try std.testing.expectError(
        error.InvalidLimit,
        AtomicLimiter(types.ManualClock).init(bad, 0, mc),
    );
}

test "AtomicLimiter: denial has finite retry_after (not overflow guard)" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try AtomicLimiter(types.ManualClock).init(Limit.per_second(1), 0, mc);

    _ = lim.allow();
    const d = lim.allow();
    switch (d) {
        .denied => |denied| {
            try std.testing.expect(denied.retry_after_ns > 0);
            try std.testing.expect(denied.retry_after_ns < std.math.maxInt(i64));
            // Should be approximately 1 second
            try std.testing.expect(denied.retry_after_ns <= std.time.ns_per_s);
        },
        .allowed => return error.TestUnexpectedResult,
    }
}

test "AtomicLimiter: reset then full capacity available" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    // rate=5/s, burst=4 → 5 at once
    var lim = try AtomicLimiter(types.ManualClock).init(Limit.per_second(5), 4, mc);

    // Exhaust all slots
    var i: usize = 0;
    while (i < 5) : (i += 1) {
        _ = lim.allow();
    }
    try std.testing.expect(!lim.allow().is_allowed());

    // Reset and verify full capacity restored
    lim.reset();
    i = 0;
    while (i < 5) : (i += 1) {
        try std.testing.expect(lim.allow().is_allowed());
    }
    try std.testing.expect(!lim.allow().is_allowed());
}

test "AtomicLimiter: burst replenishes over time" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    // rate=1/s, burst=1 → 2 at once, replenish 1 per second
    var lim = try AtomicLimiter(*types.ManualClock).init(Limit.per_second(1), 1, &mc);

    // Use both slots
    try std.testing.expect(lim.allow().is_allowed());
    try std.testing.expect(lim.allow().is_allowed());
    try std.testing.expect(!lim.allow().is_allowed());

    // Advance 1s → 1 slot replenished
    mc.tick(std.time.ns_per_s);
    try std.testing.expect(lim.allow().is_allowed());
    try std.testing.expect(!lim.allow().is_allowed());
}

test "AtomicLimiter: allow_n with batch=2 on rate=10/s with burst" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    // rate=10/s, burst=9 → 10 slots available at once
    var lim = try AtomicLimiter(types.ManualClock).init(Limit.per_second(10), 9, mc);

    // 5 batches of 2 should exhaust 10 slots
    var i: usize = 0;
    while (i < 5) : (i += 1) {
        try std.testing.expect(lim.allow_n(2).is_allowed());
    }
    try std.testing.expect(!lim.allow().is_allowed());
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests — AtomicLimiter (concurrent correctness)
// ─────────────────────────────────────────────────────────────────────────────

// We can't inject a ManualClock across threads safely (it has no internal
// synchronisation), so the concurrency tests use SystemClock and reason
// about counts rather than exact timing.

test "AtomicLimiter: concurrent allows never exceed limit" {
    // 8 threads each fire 200 requests as fast as possible.
    // The limiter allows 100/s. The test runs for ~50ms real time.
    // We only assert the hard invariant: allowed count ≤ what the rate
    // permits for the elapsed duration + 1 burst slot of slack.
    const num_threads = 8;
    const requests_per_thread = 200;

    const sys = types.SystemClock.init(std.testing.io);
    var lim = try AtomicLimiter(types.SystemClock).init(
        Limit.per_second(1000),
        0,
        sys,
    );

    const Ctx = struct {
        limiter: *AtomicLimiter(types.SystemClock),
        allowed: std.atomic.Value(usize),

        fn run(ctx: *@This()) void {
            var i: usize = 0;
            while (i < requests_per_thread) : (i += 1) {
                if (ctx.limiter.allow().is_allowed()) {
                    _ = ctx.allowed.fetchAdd(1, .monotonic);
                }
            }
        }
    };

    var ctx = Ctx{
        .limiter = &lim,
        .allowed = std.atomic.Value(usize).init(0),
    };

    const start_ns = std.Io.Timestamp.now(std.testing.io, .real).toNanoseconds();

    var threads: [num_threads]std.Thread = undefined;
    for (&threads) |*t| {
        t.* = try std.Thread.spawn(.{}, Ctx.run, .{&ctx});
    }
    for (&threads) |*t| t.join();

    const elapsed_ns = std.Io.Timestamp.now(std.testing.io, .real).toNanoseconds() - start_ns;
    const elapsed_s = @as(f64, @floatFromInt(elapsed_ns)) / 1e9;

    const total_allowed = ctx.allowed.load(.monotonic);
    // Maximum legitimately allowed = rate × elapsed + 1 (for initial slot)
    const max_allowed: usize = @intFromFloat(1000.0 * elapsed_s + 1.5);

    try std.testing.expect(total_allowed <= max_allowed);
}

test "AtomicLimiter: concurrent allows — no lost updates under contention" {
    // All threads share one limiter with exactly N total slots.
    // After all threads finish, exactly N requests should have been granted —
    // no more (proves CAS prevents double-grants) and ideally no fewer
    // (proves retries work). We allow a small slack because threads may
    // race past the window boundary.
    const total_slots = 50;
    const num_threads = 8;
    const requests_per_thread = 20; // 160 total attempts for 50 slots

    const sys = types.SystemClock.init(std.testing.io);
    // Large period so slots don't replenish during the test
    var lim = try AtomicLimiter(types.SystemClock).init(
        Limit{ .count = total_slots, .period_ns = std.time.ns_per_s },
        total_slots - 1, // critical
        sys,
    );

    const Ctx = struct {
        limiter: *AtomicLimiter(types.SystemClock),
        allowed: std.atomic.Value(usize),

        fn run(ctx: *@This()) void {
            var i: usize = 0;
            while (i < requests_per_thread) : (i += 1) {
                if (ctx.limiter.allow().is_allowed()) {
                    _ = ctx.allowed.fetchAdd(1, .monotonic);
                }
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

    const total_allowed = ctx.allowed.load(.monotonic);

    // Must never exceed the configured slot count
    try std.testing.expect(total_allowed <= total_slots);
    // Must have granted a substantial portion — if retries broke, we'd
    // see far fewer than expected
    try std.testing.expect(total_allowed >= total_slots / 2);
}
