//! GCRA engine and multi-key rate limiter for zimit.
//!
//! The engine itself (`check`) is a pure function — no allocation, no I/O,
//! no global state. The `Limiter` struct wraps it with a HashMap key store.

const std = @import("std");
const types = @import("types.zig");

pub const Limit = types.Limit;
pub const Decision = types.Decision;
pub const Clock = types.Clock;
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
/// (Step 5 will add an atomic single-key variant for lock-free hot paths.)
pub fn Limiter(comptime K: type) type {
    return struct {
        const Self = @This();
        const Store = std.HashMap(K, i64, HashContext(K), 80);

        allocator: std.mem.Allocator,
        store: Store,
        emission_interval_ns: i64,
        burst_offset_ns: i64,
        clock: Clock,

        /// Initialise a limiter.
        ///
        ///   allocator  Used for the internal HashMap.
        ///   limit      The rate to enforce.
        ///   burst      Extra requests allowed in a burst (0 = no burst).
        ///   clock      Time source. Use SystemClock in prod, ManualClock in tests.
        pub fn init(
            allocator: std.mem.Allocator,
            limit: Limit,
            burst: u32,
            clock: Clock,
        ) ZimitError!Self {
            if (limit.count == 0 or limit.period_ns <= 0) return error.InvalidLimit;
            return .{
                .allocator = allocator,
                .store = Store.init(allocator),
                .emission_interval_ns = limit.emission_interval(),
                .burst_offset_ns = limit.burst_offset(burst),
                .clock = clock,
            };
        }

        pub fn deinit(self: *Self) void {
            self.store.deinit();
        }

        /// Check `key` against the rate limit and update state atomically.
        /// Returns a `Decision` — caller decides what to do with `.denied`.
        pub fn check_key(self: *Self, key: K) ZimitError!Decision {
            const now = self.clock.now();
            const tat = self.store.get(key) orelse 0;

            const decision = check(
                tat,
                now,
                self.emission_interval_ns,
                self.burst_offset_ns,
            );

            if (decision == .allowed) {
                try self.store.put(key, decision.allowed.new_tat);
            }

            return decision;
        }

        /// Remove a key from the store (e.g. when a user session ends).
        pub fn remove(self: *Self, key: K) void {
            _ = self.store.remove(key);
        }

        /// Current number of tracked keys.
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

/// Convenience alias for the common string-keyed limiter.
pub const StringLimiter = Limiter([]const u8);

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
    const interval: i64 = 10_000_000; // 10ms → 100 req/s
    const burst_off = interval * 5; // burst of 5 extra
    const now: i64 = 1_000_000_000;
    var tat: i64 = 0;

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

// ─────────────────────────────────────────────────────────────────────────────
// Tests — Limiter (multi-key)
// ─────────────────────────────────────────────────────────────────────────────

test "Limiter: init rejects zero count" {
    var mc = types.ManualClock{};
    const bad = Limit{ .count = 0, .period_ns = std.time.ns_per_s };
    const result = StringLimiter.init(std.testing.allocator, bad, 0, mc.clock());
    try std.testing.expectError(error.InvalidLimit, result);
}

test "Limiter: init rejects non-positive period" {
    var mc = types.ManualClock{};
    const bad = Limit{ .count = 10, .period_ns = 0 };
    const result = StringLimiter.init(std.testing.allocator, bad, 0, mc.clock());
    try std.testing.expectError(error.InvalidLimit, result);
}

test "Limiter: fresh key is allowed" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try StringLimiter.init(
        std.testing.allocator,
        Limit.per_second(10),
        0,
        mc.clock(),
    );
    defer lim.deinit();

    const d = try lim.check_key("user-1");
    try std.testing.expect(d.is_allowed());
}

test "Limiter: exhausted key is denied" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try StringLimiter.init(
        std.testing.allocator,
        Limit.per_second(3),
        0,
        mc.clock(),
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
    var lim = try StringLimiter.init(
        std.testing.allocator,
        Limit.per_second(1),
        0,
        mc.clock(),
    );
    defer lim.deinit();

    _ = try lim.check_key("alice");
    const bob = try lim.check_key("bob");
    try std.testing.expect(bob.is_allowed());
}

test "Limiter: time advance allows denied key" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try StringLimiter.init(
        std.testing.allocator,
        Limit.per_second(1),
        0,
        mc.clock(),
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
    var lim = try StringLimiter.init(
        std.testing.allocator,
        Limit.per_second(1),
        0,
        mc.clock(),
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
    var lim = try StringLimiter.init(
        std.testing.allocator,
        Limit.per_second(10),
        0,
        mc.clock(),
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
    var lim = try Limiter(u64).init(
        std.testing.allocator,
        Limit.per_second(5),
        0,
        mc.clock(),
    );
    defer lim.deinit();

    const d = try lim.check_key(42);
    try std.testing.expect(d.is_allowed());

    const d2 = try lim.check_key(99);
    try std.testing.expect(d2.is_allowed());
}
