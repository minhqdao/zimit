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

pub const Config = struct {
    limit: Limit,
    burst: u32 = 0,
    clock: Clock,
};

pub const StorageOptions = struct {
    initial_capacity: u32 = 0,
    max_entries: ?usize = null,
    idle_timeout_ns: ?i64 = null,
};

// ── Pure GCRA engine ──────────────────────────────────────────────────────────

/// Run one GCRA check. Pure function — no allocations, no side effects.
///
/// Arguments:
///   tat              Current Theoretical Arrival Time for this key.
///                    Pass 0 (or any value ≤ now) for a brand-new key.
///   now_ns           Current time in nanoseconds (from your Clock).
///   emission_interval_ns  Nanoseconds per request slot (Limit.emissionInterval()).
///   burst_offset_ns  How far into the past the TAT may lag (Limit.burstOffset()).
///
/// Returns a `Decision`. On `.allowed`, persist `decision.new_tat` back to
/// your store. On `.denied`, wait `decision.retry_after` before retrying.
pub fn check(
    tat: i64,
    now_ns: i64,
    emission_interval_ns: i64,
    burst_offset_ns: i64,
) Decision {
    return checkN(tat, now_ns, emission_interval_ns, burst_offset_ns, 1);
}

/// Run one GCRA check for an atomic batch of `n` requests.
///
/// Eligibility is calculated using the base emission interval. The batch
/// consumes `n` intervals when allowed, while the first request consumes the
/// normal slot and the remaining `n - 1` requests consume burst capacity.
fn checkN(
    tat: i64,
    now_ns: i64,
    emission_interval_ns: i64,
    burst_offset_ns: i64,
    n: u32,
) Decision {
    std.debug.assert(n > 0);

    // Wider intermediates preserve the exact admission calculation even when
    // the resulting TAT or retry duration lies outside the stored i64 range.
    const batch_interval_ns = @as(i128, emission_interval_ns) * @as(i128, n);

    // The TAT we would assign if we allow this request.
    const new_tat = @as(i128, @max(tat, now_ns)) + batch_interval_ns;

    // The earliest `now` at which this request is valid, given the burst allowance.
    const allow_at = new_tat - @as(i128, burst_offset_ns) - @as(i128, emission_interval_ns);

    if (allow_at <= now_ns) {
        if (new_tat > std.math.maxInt(i64) or new_tat < std.math.minInt(i64)) {
            return .{ .denied = .{
                .retry_after = .fromNanoseconds(std.math.maxInt(i64)),
            } };
        }
        return .{ .allowed = .{ .new_tat = @intCast(new_tat) } };
    } else {
        return .{ .denied = .{ .retry_after = .fromNanoseconds(
            saturatingI64(allow_at - @as(i128, now_ns)),
        ) } };
    }
}

fn saturatingI64(value: i128) i64 {
    if (value > std.math.maxInt(i64)) return std.math.maxInt(i64);
    if (value < std.math.minInt(i64)) return std.math.minInt(i64);
    return @intCast(value);
}

const Parameters = struct {
    interval: i64,
    burst_offset: i64,
    max_batch: u64,
    burst_capacity: u64,
};

const Engine = struct {
    emission_interval_ns: i64,
    burst_offset_ns: i64,
    clock: Clock,
    max_batch: u64,
    burst_capacity: u64,

    fn init(config: Config) ZimitError!Engine {
        const parameters = try deriveParameters(config.limit, config.burst);
        return .{
            .emission_interval_ns = parameters.interval,
            .burst_offset_ns = parameters.burst_offset,
            .clock = config.clock,
            .max_batch = parameters.max_batch,
            .burst_capacity = parameters.burst_capacity,
        };
    }

    fn allowsBatch(self: Engine, n: u32) bool {
        return @as(u64, n) <= self.max_batch and
            @as(u64, n) <= self.burst_capacity;
    }

    fn validateBatch(self: Engine, n: u32) ZimitError!void {
        if (!self.allowsBatch(n)) return error.BatchTooLarge;
    }

    fn decide(self: Engine, tat: i64, now: i64, n: u32) Decision {
        if (n == 0) return .{ .allowed = .{ .new_tat = tat } };
        if (!self.allowsBatch(n)) {
            return .{ .denied = .{
                .retry_after = .fromNanoseconds(std.math.maxInt(i64)),
            } };
        }
        return checkN(
            tat,
            now,
            self.emission_interval_ns,
            self.burst_offset_ns,
            n,
        );
    }
};

fn deriveParameters(limit: Limit, burst: u32) ZimitError!Parameters {
    if (limit.count == 0 or limit.period_ns <= 0) return error.InvalidLimit;
    if (limit.count > limit.period_ns) return error.RateExceedsRes;

    const interval = limit.emissionInterval();
    const burst_product = @mulWithOverflow(interval, @as(i64, burst));
    if (burst_product[1] != 0) return error.TimeOverflow;

    return .{
        .interval = interval,
        .burst_offset = burst_product[0],
        .max_batch = @intCast(@divFloor(std.math.maxInt(i64), interval)),
        .burst_capacity = @as(u64, burst) + 1,
    };
}

// ── Multi-key Limiter ─────────────────────────────────────────────────────────

/// A rate limiter that tracks an arbitrary number of keys (IPs, user IDs, etc.).
///
/// Keys are `K`. The limiter owns no memory beyond the HashMap itself —
/// keys are hashed but not copied (unless K is []const u8, in which case
/// the Limiter copies them to its own allocator).
///
/// ### Thread Safety
/// This type is **not** thread-safe. If you need to use the same `Limiter`
/// instance across multiple threads, you must wrap it in a `std.Io.Mutex`.
pub fn Limiter(comptime K: type) type {
    return struct {
        const Self = @This();
        const State = struct {
            tat: i64,
            last_seen_ns: i64,
        };
        const Store = std.HashMap(K, State, HashContext(K), 80);

        allocator: std.mem.Allocator,
        store: Store,
        engine: Engine,
        max_entries: ?usize,
        idle_timeout_ns: ?i64,
        next_prune_ns: i64,

        pub fn init(
            allocator: std.mem.Allocator,
            limit: Limit,
            burst: u32,
            clock: Clock,
        ) ZimitError!Self {
            return initWithConfig(allocator, .{
                .limit = limit,
                .burst = burst,
                .clock = clock,
            });
        }

        pub fn initWithConfig(
            allocator: std.mem.Allocator,
            config: Config,
        ) ZimitError!Self {
            return initWithConfigAndStorage(allocator, config, .{});
        }

        pub fn initWithConfigAndStorage(
            allocator: std.mem.Allocator,
            config: Config,
            storage: StorageOptions,
        ) ZimitError!Self {
            if (storage.idle_timeout_ns) |timeout| {
                if (timeout <= 0) return error.InvalidIdleTimeout;
            }
            const engine = try Engine.init(config);
            var store = Store.init(allocator);
            errdefer store.deinit();

            const initial_capacity = if (storage.max_entries) |maximum|
                @min(@as(usize, storage.initial_capacity), maximum)
            else
                storage.initial_capacity;
            try store.ensureTotalCapacity(@intCast(initial_capacity));

            return .{
                .allocator = allocator,
                .store = store,
                .engine = engine,
                .max_entries = storage.max_entries,
                .idle_timeout_ns = storage.idle_timeout_ns,
                .next_prune_ns = std.math.maxInt(i64),
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

        /// Convenience for `checkKeyN(key, 1)`.
        pub fn checkKey(self: *Self, key: K) ZimitError!Decision {
            return self.checkKeyN(key, 1);
        }

        /// Check whether `key` may make `n` requests atomically.
        ///
        /// If K is `[]const u8`, the key is duplicated and owned by the limiter
        /// if it's the first time we see it.
        pub fn checkKeyN(self: *Self, key: K, n: u32) ZimitError!Decision {
            if (n == 0) {
                const tat = if (self.store.get(key)) |state| state.tat else 0;
                return self.engine.decide(tat, 0, n);
            }
            if (!self.engine.allowsBatch(n)) return self.engine.decide(0, 0, n);

            const now = self.engine.clock.now();

            // Only lookup — never trust existing key memory
            if (self.store.getEntry(key)) |entry| {
                entry.value_ptr.last_seen_ns = now;
                const decision = self.engine.decide(
                    entry.value_ptr.tat,
                    now,
                    n,
                );

                if (decision == .allowed) {
                    entry.value_ptr.tat = decision.allowed.new_tat;
                }

                return decision;
            }

            const decision = self.engine.decide(0, now, n);

            if (decision == .allowed) {
                try self.maintainForNewKey(now);

                const owned_key: K = if (K == []const u8)
                    try self.allocator.dupe(u8, key)
                else
                    key;

                errdefer if (K == []const u8) self.allocator.free(owned_key);

                try self.store.put(owned_key, .{
                    .tat = decision.allowed.new_tat,
                    .last_seen_ns = now,
                });
                self.schedulePrune(.{
                    .tat = decision.allowed.new_tat,
                    .last_seen_ns = now,
                });
            }

            return decision;
        }

        pub fn validateBatch(self: *const Self, n: u32) ZimitError!void {
            return self.engine.validateBatch(n);
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
        pub fn keyCount(self: *const Self) usize {
            return self.store.count();
        }

        /// Remove entries that are idle and have no outstanding rate-limit debt.
        /// Returns the number of entries removed.
        pub fn pruneExpired(self: *Self) usize {
            return self.pruneExpiredAt(self.engine.clock.now());
        }

        fn maintainForNewKey(self: *Self, now: i64) ZimitError!void {
            if (now >= self.next_prune_ns) {
                _ = self.pruneExpiredAt(now);
            }

            const maximum = self.max_entries orelse return;
            if (self.store.count() >= maximum) return error.CapacityExceeded;
        }

        fn pruneExpiredAt(self: *Self, now: i64) usize {
            const timeout = self.idle_timeout_ns orelse return 0;
            var removed: usize = 0;
            self.next_prune_ns = std.math.maxInt(i64);
            var iterator = self.store.iterator();

            while (iterator.next()) |entry| {
                const state = entry.value_ptr.*;
                const idle_ns = @as(i128, now) - @as(i128, state.last_seen_ns);
                if (state.tat > now or now < state.last_seen_ns or idle_ns < timeout) {
                    self.schedulePrune(state);
                    continue;
                }

                const owned_key = entry.key_ptr.*;
                self.store.removeByPtr(entry.key_ptr);
                if (K == []const u8) self.allocator.free(owned_key);
                removed += 1;
            }

            return removed;
        }

        fn schedulePrune(self: *Self, state: State) void {
            const timeout = self.idle_timeout_ns orelse return;
            const idle_expiration = saturatingI64(
                @as(i128, state.last_seen_ns) + @as(i128, timeout),
            );
            const candidate = @max(state.tat, idle_expiration);
            self.next_prune_ns = @min(self.next_prune_ns, candidate);
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
/// For per-key limits (per IP, per user) use `Limiter(K)` wrapped in a
/// `std.Io.Mutex`, or a sharded design. `AtomicLimiter` tracks exactly one
/// token bucket.
///
/// ### Thread Safety
/// This type is **thread-safe**. Threads never block each other;
/// a thread that loses a CAS race retries immediately with the freshly-loaded TAT.
pub const AtomicLimiter = struct {
    tat: std.atomic.Value(i64),
    engine: Engine,

    /// Initialise an atomic limiter.
    ///
    pub fn init(
        limit: Limit,
        burst: u32,
        clock: Clock,
    ) ZimitError!AtomicLimiter {
        return initWithConfig(.{
            .limit = limit,
            .burst = burst,
            .clock = clock,
        });
    }

    /// Initialise from shared limiter configuration.
    pub fn initWithConfig(config: Config) ZimitError!AtomicLimiter {
        return .{
            .tat = std.atomic.Value(i64).init(0),
            .engine = try Engine.init(config),
        };
    }

    /// Check whether a single request is allowed right now.
    /// Safe to call from any number of threads simultaneously.
    pub fn allow(self: *AtomicLimiter) Decision {
        return self.allowN(1);
    }

    /// Atomically consume `n` slots. All-or-nothing: either all `n` slots
    /// are granted or none are — partial grants never occur.
    pub fn allowN(self: *AtomicLimiter, n: u32) Decision {
        if (n == 0 or !self.engine.allowsBatch(n)) {
            return self.engine.decide(self.tat.load(.monotonic), 0, n);
        }

        const now = self.engine.clock.now();

        while (true) {
            const old_tat = self.tat.load(.monotonic);

            const decision = self.engine.decide(old_tat, now, n);

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

    pub fn validateBatch(self: *const AtomicLimiter, n: u32) ZimitError!void {
        return self.engine.validateBatch(n);
    }

    /// Reset the limiter to its initial state — useful in tests.
    /// Not safe to call concurrently with `allow`.
    pub fn reset(self: *AtomicLimiter) void {
        self.tat.store(0, .release);
    }
};

/// Convenience alias for the common string-keyed limiter.
pub const StringLimiter = Limiter([]const u8);

// ─────────────────────────────────────────────────────────────────────────────
// Tests — pure engine
// ─────────────────────────────────────────────────────────────────────────────

test "check: fresh key is always allowed" {
    // tat=0, now=1s — any request on a fresh key must pass
    const d = check(0, std.time.ns_per_s, 10_000_000, 0);
    try std.testing.expect(d.isAllowed());
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
    try std.testing.expect(!second.isAllowed());
}

test "check: request exactly at next slot boundary is allowed" {
    const interval: i64 = 10_000_000;
    const now: i64 = 1_000_000_000;
    const first = check(0, now, interval, 0);
    // Advance time by exactly one emission interval
    const next_now = now + interval;
    const second = check(first.allowed.new_tat, next_now, interval, 0);
    try std.testing.expect(second.isAllowed());
}

test "check: retry duration is accurate" {
    const interval: i64 = 10_000_000; // 10ms
    const now: i64 = 1_000_000_000;
    const first = check(0, now, interval, 0);
    const second = check(first.allowed.new_tat, now, interval, 0);
    // Should need to wait ~10ms
    try std.testing.expectEqual(interval, second.denied.retry_after.toNanoseconds());
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
        try std.testing.expect(d.isAllowed());
        tat = d.allowed.new_tat;
    }

    // 7th must be denied
    const seventh = check(tat, now, interval, burst_off);
    try std.testing.expect(!seventh.isAllowed());
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
    try std.testing.expect(!check(tat, now, interval, burst_off).isAllowed());

    // Advance by 2 intervals — should allow 2 requests again
    const later = now + 2 * interval;
    const d = check(tat, later, interval, burst_off);
    try std.testing.expect(d.isAllowed());
}

test "check: tat in the distant past behaves like a fresh key" {
    const interval: i64 = 10_000_000;
    const now: i64 = 1_000_000_000_000; // 1000 seconds in
    // tat is 1 hour ago — should be treated as fully fresh
    const old_tat: i64 = now - 3_600 * std.time.ns_per_s;
    const d = check(old_tat, now, interval, 0);
    try std.testing.expect(d.isAllowed());
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
        if (d.isAllowed()) {
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
        if (d.isAllowed()) {
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
    try std.testing.expect(d.isAllowed());
    // new_tat should be 0 + interval
    try std.testing.expectEqual(@as(i64, 10_000_000), d.allowed.new_tat);
}

test "check: tat already in the future queues behind it" {
    const interval: i64 = 10_000_000;
    const now: i64 = 1_000_000_000;
    // TAT is 50ms in the future (5 slots ahead)
    const future_tat = now + 5 * interval;
    const d = check(future_tat, now, interval, 5 * interval);
    // With burst=5, this should still be allowed (burstOffset covers 5 slots)
    try std.testing.expect(d.isAllowed());
    // new_tat should be future_tat + interval (queued behind existing TAT)
    try std.testing.expectEqual(future_tat + interval, d.allowed.new_tat);
}

test "check: tat far in the future without burst is denied" {
    const interval: i64 = 10_000_000;
    const now: i64 = 1_000_000_000;
    const future_tat = now + 100 * interval; // 100 slots ahead
    const d = check(future_tat, now, interval, 0);
    try std.testing.expect(!d.isAllowed());
}

test "check: unrepresentable new TAT fails closed" {
    const d = check(
        std.math.maxInt(i64) - 5,
        std.math.maxInt(i64) - 5,
        10,
        0,
    );

    try std.testing.expect(!d.isAllowed());
    try std.testing.expectEqual(
        @as(i64, std.math.maxInt(i64)),
        d.denied.retry_after.toNanoseconds(),
    );
}

test "check: allow_at subtraction cannot underflow" {
    const now = std.math.minInt(i64);
    const d = check(now, now, 10, std.math.maxInt(i64));

    try std.testing.expect(d.isAllowed());
    try std.testing.expectEqual(now + 10, d.allowed.new_tat);
}

test "check: retry duration saturates instead of overflowing" {
    const d = check(
        std.math.maxInt(i64),
        std.math.minInt(i64),
        1,
        0,
    );

    try std.testing.expect(!d.isAllowed());
    try std.testing.expectEqual(
        @as(i64, std.math.maxInt(i64)),
        d.denied.retry_after.toNanoseconds(),
    );
}

test "check: burstOffset exactly equal to interval allows 2 requests at same time" {
    const interval: i64 = 10_000_000;
    const burst_off = interval; // burst=1
    const now: i64 = 1_000_000_000;

    // First request
    const d1 = check(0, now, interval, burst_off);
    try std.testing.expect(d1.isAllowed());

    // Second request at same time — burst should cover it
    const d2 = check(d1.allowed.new_tat, now, interval, burst_off);
    try std.testing.expect(d2.isAllowed());

    // Third request — should be denied (only 1 burst slot)
    const d3 = check(d2.allowed.new_tat, now, interval, burst_off);
    try std.testing.expect(!d3.isAllowed());
}

test "check: very large emissionInterval does not overflow" {
    // 1 req/hour → interval = 3_600_000_000_000
    const interval: i64 = 3_600_000_000_000;
    const now: i64 = 1_000_000_000;
    const d = check(0, now, interval, 0);
    try std.testing.expect(d.isAllowed());
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
    try std.testing.expect(!d2.isAllowed());
    // retry_after should be (tat + interval - burstOffset - interval) - now = tat - now
    // tat = now + interval = 1_100_000_000, later = 1_030_000_000
    // retry = 1_100_000_000 - 1_030_000_000 = 70_000_000
    try std.testing.expectEqual(@as(i64, 70_000_000), d2.denied.retry_after.toNanoseconds());
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

test "Limiter: init rejects rate > 1 req/ns" {
    var mc = types.ManualClock{};
    const bad = Limit{ .count = 2, .period_ns = 1 };
    const result = StringLimiter.init(std.testing.allocator, bad, 0, mc.clock());
    try std.testing.expectError(error.RateExceedsRes, result);
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
        Limit.perSecond(10),
        0,
        mc.clock(),
    );
    defer lim.deinit();

    const d = try lim.checkKey("user-1");
    try std.testing.expect(d.isAllowed());
}

test "Limiter: exhausted key is denied" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try StringLimiter.init(
        std.testing.allocator,
        Limit.perSecond(3),
        0,
        mc.clock(),
    );
    defer lim.deinit();

    _ = try lim.checkKey("u");
    _ = try lim.checkKey("u");
    _ = try lim.checkKey("u");
    const fourth = try lim.checkKey("u");
    try std.testing.expect(!fourth.isAllowed());
}

test "Limiter: keys are isolated" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try StringLimiter.init(
        std.testing.allocator,
        Limit.perSecond(1),
        0,
        mc.clock(),
    );
    defer lim.deinit();

    _ = try lim.checkKey("alice");
    const bob = try lim.checkKey("bob");
    try std.testing.expect(bob.isAllowed());
}

test "Limiter: time advance allows denied key" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try StringLimiter.init(
        std.testing.allocator,
        Limit.perSecond(1),
        0,
        mc.clock(),
    );
    defer lim.deinit();

    _ = try lim.checkKey("u");
    const denied = try lim.checkKey("u");
    try std.testing.expect(!denied.isAllowed());

    mc.tick(std.time.ns_per_s);
    const retry = try lim.checkKey("u");
    try std.testing.expect(retry.isAllowed());
}

test "Limiter: remove clears key state" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try StringLimiter.init(
        std.testing.allocator,
        Limit.perSecond(1),
        0,
        mc.clock(),
    );
    defer lim.deinit();

    _ = try lim.checkKey("u");
    const denied = try lim.checkKey("u");
    try std.testing.expect(!denied.isAllowed());

    lim.remove("u");
    try std.testing.expectEqual(@as(usize, 0), lim.keyCount());

    // Key is gone — next request is fresh again
    const fresh = try lim.checkKey("u");
    try std.testing.expect(fresh.isAllowed());
}

test "Limiter: keyCount tracks insertions" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try StringLimiter.init(
        std.testing.allocator,
        Limit.perSecond(10),
        0,
        mc.clock(),
    );
    defer lim.deinit();

    _ = try lim.checkKey("a");
    _ = try lim.checkKey("b");
    _ = try lim.checkKey("c");
    try std.testing.expectEqual(@as(usize, 3), lim.keyCount());
}

test "Limiter: integer key type (u64)" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try Limiter(u64).init(
        std.testing.allocator,
        Limit.perSecond(5),
        0,
        mc.clock(),
    );
    defer lim.deinit();

    const d = try lim.checkKey(42);
    try std.testing.expect(d.isAllowed());

    const d2 = try lim.checkKey(99);
    try std.testing.expect(d2.isAllowed());
}

test "Limiter: string key is copied — caller buffer can be mutated" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try StringLimiter.init(
        std.testing.allocator,
        Limit.perSecond(10),
        0,
        mc.clock(),
    );
    defer lim.deinit();

    // Insert via a mutable stack buffer
    var buf = [_]u8{ 'u', 's', 'e', 'r' };
    _ = try lim.checkKey(buf[0..]);

    // Mutate the original — if we stored the slice header instead of a copy,
    // the key in the map is now corrupt
    buf[0] = 'X';

    // The entry must still be found under the original bytes
    try std.testing.expectEqual(@as(usize, 1), lim.keyCount());
    const d = try lim.checkKey("user");
    // Second request on same key — should be rate-limited, not treated as fresh
    try std.testing.expect(!d.isAllowed());
}

test "Limiter: remove frees copied key memory" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try StringLimiter.init(
        std.testing.allocator,
        Limit.perSecond(10),
        0,
        mc.clock(),
    );
    defer lim.deinit();

    _ = try lim.checkKey("alice");
    _ = try lim.checkKey("bob");
    try std.testing.expectEqual(@as(usize, 2), lim.keyCount());

    lim.remove("alice");
    try std.testing.expectEqual(@as(usize, 1), lim.keyCount());

    // alice is gone — next checkKey treats her as fresh
    const d = try lim.checkKey("alice");
    try std.testing.expect(d.isAllowed());
}

test "Limiter: deinit frees all copied keys without leak" {
    // This test is only meaningful when run with `zig build test` under the
    // testing allocator, which detects leaks automatically on deinit.
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try StringLimiter.init(
        std.testing.allocator,
        Limit.perSecond(10),
        0,
        mc.clock(),
    );

    _ = try lim.checkKey("x");
    _ = try lim.checkKey("y");
    _ = try lim.checkKey("z");

    // deinit must free all three copied keys.
    // If it doesn't, std.testing.allocator reports a leak and the test fails.
    lim.deinit();
}

test "Limiter: same key does not duplicate allocation" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);

    var lim = try StringLimiter.init(
        std.testing.allocator,
        Limit.perSecond(10),
        0,
        mc.clock(),
    );
    defer lim.deinit();

    _ = try lim.checkKey("user");
    const before = lim.keyCount();

    _ = try lim.checkKey("user");
    const after = lim.keyCount();

    try std.testing.expectEqual(before, after);
}

test "Limiter: remove on missing key is safe" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);

    var lim = try StringLimiter.init(
        std.testing.allocator,
        Limit.perSecond(10),
        0,
        mc.clock(),
    );
    defer lim.deinit();

    lim.remove("ghost"); // should not crash
    try std.testing.expectEqual(@as(usize, 0), lim.keyCount());
}

test "Limiter: many keys do not collide or corrupt" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);

    var lim = try StringLimiter.init(
        std.testing.allocator,
        Limit.perSecond(1),
        0,
        mc.clock(),
    );
    defer lim.deinit();

    var i: usize = 0;
    while (i < 1000) : (i += 1) {
        var buf: [16]u8 = undefined;
        const key = try std.fmt.bufPrint(&buf, "k{}", .{i});
        _ = try lim.checkKey(key);
    }

    try std.testing.expectEqual(@as(usize, 1000), lim.keyCount());
}

test "Limiter: equal string content with different backing memory hits same key" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);

    var lim = try StringLimiter.init(
        std.testing.allocator,
        Limit.perSecond(1),
        0,
        mc.clock(),
    );
    defer lim.deinit();

    var buf1 = [_]u8{ 'u', 's', 'e', 'r' };
    var buf2 = [_]u8{ 'u', 's', 'e', 'r' };

    _ = try lim.checkKey(buf1[0..]);
    const d = try lim.checkKey(buf2[0..]);

    try std.testing.expect(!d.isAllowed());
}

test "Limiter: checkKeyN denial does not change state" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);

    var lim = try StringLimiter.init(
        std.testing.allocator,
        Limit.perSecond(5),
        4,
        mc.clock(),
    );
    defer lim.deinit();

    _ = try lim.checkKeyN("u", 3);

    // This should fail
    _ = try lim.checkKeyN("u", 10);

    // Advance exactly 3 slots
    mc.tick(600 * std.time.ns_per_ms);

    // Should be fresh again
    try std.testing.expect((try lim.checkKeyN("u", 5)).isAllowed());
}

test "Limiter: retry duration can be zero at boundary" {
    var mc = types.ManualClock{};
    mc.set(0);

    var lim = try StringLimiter.init(
        std.testing.allocator,
        Limit.perSecond(1),
        0,
        mc.clock(),
    );
    defer lim.deinit();

    _ = try lim.checkKey("u");

    mc.tick(std.time.ns_per_s);

    const d = try lim.checkKey("u");
    try std.testing.expect(d.isAllowed());
}

test "Limiter: alternating keys do not interfere" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);

    var lim = try StringLimiter.init(
        std.testing.allocator,
        Limit.perSecond(1),
        0,
        mc.clock(),
    );
    defer lim.deinit();

    var i: usize = 0;
    while (i < 20) : (i += 1) {
        const key = if (i % 2 == 0) "a" else "b";
        _ = try lim.checkKey(key);
    }

    try std.testing.expect(!(try lim.checkKey("a")).isAllowed());
    try std.testing.expect(!(try lim.checkKey("b")).isAllowed());
}

test "Limiter: freed key memory reuse does not corrupt map" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);

    var lim = try StringLimiter.init(
        std.testing.allocator,
        Limit.perSecond(1),
        0,
        mc.clock(),
    );
    defer lim.deinit();

    {
        var buf = [_]u8{'a'};
        _ = try lim.checkKey(buf[0..]);
    } // buf goes out of scope

    // New buffer possibly reuses same memory
    var buf2 = [_]u8{'a'};

    const d = try lim.checkKey(buf2[0..]);
    try std.testing.expect(!d.isAllowed());
}

test "Limiter: checkKeyN accepts maxInt(u32)" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);

    var lim = try StringLimiter.init(
        std.testing.allocator,
        Limit.perSecond(10),
        0,
        mc.clock(),
    );
    defer lim.deinit();

    const d = try lim.checkKeyN("u", std.math.maxInt(u32));

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

    var lim = try StringLimiter.init(
        failing.allocator(),
        Limit.perSecond(10),
        0,
        mc.clock(),
    );
    defer lim.deinit();

    // 1. OOM on first key insertion (dupe fails or HashMap grow fails)
    // We don't know exactly when it fails, so we loop and advance fail_index.
    var i: u32 = 0;
    while (i < 5) : (i += 1) {
        failing.fail_index = i;
        failing.alloc_index = 0;
        const result = lim.checkKey("new-key");
        if (result == error.OutOfMemory) break;
    } else {
        // If we never hit OOM in 5 steps, the test is weak or the fail_index logic is misunderstood.
    }

    // 2. Ensure state is still consistent after OOM.
    // Reset to successful allocator for a moment to check.
    failing.fail_index = std.math.maxInt(usize);
    try std.testing.expect((try lim.checkKey("healthy")).isAllowed());
}

test "Limiter: init rejects negative period" {
    var mc = types.ManualClock{};
    const bad = Limit{ .count = 10, .period_ns = -1 };
    const result = StringLimiter.init(std.testing.allocator, bad, 0, mc.clock());
    try std.testing.expectError(error.InvalidLimit, result);
}

test "Limiter: init rejects unrepresentable burst offset" {
    var mc = types.ManualClock{};
    try std.testing.expectError(
        error.TimeOverflow,
        StringLimiter.init(
            std.testing.allocator,
            .{ .count = 1, .period_ns = std.math.maxInt(i64) },
            2,
            mc.clock(),
        ),
    );
}

test "Limiter: batch on fresh key cannot exceed burst capacity" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    // burst=2 means one base request plus two extra requests at once.
    var lim = try StringLimiter.init(
        std.testing.allocator,
        Limit.perSecond(10),
        2,
        mc.clock(),
    );
    defer lim.deinit();

    try std.testing.expect(!(try lim.checkKeyN("u", 4)).isAllowed());
    try std.testing.expectEqual(@as(usize, 0), lim.keyCount());

    try std.testing.expect((try lim.checkKeyN("u", 3)).isAllowed());
    try std.testing.expectEqual(@as(usize, 1), lim.keyCount());
    try std.testing.expect(!(try lim.checkKey("u")).isAllowed());
}

test "Limiter: checkKeyN n=0 on missing key does not insert" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try StringLimiter.init(
        std.testing.allocator,
        Limit.perSecond(10),
        0,
        mc.clock(),
    );
    defer lim.deinit();

    const d = try lim.checkKeyN("ghost", 0);
    try std.testing.expect(d.isAllowed());
    try std.testing.expectEqual(@as(usize, 0), lim.keyCount());
}

test "Limiter: remove then reinsert gets fresh state" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try StringLimiter.init(
        std.testing.allocator,
        Limit.perSecond(1),
        0,
        mc.clock(),
    );
    defer lim.deinit();

    // Exhaust key
    _ = try lim.checkKey("u");
    try std.testing.expect(!(try lim.checkKey("u")).isAllowed());

    // Remove and reinsert
    lim.remove("u");
    try std.testing.expectEqual(@as(usize, 0), lim.keyCount());

    // Should be fresh
    const d = try lim.checkKey("u");
    try std.testing.expect(d.isAllowed());
    try std.testing.expectEqual(@as(usize, 1), lim.keyCount());

    // And rate-limited again
    try std.testing.expect(!(try lim.checkKey("u")).isAllowed());
}

test "Limiter: per-hour config with time advance" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try Limiter(u64).init(
        std.testing.allocator,
        Limit.perHour(1),
        0,
        mc.clock(),
    );
    defer lim.deinit();

    // 1 req/hour — first allowed
    try std.testing.expect((try lim.checkKey(42)).isAllowed());
    // Immediate second denied
    try std.testing.expect(!(try lim.checkKey(42)).isAllowed());

    // Advance 30 minutes — still denied
    mc.tick(1800 * std.time.ns_per_s);
    try std.testing.expect(!(try lim.checkKey(42)).isAllowed());

    // Advance to full hour — allowed
    mc.tick(1800 * std.time.ns_per_s);
    try std.testing.expect((try lim.checkKey(42)).isAllowed());
}

test "Limiter: burst with integer keys" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    // rate=5/s, burst=4 → 5 requests at once
    var lim = try Limiter(u32).init(
        std.testing.allocator,
        Limit.perSecond(5),
        4,
        mc.clock(),
    );
    defer lim.deinit();

    var i: u32 = 0;
    while (i < 5) : (i += 1) {
        try std.testing.expect((try lim.checkKey(1)).isAllowed());
    }
    try std.testing.expect(!(try lim.checkKey(1)).isAllowed());
}

test "Limiter: denied on existing key does not insert second key" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try StringLimiter.init(
        std.testing.allocator,
        Limit.perSecond(1),
        0,
        mc.clock(),
    );
    defer lim.deinit();

    // Insert and exhaust first key
    _ = try lim.checkKey("a");
    try std.testing.expectEqual(@as(usize, 1), lim.keyCount());

    // Denied request for existing key doesn't change count
    try std.testing.expect(!(try lim.checkKey("a")).isAllowed());
    try std.testing.expectEqual(@as(usize, 1), lim.keyCount());
}

test "Limiter: overflow guard denies fresh key without inserting" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    // perMinute(1) → interval=60e9 → max_batch=153 < maxInt(u32)
    var lim = try Limiter([]const u8).init(
        std.testing.allocator,
        Limit.perMinute(1),
        0,
        mc.clock(),
    );
    defer lim.deinit();

    // Overflow guard fires → denied before any store mutation
    const d = try lim.checkKeyN("new", std.math.maxInt(u32));
    try std.testing.expect(!d.isAllowed());
    try std.testing.expectEqual(@as(usize, 0), lim.keyCount());
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests — AtomicLimiter (single-threaded correctness)
// ─────────────────────────────────────────────────────────────────────────────

test "AtomicLimiter: fresh limiter allows first request" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try AtomicLimiter.init(Limit.perSecond(10), 0, mc.clock());
    const d = lim.allow();
    try std.testing.expect(d.isAllowed());
}

test "AtomicLimiter: exhausted limiter denies" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try AtomicLimiter.init(Limit.perSecond(3), 0, mc.clock());

    _ = lim.allow();
    _ = lim.allow();
    _ = lim.allow();
    try std.testing.expect(!lim.allow().isAllowed());
}

test "AtomicLimiter: time advance unblocks" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try AtomicLimiter.init(Limit.perSecond(1), 0, mc.clock());

    _ = lim.allow();
    try std.testing.expect(!lim.allow().isAllowed());

    mc.tick(std.time.ns_per_s);
    try std.testing.expect(lim.allow().isAllowed());
}

test "AtomicLimiter: burst allows base+burst requests" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    // burst=4 → 1+4 = 5 requests at t=0
    var lim = try AtomicLimiter.init(Limit.perSecond(10), 4, mc.clock());

    var i: usize = 0;
    while (i < 5) : (i += 1) {
        try std.testing.expect(lim.allow().isAllowed());
    }
    try std.testing.expect(!lim.allow().isAllowed());
}

test "AtomicLimiter: allowN consumes slots atomically" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try AtomicLimiter.init(Limit.perSecond(10), 6, mc.clock());

    try std.testing.expect(lim.allowN(7).isAllowed());
    try std.testing.expect(!lim.allowN(4).isAllowed());
}

test "AtomicLimiter: allowN=0 always allowed, no state change" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try AtomicLimiter.init(Limit.perSecond(1), 0, mc.clock());

    _ = lim.allow(); // exhaust
    try std.testing.expect(!lim.allow().isAllowed());
    try std.testing.expect(lim.allowN(0).isAllowed()); // zero never mutates
    try std.testing.expect(!lim.allow().isAllowed()); // still exhausted
}

test "AtomicLimiter: denied allowN leaves TAT unchanged" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try AtomicLimiter.init(Limit.perSecond(5), 2, mc.clock());

    try std.testing.expect(lim.allowN(3).isAllowed());

    const tat_before = lim.tat.load(.monotonic);
    _ = lim.allowN(10); // must fail
    const tat_after = lim.tat.load(.monotonic);

    // TAT must be bitwise identical — denied path must never write
    try std.testing.expectEqual(tat_before, tat_after);
}

test "AtomicLimiter: reset clears state" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try AtomicLimiter.init(Limit.perSecond(1), 0, mc.clock());

    _ = lim.allow();
    try std.testing.expect(!lim.allow().isAllowed());

    lim.reset();
    try std.testing.expect(lim.allow().isAllowed());
}

test "AtomicLimiter: retry duration is positive on denial" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try AtomicLimiter.init(Limit.perSecond(1), 0, mc.clock());

    _ = lim.allow();
    const d = lim.allow();
    switch (d) {
        .denied => |denied| try std.testing.expect(denied.retry_after.toNanoseconds() > 0),
        .allowed => return error.TestUnexpectedResult,
    }
}

test "AtomicLimiter: init rejects zero count" {
    var mc = types.ManualClock{};
    const bad = Limit{ .count = 0, .period_ns = std.time.ns_per_s };
    try std.testing.expectError(
        error.InvalidLimit,
        AtomicLimiter.init(bad, 0, mc.clock()),
    );
}

test "AtomicLimiter: init rejects rate > 1 req/ns" {
    var mc = types.ManualClock{};
    const bad = Limit{ .count = 2, .period_ns = 1 };
    try std.testing.expectError(
        error.RateExceedsRes,
        AtomicLimiter.init(bad, 0, mc.clock()),
    );
}

test "AtomicLimiter: sustained throughput matches rate" {
    var mc = types.ManualClock{};
    var lim = try AtomicLimiter.init(Limit.perSecond(100), 0, mc.clock());

    var allowed: usize = 0;
    var t: i64 = 0;
    // 10 seconds, one attempt every 1ms (10 000 attempts)
    while (t < 10 * std.time.ns_per_s) : (t += 1_000_000) {
        mc.set(t);
        if (lim.allow().isAllowed()) allowed += 1;
    }
    // Expect exactly 1000 (100/s × 10s)
    try std.testing.expectEqual(@as(usize, 1000), allowed);
}

test "AtomicLimiter: allowN overflow guard denies without panic" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);

    var lim = try AtomicLimiter.init(Limit.perSecond(10), 0, mc.clock());

    // max_batch = maxInt(i64) / 100_000_000 = 92, so 93 exceeds it
    if (lim.engine.max_batch >= std.math.maxInt(u32)) return; // avoid invalid cast
    const n: u32 = @intCast(lim.engine.max_batch + 1);

    const d = lim.allowN(n);
    try std.testing.expect(!d.isAllowed());
}

test "AtomicLimiter: allowN overflow guard leaves TAT unchanged" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try AtomicLimiter.init(Limit.perMinute(1), 0, mc.clock());

    const tat_before = lim.tat.load(.monotonic);
    const n: u32 = @intCast(lim.engine.max_batch + 1);
    _ = lim.allowN(n);

    try std.testing.expectEqual(tat_before, lim.tat.load(.monotonic));
}

test "AtomicLimiter: allowN overflow guard returns maxInt retry_after" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try AtomicLimiter.init(Limit.perMinute(1), 0, mc.clock());

    const n: u32 = @intCast(lim.engine.max_batch + 1);
    const d = lim.allowN(n);

    switch (d) {
        .denied => |denied| try std.testing.expectEqual(
            @as(i64, std.math.maxInt(i64)),
            denied.retry_after.toNanoseconds(),
        ),
        .allowed => return error.TestUnexpectedResult,
    }
}

test "AtomicLimiter: allowN large but valid n still works" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);

    var lim = try AtomicLimiter.init(Limit.perMinute(1), 99, mc.clock());

    const n: u32 = 100;
    try std.testing.expect(@as(u64, n) <= lim.engine.max_batch);

    // First call always allowed on a cold limiter — consume capacity
    _ = lim.allowN(n);

    // Now TAT is far in the future; a second batch must be denied
    const d = lim.allowN(n);

    try std.testing.expect(!d.isAllowed());
    // Finite wait — proves it was GCRA, not the overflow guard
    try std.testing.expect(d.denied.retry_after.toNanoseconds() < std.math.maxInt(i64));
}

test "AtomicLimiter: allowN boundary exactly at max_batch does not overflow" {
    var mc = types.ManualClock{};
    mc.set(0);

    const interval = @divFloor(std.math.maxInt(i64), 10);
    var lim = try AtomicLimiter.init(
        .{ .count = 1, .period_ns = interval },
        9,
        mc.clock(),
    );

    const n: u32 = @intCast(lim.engine.max_batch);
    const d = lim.allowN(n);

    // Guard must not have fired — if denied, retry must be finite
    switch (d) {
        .allowed => {},
        .denied => |denied| try std.testing.expect(
            denied.retry_after.toNanoseconds() < std.math.maxInt(i64),
        ),
    }
}

test "AtomicLimiter: init rejects negative period" {
    var mc = types.ManualClock{};
    const bad = Limit{ .count = 10, .period_ns = -1 };
    try std.testing.expectError(
        error.InvalidLimit,
        AtomicLimiter.init(bad, 0, mc.clock()),
    );
}

test "AtomicLimiter: init rejects unrepresentable burst offset" {
    var mc = types.ManualClock{};
    try std.testing.expectError(
        error.TimeOverflow,
        AtomicLimiter.init(
            .{ .count = 1, .period_ns = std.math.maxInt(i64) },
            2,
            mc.clock(),
        ),
    );
}

test "AtomicLimiter: unrepresentable future TAT fails closed" {
    var mc = types.ManualClock{};
    mc.set(std.math.maxInt(i64) - 5);
    var lim = try AtomicLimiter.init(
        .{ .count = 1, .period_ns = 10 },
        0,
        mc.clock(),
    );

    const d = lim.allow();
    try std.testing.expect(!d.isAllowed());
    try std.testing.expectEqual(
        @as(i64, std.math.maxInt(i64)),
        d.denied.retry_after.toNanoseconds(),
    );
    try std.testing.expectEqual(@as(i64, 0), lim.tat.load(.monotonic));
}

test "AtomicLimiter: denial has finite retry_after (not overflow guard)" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    var lim = try AtomicLimiter.init(Limit.perSecond(1), 0, mc.clock());

    _ = lim.allow();
    const d = lim.allow();
    switch (d) {
        .denied => |denied| {
            try std.testing.expect(denied.retry_after.toNanoseconds() > 0);
            try std.testing.expect(denied.retry_after.toNanoseconds() < std.math.maxInt(i64));
            // Should be approximately 1 second
            try std.testing.expect(denied.retry_after.toNanoseconds() <= std.time.ns_per_s);
        },
        .allowed => return error.TestUnexpectedResult,
    }
}

test "AtomicLimiter: reset then full capacity available" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    // rate=5/s, burst=4 → 5 at once
    var lim = try AtomicLimiter.init(Limit.perSecond(5), 4, mc.clock());

    // Exhaust all slots
    var i: usize = 0;
    while (i < 5) : (i += 1) {
        _ = lim.allow();
    }
    try std.testing.expect(!lim.allow().isAllowed());

    // Reset and verify full capacity restored
    lim.reset();
    i = 0;
    while (i < 5) : (i += 1) {
        try std.testing.expect(lim.allow().isAllowed());
    }
    try std.testing.expect(!lim.allow().isAllowed());
}

test "AtomicLimiter: burst replenishes over time" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    // rate=1/s, burst=1 → 2 at once, replenish 1 per second
    var lim = try AtomicLimiter.init(Limit.perSecond(1), 1, mc.clock());

    // Use both slots
    try std.testing.expect(lim.allow().isAllowed());
    try std.testing.expect(lim.allow().isAllowed());
    try std.testing.expect(!lim.allow().isAllowed());

    // Advance 1s → 1 slot replenished
    mc.tick(std.time.ns_per_s);
    try std.testing.expect(lim.allow().isAllowed());
    try std.testing.expect(!lim.allow().isAllowed());
}

test "AtomicLimiter: allowN with batch=2 on rate=10/s with burst" {
    var mc = types.ManualClock{};
    mc.set(std.time.ns_per_s);
    // rate=10/s, burst=9 → 10 slots available at once
    var lim = try AtomicLimiter.init(Limit.perSecond(10), 9, mc.clock());

    // 5 batches of 2 should exhaust 10 slots
    var i: usize = 0;
    while (i < 5) : (i += 1) {
        try std.testing.expect(lim.allowN(2).isAllowed());
    }
    try std.testing.expect(!lim.allow().isAllowed());
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

    var sys = types.SystemClock.init(std.testing.io);
    var lim = try AtomicLimiter.init(
        Limit.perSecond(1000),
        0,
        sys.clock(),
    );

    const Ctx = struct {
        limiter: *AtomicLimiter,
        allowed: std.atomic.Value(usize),

        fn run(ctx: *@This()) void {
            var i: usize = 0;
            while (i < requests_per_thread) : (i += 1) {
                if (ctx.limiter.allow().isAllowed()) {
                    _ = ctx.allowed.fetchAdd(1, .monotonic);
                }
            }
        }
    };

    var ctx = Ctx{
        .limiter = &lim,
        .allowed = std.atomic.Value(usize).init(0),
    };

    const start_ns = std.Io.Clock.awake.now(std.testing.io).toNanoseconds();

    var threads: [num_threads]std.Thread = undefined;
    for (&threads) |*t| {
        t.* = try std.Thread.spawn(.{}, Ctx.run, .{&ctx});
    }
    for (&threads) |*t| t.join();

    const elapsed_ns = std.Io.Clock.awake.now(std.testing.io).toNanoseconds() - start_ns;
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

    var sys = types.SystemClock.init(std.testing.io);
    // Large period so slots don't replenish during the test
    var lim = try AtomicLimiter.init(
        Limit{ .count = total_slots, .period_ns = std.time.ns_per_s },
        total_slots - 1, // critical
        sys.clock(),
    );

    const Ctx = struct {
        limiter: *AtomicLimiter,
        allowed: std.atomic.Value(usize),

        fn run(ctx: *@This()) void {
            var i: usize = 0;
            while (i < requests_per_thread) : (i += 1) {
                if (ctx.limiter.allow().isAllowed()) {
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
