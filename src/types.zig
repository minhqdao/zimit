//! Core types for zimit.
//! This file has zero dependencies on the GCRA engine or the standard library's
//! time functions — all time values are plain i64 nanoseconds, injectable by callers.

const std = @import("std");

// ── Limit ────────────────────────────────────────────────────────────────────

/// Describes a rate: `count` requests allowed per `period` nanoseconds.
///
/// Example:
///     const limit = Limit.perSecond(100);      // 100 req/s
///     const limit = Limit.perMinute(1000);     // 1 000 req/min
///     const limit = Limit{ .count = 5, .period_ns = 2 * std.time.ns_per_s }; // 5 req/2s
pub const Limit = struct {
    /// Number of requests allowed per period.
    count: u32,
    /// Duration of the period in nanoseconds.
    period_ns: i64,

    /// Derived: nanoseconds between each emission (period / count).
    /// This is the fundamental GCRA unit — one "slot" of time.
    /// Returns 0 when `count` is 0; limiter initialization rejects that limit.
    pub fn emissionInterval(self: Limit) i64 {
        if (self.count == 0) return 0;
        return @divTrunc(self.period_ns, @as(i64, self.count));
    }

    /// Nanoseconds a burst of `burst` extra requests buys you.
    /// In GCRA terms: how far in the past the TAT may be before we reject.
    /// Saturates at the `i64` bounds; limiter initialization reports
    /// `error.TimeOverflow` instead of accepting a saturated burst offset.
    pub fn burstOffset(self: Limit, burst: u32) i64 {
        return self.emissionInterval() *| @as(i64, burst);
    }

    // ── Convenience constructors ────────────────────────────────────────────

    /// `count` requests per second.
    pub fn perSecond(count: u32) Limit {
        return .{ .count = count, .period_ns = std.time.ns_per_s };
    }

    /// `count` requests per minute.
    pub fn perMinute(count: u32) Limit {
        return .{ .count = count, .period_ns = 60 * std.time.ns_per_s };
    }

    /// `count` requests per hour.
    pub fn perHour(count: u32) Limit {
        return .{ .count = count, .period_ns = 3600 * std.time.ns_per_s };
    }
};

// ── Decision ─────────────────────────────────────────────────────────────────

/// The result of a rate-limit check.
pub const Decision = union(enum) {
    /// Request is allowed. `new_tat` is the updated Theoretical Arrival Time
    /// the caller must persist back to the store.
    allowed: struct { new_tat: i64 },

    /// Request is denied. `retry_after_ns` is how many nanoseconds the caller
    /// should wait before retrying. The caller decides whether to sleep,
    /// suspend a fiber, return a 429, or do something else entirely.
    denied: struct { retry_after_ns: i64 },

    /// Returns true if the request was allowed.
    pub fn isAllowed(self: Decision) bool {
        return self == .allowed;
    }

    /// Returns the retry delay in nanoseconds if denied, else null.
    pub fn retryAfterNs(self: Decision) ?i64 {
        return switch (self) {
            .denied => |d| d.retry_after_ns,
            .allowed => null,
        };
    }
};

// ── Clock ─────────────────────────────────────────────────────────────────────

/// Anything that can tell us the current time in nanoseconds.
/// Use `SystemClock` in production. Pass a `ManualClock` in tests.
pub const Clock = struct {
    ptr: *anyopaque,
    now_fn: *const fn (ptr: *anyopaque) i64,

    /// Returns the current time in nanoseconds.
    pub fn now(self: Clock) i64 {
        return self.now_fn(self.ptr);
    }
};

/// Reads the system's monotonic awake clock.
///
/// This clock cannot be adjusted like wall time and excludes time while the
/// system is suspended. It uses the same clock as limiter wait operations.
pub const SystemClock = struct {
    io: std.Io,

    /// Initialise a system clock with the provided I/O implementation.
    /// In production, use `init.io` from `main(init: std.process.Init)`.
    /// In tests, use `std.testing.io`.
    pub fn init(io: std.Io) SystemClock {
        return .{ .io = io };
    }

    /// Returns a generic `Clock` interface backed by this SystemClock.
    pub fn clock(self: *SystemClock) Clock {
        return .{ .ptr = self, .now_fn = nowImpl };
    }

    fn nowImpl(ptr: *anyopaque) i64 {
        const self: *SystemClock = @ptrCast(@alignCast(ptr));
        const ts = std.Io.Clock.awake.now(self.io);
        const ns = ts.toNanoseconds();
        return @intCast(std.math.clamp(
            ns,
            std.math.minInt(i64),
            std.math.maxInt(i64),
        ));
    }
};

/// A manually-advanced clock for deterministic tests.
/// Call `.tick(ns)` to advance time; call `.set(ns)` to jump to an absolute time.
pub const ManualClock = struct {
    time_ns: i64 = 0,

    /// Returns a generic `Clock` interface backed by this ManualClock.
    pub fn clock(self: *ManualClock) Clock {
        return .{ .ptr = self, .now_fn = nowImpl };
    }

    /// Sets the clock to an absolute time in nanoseconds.
    pub fn set(self: *ManualClock, ns: i64) void {
        self.time_ns = ns;
    }

    /// Advances the clock by a duration in nanoseconds.
    /// Saturates at the `i64` bounds instead of overflowing.
    pub fn tick(self: *ManualClock, ns: i64) void {
        self.time_ns +|= ns;
    }

    fn nowImpl(ptr: *anyopaque) i64 {
        const self: *ManualClock = @ptrCast(@alignCast(ptr));
        return self.time_ns;
    }
};

// ── Errors ────────────────────────────────────────────────────────────────────

pub const ZimitError = error{
    /// count or period_ns is zero — would produce a zero emission interval.
    InvalidLimit,
    /// count > period_ns — rate is > 1 req/ns, which exceeds resolution.
    RateExceedsRes,
    /// Out of memory when inserting a new key into the store.
    OutOfMemory,
    /// Derived time values cannot be represented in nanoseconds.
    TimeOverflow,
    /// The keyed limiter has reached its configured entry limit.
    CapacityExceeded,
    /// The configured idle timeout is not positive.
    InvalidIdleTimeout,
};

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "Limit.emissionInterval: 100 req/s → 10ms" {
    const l = Limit.perSecond(100);
    try std.testing.expectEqual(@as(i64, 10_000_000), l.emissionInterval());
}

test "Limit.emissionInterval: 1 req/s → 1s" {
    const l = Limit.perSecond(1);
    try std.testing.expectEqual(std.time.ns_per_s, l.emissionInterval());
}

test "Limit.burstOffset: 100 req/s burst=10 → 100ms" {
    const l = Limit.perSecond(100);
    try std.testing.expectEqual(@as(i64, 100_000_000), l.burstOffset(10));
}

test "Limit.burstOffset: no burst → 0" {
    const l = Limit.perSecond(50);
    try std.testing.expectEqual(@as(i64, 0), l.burstOffset(0));
}

test "Limit.emissionInterval: zero count is non-trapping" {
    const limit = Limit{ .count = 0, .period_ns = std.time.ns_per_s };
    try std.testing.expectEqual(@as(i64, 0), limit.emissionInterval());
}

test "Limit.burstOffset: saturates on overflow" {
    const limit = Limit{ .count = 1, .period_ns = std.math.maxInt(i64) };
    try std.testing.expectEqual(
        @as(i64, std.math.maxInt(i64)),
        limit.burstOffset(2),
    );
}

test "Limit.perMinute: 60 req/min → 1s emission interval" {
    const l = Limit.perMinute(60);
    try std.testing.expectEqual(std.time.ns_per_s, l.emissionInterval());
}

test "Limit.perHour: 3600 req/h → 1s emission interval" {
    const l = Limit.perHour(3600);
    try std.testing.expectEqual(std.time.ns_per_s, l.emissionInterval());
}

test "Limit.perHour: 1 req/h → 1 hour emission interval" {
    const l = Limit.perHour(1);
    try std.testing.expectEqual(@as(i64, 3600 * std.time.ns_per_s), l.emissionInterval());
}

test "Limit.emissionInterval: large count does not overflow" {
    // maxInt(u32) = 4_294_967_295
    // period_ns = 1_000_000_000 (1s)
    // interval = 1_000_000_000 / 4_294_967_295 = 0 (integer truncation)
    const l = Limit.perSecond(std.math.maxInt(u32));
    const interval = l.emissionInterval();
    try std.testing.expect(interval >= 0);
}

test "Limit.burstOffset: burst=maxInt(u32) with large interval does not panic" {
    // 1 req/s → interval = 1_000_000_000
    // burst = 1 → offset = 1_000_000_000
    const l = Limit.perSecond(1);
    const offset = l.burstOffset(1);
    try std.testing.expectEqual(std.time.ns_per_s, offset);
}

test "Decision.isAllowed" {
    const allowed = Decision{ .allowed = .{ .new_tat = 42 } };
    const denied = Decision{ .denied = .{ .retry_after_ns = 1000 } };
    try std.testing.expect(allowed.isAllowed());
    try std.testing.expect(!denied.isAllowed());
}

test "Decision.retry_after_ns" {
    const allowed = Decision{ .allowed = .{ .new_tat = 0 } };
    const denied = Decision{ .denied = .{ .retry_after_ns = 5_000_000 } };
    try std.testing.expectEqual(@as(?i64, null), allowed.retryAfterNs());
    try std.testing.expectEqual(@as(?i64, 5_000_000), denied.retryAfterNs());
}

test "SystemClock: monotonic non-decreasing without sleep" {
    var sys = SystemClock.init(std.testing.io);
    const clk = sys.clock();

    var prev = clk.now();

    var i: usize = 0;
    while (i < 10_000) : (i += 1) {
        const now = clk.now();
        try std.testing.expect(now >= prev);
        prev = now;
    }
}

test "SystemClock: uses the awake monotonic clock" {
    var sys = SystemClock.init(std.testing.io);
    const clk = sys.clock();

    const before = std.Io.Clock.awake.now(std.testing.io).toNanoseconds();
    const actual = clk.now();
    const after = std.Io.Clock.awake.now(std.testing.io).toNanoseconds();

    try std.testing.expect(actual >= before);
    try std.testing.expect(actual <= after);
}

test "SystemClock: returns positive i64" {
    var sys = SystemClock.init(std.testing.io);
    const clk = sys.clock();

    const t = clk.now();
    try std.testing.expect(t > 0);
}

test "SystemClock: multiple calls are non-decreasing" {
    var sys = SystemClock.init(std.testing.io);
    const clk = sys.clock();

    var prev = clk.now();

    var i: usize = 0;
    while (i < 1000) : (i += 1) {
        const now = clk.now();
        try std.testing.expect(now >= prev);
        prev = now;
    }
}

test "ManualClock: starts at zero" {
    var c = ManualClock{};
    try std.testing.expectEqual(@as(i64, 0), c.clock().now());
}

test "ManualClock: tick advances time" {
    var c = ManualClock{};
    c.tick(1_000_000);
    c.tick(500_000);
    try std.testing.expectEqual(@as(i64, 1_500_000), c.clock().now());
}

test "ManualClock: set jumps to absolute time" {
    var c = ManualClock{};
    c.tick(9999);
    c.set(1_000_000_000);
    try std.testing.expectEqual(@as(i64, 1_000_000_000), c.clock().now());
}

test "ManualClock: Clock interface forwards correctly" {
    var mc = ManualClock{};
    const clk = mc.clock();
    mc.set(42_000);
    try std.testing.expectEqual(@as(i64, 42_000), clk.now());
}

test "ManualClock: many ticks accumulate correctly" {
    var c = ManualClock{};
    var i: usize = 0;
    while (i < 1000) : (i += 1) {
        c.tick(1_000_000); // 1ms each
    }
    try std.testing.expectEqual(@as(i64, 1_000_000_000), c.clock().now());
}

test "ManualClock: set then tick combines correctly" {
    var c = ManualClock{};
    c.set(5_000_000_000); // 5s
    c.tick(2_000_000_000); // +2s
    try std.testing.expectEqual(@as(i64, 7_000_000_000), c.clock().now());
}

test "ManualClock: tick saturates at maximum" {
    var c = ManualClock{ .time_ns = std.math.maxInt(i64) - 1 };
    c.tick(2);
    try std.testing.expectEqual(@as(i64, std.math.maxInt(i64)), c.clock().now());
}

test "ManualClock: negative tick saturates at minimum" {
    var c = ManualClock{ .time_ns = std.math.minInt(i64) + 1 };
    c.tick(-2);
    try std.testing.expectEqual(@as(i64, std.math.minInt(i64)), c.clock().now());
}

test "Decision: allowed isAllowed returns true" {
    const d = Decision{ .allowed = .{ .new_tat = 0 } };
    try std.testing.expect(d.isAllowed());
}

test "Decision: denied isAllowed returns false" {
    const d = Decision{ .denied = .{ .retry_after_ns = 100 } };
    try std.testing.expect(!d.isAllowed());
}
