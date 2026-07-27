//! Core types for zimit.
//! This file has zero dependencies on the GCRA engine. Public time values use
//! Zig's `std.Io.Duration` and `std.Io.Timestamp`.

const std = @import("std");

// ── Limit ────────────────────────────────────────────────────────────────────

/// Describes a rate: `count` requests allowed per `period`.
///
/// Example:
///     const limit = Limit.perSecond(100);      // 100 req/s
///     const limit = Limit.perMinute(1000);     // 1 000 req/min
///     const limit = Limit{ .count = 5, .period = .fromSeconds(2) }; // 5 req/2s
pub const Limit = struct {
    /// Number of requests allowed per period.
    count: u32,
    /// Duration of the period.
    period: std.Io.Duration,

    /// Derived duration between each emission (period / count).
    /// This is the fundamental GCRA unit — one "slot" of time.
    /// Returns `.zero` when `count` is 0; initialization rejects that limit.
    pub fn emissionInterval(self: Limit) std.Io.Duration {
        if (self.count == 0) return .zero;
        return .fromNanoseconds(
            @divTrunc(self.period.toNanoseconds(), @as(i96, self.count)),
        );
    }

    /// Duration a burst of `burst` extra requests buys you.
    /// In GCRA terms: how far in the past the TAT may be before we reject.
    /// Saturates at the `std.Io.Duration` bounds; limiter initialization
    /// reports `error.TimeOverflow` if its internal representation is narrower.
    pub fn burstOffset(self: Limit, burst: u32) std.Io.Duration {
        return .fromNanoseconds(
            self.emissionInterval().toNanoseconds() *| @as(i96, burst),
        );
    }

    // ── Convenience constructors ────────────────────────────────────────────

    /// `count` requests per second.
    pub fn perSecond(count: u32) Limit {
        return .{ .count = count, .period = .fromSeconds(1) };
    }

    /// `count` requests per minute.
    pub fn perMinute(count: u32) Limit {
        return .{ .count = count, .period = .fromSeconds(60) };
    }

    /// `count` requests per hour.
    pub fn perHour(count: u32) Limit {
        return .{ .count = count, .period = .fromSeconds(3600) };
    }
};

// ── Decision ─────────────────────────────────────────────────────────────────

/// The result of a rate-limit check.
pub const Decision = union(enum) {
    /// Request is allowed.
    allowed,

    /// Request is denied. The caller decides whether to sleep, suspend a fiber,
    /// return a 429, or do something else with `retry_after`.
    denied: struct { retry_after: std.Io.Duration },

    /// Returns true if the request was allowed.
    pub fn isAllowed(self: Decision) bool {
        return self == .allowed;
    }

    /// Returns the retry delay if denied, otherwise `null`.
    pub fn retryAfter(self: Decision) ?std.Io.Duration {
        return switch (self) {
            .denied => |d| d.retry_after,
            .allowed => null,
        };
    }

    /// Returns the retry delay in whole milliseconds, rounded up, if denied.
    pub fn retryAfterMillisecondsCeil(self: Decision) ?i64 {
        const duration = self.retryAfter() orelse return null;
        const nanoseconds = duration.toNanoseconds();
        const milliseconds = @divFloor(nanoseconds, std.time.ns_per_ms);
        return @intCast(milliseconds +
            @intFromBool(@mod(nanoseconds, std.time.ns_per_ms) != 0));
    }
};

// ── Clock ─────────────────────────────────────────────────────────────────────

/// A time source used by the limiter.
///
/// The system variant owns a copy of `std.Io`, so it remains valid when the
/// limiter is moved. Custom clocks borrow their backing object, which must
/// outlive the clock. When used by a shared `GlobalLimiter`, a custom clock's
/// `now_fn` must support concurrent calls.
pub const Clock = union(enum) {
    system: std.Io,
    custom: struct {
        ptr: *anyopaque,
        now_fn: *const fn (ptr: *anyopaque) i64,
    },

    /// Returns the current time in nanoseconds.
    pub fn now(self: Clock) i64 {
        return switch (self) {
            .system => |io| systemNow(io),
            .custom => |custom| custom.now_fn(custom.ptr),
        };
    }

    fn systemNow(io: std.Io) i64 {
        const ts = std.Io.Clock.awake.now(io);
        const ns = ts.toNanoseconds();
        return @intCast(std.math.clamp(
            ns,
            std.math.minInt(i64),
            std.math.maxInt(i64),
        ));
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

    /// Returns a movable clock that owns a copy of the `std.Io` interface.
    pub fn clock(self: SystemClock) Clock {
        return .{ .system = self.io };
    }
};

/// A manually-advanced clock for deterministic tests.
/// Call `.tick(duration)` to advance time; call `.set(timestamp)` to jump to
/// an absolute time.
///
/// This type is not thread-safe. Do not inject it into a `GlobalLimiter` that
/// is accessed concurrently.
pub const ManualClock = struct {
    time_ns: i64 = 0,

    /// Returns a generic `Clock` interface backed by this ManualClock.
    pub fn clock(self: *ManualClock) Clock {
        return .{ .custom = .{ .ptr = self, .now_fn = nowImpl } };
    }

    /// Sets the clock to an absolute timestamp.
    pub fn set(self: *ManualClock, timestamp: std.Io.Timestamp) void {
        self.time_ns = clampI64(timestamp.toNanoseconds());
    }

    /// Advances the clock by a duration.
    /// Saturates at the `i64` bounds instead of overflowing.
    pub fn tick(self: *ManualClock, duration: std.Io.Duration) void {
        self.time_ns = clampI64(
            @as(i96, self.time_ns) + duration.toNanoseconds(),
        );
    }

    fn nowImpl(ptr: *anyopaque) i64 {
        const self: *ManualClock = @ptrCast(@alignCast(ptr));
        return self.time_ns;
    }

    fn clampI64(value: i96) i64 {
        return @intCast(std.math.clamp(
            value,
            std.math.minInt(i64),
            std.math.maxInt(i64),
        ));
    }
};

// ── Errors ────────────────────────────────────────────────────────────────────

pub const ZimitError = error{
    /// count or period is zero — would produce a zero emission interval.
    InvalidLimit,
    /// count exceeds the period's nanoseconds — rate is > 1 req/ns.
    RateExceedsRes,
    /// Out of memory when inserting a new key into the store.
    OutOfMemory,
    /// Derived time values cannot be represented in nanoseconds.
    TimeOverflow,
    /// The keyed limiter has reached its configured entry limit.
    CapacityExceeded,
    /// The configured idle timeout is not positive.
    InvalidIdleTimeout,
    /// A batch exceeds configured capacity or the representable time range.
    BatchTooLarge,
};

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "Limit.emissionInterval: 100 req/s → 10ms" {
    const l = Limit.perSecond(100);
    try std.testing.expectEqual(@as(i96, 10_000_000), l.emissionInterval().toNanoseconds());
}

test "Limit.emissionInterval: 1 req/s → 1s" {
    const l = Limit.perSecond(1);
    try std.testing.expectEqual(@as(i96, std.time.ns_per_s), l.emissionInterval().toNanoseconds());
}

test "Limit.burstOffset: 100 req/s burst=10 → 100ms" {
    const l = Limit.perSecond(100);
    try std.testing.expectEqual(@as(i96, 100_000_000), l.burstOffset(10).toNanoseconds());
}

test "Limit.burstOffset: no burst → 0" {
    const l = Limit.perSecond(50);
    try std.testing.expectEqual(@as(i96, 0), l.burstOffset(0).toNanoseconds());
}

test "Limit.emissionInterval: zero count is non-trapping" {
    const limit = Limit{ .count = 0, .period = .fromNanoseconds(std.time.ns_per_s) };
    try std.testing.expectEqual(@as(i96, 0), limit.emissionInterval().toNanoseconds());
}

test "Limit.burstOffset: uses Duration's wider representation" {
    const limit = Limit{ .count = 1, .period = .fromNanoseconds(std.math.maxInt(i64)) };
    try std.testing.expectEqual(
        @as(i96, std.math.maxInt(i64)) * 2,
        limit.burstOffset(2).toNanoseconds(),
    );
}

test "Limit.perMinute: 60 req/min → 1s emission interval" {
    const l = Limit.perMinute(60);
    try std.testing.expectEqual(@as(i96, std.time.ns_per_s), l.emissionInterval().toNanoseconds());
}

test "Limit.perHour: 3600 req/h → 1s emission interval" {
    const l = Limit.perHour(3600);
    try std.testing.expectEqual(@as(i96, std.time.ns_per_s), l.emissionInterval().toNanoseconds());
}

test "Limit.perHour: 1 req/h → 1 hour emission interval" {
    const l = Limit.perHour(1);
    try std.testing.expectEqual(
        @as(i96, 3600 * std.time.ns_per_s),
        l.emissionInterval().toNanoseconds(),
    );
}

test "Limit.emissionInterval: large count does not overflow" {
    // maxInt(u32) = 4_294_967_295
    // period = 1_000_000_000ns (1s)
    // interval = 1_000_000_000 / 4_294_967_295 = 0 (integer truncation)
    const l = Limit.perSecond(std.math.maxInt(u32));
    const interval = l.emissionInterval();
    try std.testing.expect(interval.toNanoseconds() >= 0);
}

test "Limit.burstOffset: burst=maxInt(u32) with large interval does not panic" {
    // 1 req/s → interval = 1_000_000_000
    // burst = 1 → offset = 1_000_000_000
    const l = Limit.perSecond(1);
    const offset = l.burstOffset(1);
    try std.testing.expectEqual(@as(i96, std.time.ns_per_s), offset.toNanoseconds());
}

test "Decision.isAllowed" {
    const allowed: Decision = .allowed;
    const denied = Decision{ .denied = .{
        .retry_after = .fromNanoseconds(1000),
    } };
    try std.testing.expect(allowed.isAllowed());
    try std.testing.expect(!denied.isAllowed());
}

test "Decision.retryAfter returns an Io Duration" {
    const allowed: Decision = .allowed;
    const denied = Decision{ .denied = .{
        .retry_after = .fromNanoseconds(5_000_000),
    } };

    try std.testing.expectEqual(@as(?std.Io.Duration, null), allowed.retryAfter());
    try std.testing.expectEqual(
        @as(i96, 5_000_000),
        denied.retryAfter().?.toNanoseconds(),
    );
}

test "Decision.retryAfterMillisecondsCeil rounds up safely" {
    const cases = [_]struct {
        nanoseconds: i64,
        expected_milliseconds: i64,
    }{
        .{ .nanoseconds = 1, .expected_milliseconds = 1 },
        .{ .nanoseconds = 5_000_000, .expected_milliseconds = 5 },
        .{ .nanoseconds = 5_000_001, .expected_milliseconds = 6 },
        .{
            .nanoseconds = std.math.maxInt(i64),
            .expected_milliseconds = @divFloor(
                std.math.maxInt(i64),
                std.time.ns_per_ms,
            ) + 1,
        },
    };

    for (cases) |case| {
        const decision = Decision{ .denied = .{
            .retry_after = .fromNanoseconds(case.nanoseconds),
        } };
        try std.testing.expectEqual(
            @as(?i64, case.expected_milliseconds),
            decision.retryAfterMillisecondsCeil(),
        );
    }

    const allowed: Decision = .allowed;
    try std.testing.expectEqual(
        @as(?i64, null),
        allowed.retryAfterMillisecondsCeil(),
    );
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

fn makeSystemClock(io: std.Io) Clock {
    return SystemClock.init(io).clock();
}

test "SystemClock: returned Clock owns movable Io state" {
    const clock = makeSystemClock(std.testing.io);
    const first = clock.now();
    const second = clock.now();

    try std.testing.expect(second >= first);
}

test "ManualClock: starts at zero" {
    var c = ManualClock{};
    try std.testing.expectEqual(@as(i64, 0), c.clock().now());
}

test "ManualClock: tick advances time" {
    var c = ManualClock{};
    c.tick(.fromNanoseconds(1_000_000));
    c.tick(.fromNanoseconds(500_000));
    try std.testing.expectEqual(@as(i64, 1_500_000), c.clock().now());
}

test "ManualClock: set jumps to absolute time" {
    var c = ManualClock{};
    c.tick(.fromNanoseconds(9999));
    c.set(.fromNanoseconds(1_000_000_000));
    try std.testing.expectEqual(@as(i64, 1_000_000_000), c.clock().now());
}

test "ManualClock: Clock interface forwards correctly" {
    var mc = ManualClock{};
    const clk = mc.clock();
    mc.set(.fromNanoseconds(42_000));
    try std.testing.expectEqual(@as(i64, 42_000), clk.now());
}

test "ManualClock: many ticks accumulate correctly" {
    var c = ManualClock{};
    var i: usize = 0;
    while (i < 1000) : (i += 1) {
        c.tick(.fromNanoseconds(1_000_000)); // 1ms each
    }
    try std.testing.expectEqual(@as(i64, 1_000_000_000), c.clock().now());
}

test "ManualClock: set then tick combines correctly" {
    var c = ManualClock{};
    c.set(.fromNanoseconds(5_000_000_000)); // 5s
    c.tick(.fromNanoseconds(2_000_000_000)); // +2s
    try std.testing.expectEqual(@as(i64, 7_000_000_000), c.clock().now());
}

test "ManualClock: tick saturates at maximum" {
    var c = ManualClock{ .time_ns = std.math.maxInt(i64) - 1 };
    c.tick(.fromNanoseconds(2));
    try std.testing.expectEqual(@as(i64, std.math.maxInt(i64)), c.clock().now());
}

test "ManualClock: negative tick saturates at minimum" {
    var c = ManualClock{ .time_ns = std.math.minInt(i64) + 1 };
    c.tick(.fromNanoseconds(-2));
    try std.testing.expectEqual(@as(i64, std.math.minInt(i64)), c.clock().now());
}

test "ManualClock: wider Duration and Timestamp values clamp safely" {
    var c = ManualClock{};
    c.set(.fromNanoseconds(std.math.maxInt(i96)));
    try std.testing.expectEqual(@as(i64, std.math.maxInt(i64)), c.clock().now());

    c.set(.fromNanoseconds(std.math.minInt(i96)));
    try std.testing.expectEqual(@as(i64, std.math.minInt(i64)), c.clock().now());

    c.set(.zero);
    c.tick(.max);
    try std.testing.expectEqual(@as(i64, std.math.maxInt(i64)), c.clock().now());
}
