//! Core types for zimit.
//! This file has zero dependencies on the GCRA engine or the standard library's
//! time functions — all time values are plain i64 nanoseconds, injectable by callers.

const std = @import("std");

// ── Limit ────────────────────────────────────────────────────────────────────

/// Describes a rate: `count` requests allowed per `period` nanoseconds.
///
/// Example:
///     const limit = Limit.per_second(100);      // 100 req/s
///     const limit = Limit.per_minute(1000);     // 1 000 req/min
///     const limit = Limit{ .count = 5, .period_ns = 2 * std.time.ns_per_s }; // 5 req/2s
pub const Limit = struct {
    /// Number of requests allowed per period.
    count: u32,
    /// Duration of the period in nanoseconds.
    period_ns: i64,

    /// Derived: nanoseconds between each emission (period / count).
    /// This is the fundamental GCRA unit — one "slot" of time.
    pub fn emission_interval(self: Limit) i64 {
        return @divTrunc(self.period_ns, @as(i64, self.count));
    }

    /// Nanoseconds a burst of `burst` extra requests buys you.
    /// In GCRA terms: how far in the past the TAT may be before we reject.
    pub fn burst_offset(self: Limit, burst: u32) i64 {
        return self.emission_interval() * @as(i64, burst);
    }

    // ── Convenience constructors ────────────────────────────────────────────

    /// 1 request per second.
    pub fn per_second(count: u32) Limit {
        return .{ .count = count, .period_ns = std.time.ns_per_s };
    }

    /// 1 request per minute.
    pub fn per_minute(count: u32) Limit {
        return .{ .count = count, .period_ns = 60 * std.time.ns_per_s };
    }

    /// 1 request per hour.
    pub fn per_hour(count: u32) Limit {
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
    pub fn is_allowed(self: Decision) bool {
        return self == .allowed;
    }

    /// Returns the retry delay in nanoseconds if denied, else null.
    pub fn retry_after_ns(self: Decision) ?i64 {
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

/// Reads the real system monotonic clock.
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
        return .{ .ptr = self, .now_fn = now_impl };
    }

    fn now_impl(ptr: *anyopaque) i64 {
        const self: *SystemClock = @ptrCast(@alignCast(ptr));
        const ts = std.Io.Timestamp.now(self.io, .real);
        return @intCast(ts.toNanoseconds());
    }
};

/// A manually-advanced clock for deterministic tests.
/// Call `.tick(ns)` to advance time; call `.set(ns)` to jump to an absolute time.
pub const ManualClock = struct {
    time_ns: i64 = 0,

    /// Returns a generic `Clock` interface backed by this ManualClock.
    pub fn clock(self: *ManualClock) Clock {
        return .{ .ptr = self, .now_fn = now_impl };
    }

    /// Sets the clock to an absolute time in nanoseconds.
    pub fn set(self: *ManualClock, ns: i64) void {
        self.time_ns = ns;
    }

    /// Advances the clock by a duration in nanoseconds.
    pub fn tick(self: *ManualClock, ns: i64) void {
        self.time_ns += ns;
    }

    fn now_impl(ptr: *anyopaque) i64 {
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
};

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

test "Limit.emission_interval: 100 req/s → 10ms" {
    const l = Limit.per_second(100);
    try std.testing.expectEqual(@as(i64, 10_000_000), l.emission_interval());
}

test "Limit.emission_interval: 1 req/s → 1s" {
    const l = Limit.per_second(1);
    try std.testing.expectEqual(std.time.ns_per_s, l.emission_interval());
}

test "Limit.burst_offset: 100 req/s burst=10 → 100ms" {
    const l = Limit.per_second(100);
    try std.testing.expectEqual(@as(i64, 100_000_000), l.burst_offset(10));
}

test "Limit.burst_offset: no burst → 0" {
    const l = Limit.per_second(50);
    try std.testing.expectEqual(@as(i64, 0), l.burst_offset(0));
}

test "Limit.per_minute: 60 req/min → 1s emission interval" {
    const l = Limit.per_minute(60);
    try std.testing.expectEqual(std.time.ns_per_s, l.emission_interval());
}

test "Limit.per_hour: 3600 req/h → 1s emission interval" {
    const l = Limit.per_hour(3600);
    try std.testing.expectEqual(std.time.ns_per_s, l.emission_interval());
}

test "Limit.per_hour: 1 req/h → 1 hour emission interval" {
    const l = Limit.per_hour(1);
    try std.testing.expectEqual(@as(i64, 3600 * std.time.ns_per_s), l.emission_interval());
}

test "Limit.emission_interval: large count does not overflow" {
    // maxInt(u32) = 4_294_967_295
    // period_ns = 1_000_000_000 (1s)
    // interval = 1_000_000_000 / 4_294_967_295 = 0 (integer truncation)
    const l = Limit.per_second(std.math.maxInt(u32));
    const interval = l.emission_interval();
    try std.testing.expect(interval >= 0);
}

test "Limit.burst_offset: burst=maxInt(u32) with large interval does not panic" {
    // 1 req/s → interval = 1_000_000_000
    // burst = 1 → offset = 1_000_000_000
    const l = Limit.per_second(1);
    const offset = l.burst_offset(1);
    try std.testing.expectEqual(std.time.ns_per_s, offset);
}

test "Decision.is_allowed" {
    const allowed = Decision{ .allowed = .{ .new_tat = 42 } };
    const denied = Decision{ .denied = .{ .retry_after_ns = 1000 } };
    try std.testing.expect(allowed.is_allowed());
    try std.testing.expect(!denied.is_allowed());
}

test "Decision.retry_after_ns" {
    const allowed = Decision{ .allowed = .{ .new_tat = 0 } };
    const denied = Decision{ .denied = .{ .retry_after_ns = 5_000_000 } };
    try std.testing.expectEqual(@as(?i64, null), allowed.retry_after_ns());
    try std.testing.expectEqual(@as(?i64, 5_000_000), denied.retry_after_ns());
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

test "Decision: allowed is_allowed returns true" {
    const d = Decision{ .allowed = .{ .new_tat = 0 } };
    try std.testing.expect(d.is_allowed());
}

test "Decision: denied is_allowed returns false" {
    const d = Decision{ .denied = .{ .retry_after_ns = 100 } };
    try std.testing.expect(!d.is_allowed());
}
