const std = @import("std");
const zimit = @import("zimit");
const options = @import("bench_options");

const BenchClock = struct {
    now_ns: std.atomic.Value(i64) = .init(0),

    fn clock(self: *BenchClock) zimit.Clock {
        return .{ .custom = .{
            .ptr = self,
            .now_fn = now,
        } };
    }

    fn now(ptr: *anyopaque) i64 {
        const self: *BenchClock = @ptrCast(@alignCast(ptr));
        return self.now_ns.load(.monotonic);
    }
};

const Result = struct {
    name: []const u8,
    elapsed_ns: i96,
    operations: usize,
    outcomes: usize,

    fn print(self: Result) void {
        const ns_per_op = @as(f64, @floatFromInt(self.elapsed_ns)) /
            @as(f64, @floatFromInt(self.operations));
        std.debug.print(
            "{s:<30} {d:>10.2} ns/op  ({d} ops, {d} outcomes, {d:.3} ms)\n",
            .{
                self.name,
                ns_per_op,
                self.operations,
                self.outcomes,
                @as(f64, @floatFromInt(self.elapsed_ns)) /
                    std.time.ns_per_ms,
            },
        );
    }
};

fn timestamp(io: std.Io) i96 {
    return std.Io.Clock.awake.now(io).toNanoseconds();
}

fn benchmarkGlobalAdmission(io: std.Io) !Result {
    var clock = BenchClock{};
    var limiter = try zimit.GlobalLimiter.initWithClock(.{
        .limit = .perSecond(1_000_000_000),
        .burst = std.math.maxInt(u32),
    }, clock.clock());

    var allowed: usize = 0;
    const start = timestamp(io);
    for (0..options.iterations) |_| {
        allowed += @intFromBool((try limiter.allow()).isAllowed());
    }
    const end = timestamp(io);
    return .{
        .name = "global/admission",
        .elapsed_ns = end - start,
        .operations = options.iterations,
        .outcomes = allowed,
    };
}

const ContentionContext = struct {
    limiter: *zimit.GlobalLimiter,
    operations: usize,
    allowed: std.atomic.Value(usize) = .init(0),

    fn run(self: *ContentionContext) void {
        var allowed: usize = 0;
        for (0..self.operations) |_| {
            allowed += @intFromBool(
                (self.limiter.allow() catch unreachable).isAllowed(),
            );
        }
        _ = self.allowed.fetchAdd(allowed, .monotonic);
    }
};

fn benchmarkGlobalContention(io: std.Io) !Result {
    var clock = BenchClock{};
    var limiter = try zimit.GlobalLimiter.initWithClock(.{
        .limit = .perSecond(1_000_000_000),
        .burst = std.math.maxInt(u32),
    }, clock.clock());
    const operations_per_thread =
        (options.iterations + options.threads - 1) / options.threads;
    var context = ContentionContext{
        .limiter = &limiter,
        .operations = operations_per_thread,
    };
    var threads: [options.threads]std.Thread = undefined;

    const start = timestamp(io);
    var spawned: usize = 0;
    errdefer for (threads[0..spawned]) |*thread| thread.join();
    for (&threads) |*thread| {
        thread.* = try std.Thread.spawn(.{}, ContentionContext.run, .{&context});
        spawned += 1;
    }
    for (&threads) |*thread| thread.join();
    const end = timestamp(io);

    return .{
        .name = "global/contention",
        .elapsed_ns = end - start,
        .operations = operations_per_thread * options.threads,
        .outcomes = context.allowed.load(.monotonic),
    };
}

fn benchmarkKeyedHits(allocator: std.mem.Allocator, io: std.Io) !Result {
    var clock = zimit.ManualClock{};
    var limiter = try zimit.RateLimiter(u64).initWithClock(.{
        .allocator = allocator,
        .limit = .perSecond(1_000_000_000),
        .burst = std.math.maxInt(u32),
    }, clock.clock());
    defer limiter.deinit();
    _ = try limiter.allow(42);

    var allowed: usize = 0;
    const start = timestamp(io);
    for (0..options.iterations) |_| {
        allowed += @intFromBool((try limiter.allow(42)).isAllowed());
    }
    const end = timestamp(io);
    return .{
        .name = "keyed/hit",
        .elapsed_ns = end - start,
        .operations = options.iterations,
        .outcomes = allowed,
    };
}

const BorrowedStringLimiter = zimit.RateLimiterWithContext(
    []const u8,
    std.hash_map.StringContext,
);

fn benchmarkBorrowedInsertions(
    allocator: std.mem.Allocator,
    io: std.Io,
    keys: []u64,
) !Result {
    var clock = zimit.ManualClock{};
    var limiter = try BorrowedStringLimiter.initWithClock(.{
        .allocator = allocator,
        .limit = .perSecond(1_000_000_000),
        .burst = 0,
        .context = .{},
        .ownership = .borrowed,
    }, clock.clock());
    defer limiter.deinit();

    var allowed: usize = 0;
    const start = timestamp(io);
    for (keys) |*key| {
        allowed += @intFromBool((try limiter.allow(std.mem.asBytes(key))).isAllowed());
    }
    const end = timestamp(io);
    return .{
        .name = "keyed/new borrowed",
        .elapsed_ns = end - start,
        .operations = keys.len,
        .outcomes = allowed,
    };
}

fn benchmarkCopiedInsertions(
    allocator: std.mem.Allocator,
    io: std.Io,
    keys: []u64,
) !Result {
    var clock = zimit.ManualClock{};
    var limiter = try zimit.RateLimiter([]const u8).initWithClock(.{
        .allocator = allocator,
        .limit = .perSecond(1_000_000_000),
        .burst = 0,
    }, clock.clock());
    defer limiter.deinit();

    var allowed: usize = 0;
    const start = timestamp(io);
    for (keys) |*key| {
        allowed += @intFromBool((try limiter.allow(std.mem.asBytes(key))).isAllowed());
    }
    const end = timestamp(io);
    return .{
        .name = "keyed/new copied",
        .elapsed_ns = end - start,
        .operations = keys.len,
        .outcomes = allowed,
    };
}

fn benchmarkCapacityPressure(allocator: std.mem.Allocator, io: std.Io) !Result {
    var clock = zimit.ManualClock{};
    var limiter = try zimit.RateLimiter(u64).initWithClock(.{
        .allocator = allocator,
        .limit = .perSecond(1_000_000_000),
        .initial_capacity = @intCast(@min(options.keys, std.math.maxInt(u32))),
        .max_entries = options.keys,
    }, clock.clock());
    defer limiter.deinit();
    for (0..options.keys) |key| _ = try limiter.allow(@intCast(key));

    var rejected: usize = 0;
    const start = timestamp(io);
    for (0..options.iterations) |i| {
        _ = limiter.allow(@intCast(options.keys + i)) catch |err| {
            if (err != error.CapacityExceeded) return err;
            rejected += 1;
            continue;
        };
    }
    const end = timestamp(io);
    return .{
        .name = "keyed/capacity full",
        .elapsed_ns = end - start,
        .operations = options.iterations,
        .outcomes = rejected,
    };
}

fn benchmarkPruning(allocator: std.mem.Allocator, io: std.Io) !Result {
    var clock = zimit.ManualClock{};
    var limiter = try zimit.RateLimiter(u64).initWithClock(.{
        .allocator = allocator,
        .limit = .perSecond(1_000_000_000),
        .initial_capacity = @intCast(@min(options.keys, std.math.maxInt(u32))),
        .max_entries = options.keys,
        .idle_timeout = .fromNanoseconds(1),
    }, clock.clock());
    defer limiter.deinit();
    for (0..options.keys) |key| _ = try limiter.allow(@intCast(key));
    clock.tick(.fromNanoseconds(1));

    const start = timestamp(io);
    const removed = try limiter.pruneExpired();
    const end = timestamp(io);
    return .{
        .name = "keyed/prune expired",
        .elapsed_ns = end - start,
        .operations = options.keys,
        .outcomes = removed,
    };
}

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const io = init.io;
    const keys = try allocator.alloc(u64, options.keys);
    defer allocator.free(keys);
    for (keys, 0..) |*key, i| key.* = @intCast(i);

    std.debug.print(
        "zimit benchmarks: {d} iterations, {d} keys, {d} threads\n",
        .{ options.iterations, options.keys, options.threads },
    );
    (try benchmarkGlobalAdmission(io)).print();
    (try benchmarkGlobalContention(io)).print();
    (try benchmarkKeyedHits(allocator, io)).print();
    (try benchmarkBorrowedInsertions(allocator, io, keys)).print();
    (try benchmarkCopiedInsertions(allocator, io, keys)).print();
    (try benchmarkCapacityPressure(allocator, io)).print();
    (try benchmarkPruning(allocator, io)).print();
}
