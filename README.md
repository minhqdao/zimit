# zimit

A zero-dependency GCRA-based rate limiter with a token-bucket-like API for Zig 0.16.0+.

## Features

- **Global limiting:** Use `GlobalLimiter` when you want a single shared limit across all requests (e.g. protect total server throughput). It's lock-free and thread-safe.
- **Per-key rate limiting:** Each key is tracked independently (e.g. per user ID or IP address). The `RateLimiter` is **not** thread-safe. If you share it across multiple threads, you should protect it with a `std.Io.Mutex`.
- **Key storage controls:** Use `initial_capacity` to reserve space up front, and configure `max_entries` with `idle_timeout_ns` to bound memory and reclaim inactive, fully-drained keys. Explicit `pruneExpired()` calls return an allocation error if pruning scratch space cannot be reserved.
- **Blocking vs non-blocking:**
  - `allow()` → Immediate `Decision`; denied results carry a `std.Io.Duration`
  - `allowN(n)` → Atomically consumes `n` requests. The maximum batch size is `1 + burst`; larger batches are denied.
  - `wait(io, key)` → Blocks until allowed (uses `std.Io.sleep`)
  - `waitN(io, n)` / `waitN(io, key, n)` → Blocks until an entire batch is allowed; impossible batches return `error.BatchTooLarge`.
- **Clocks:**
  - `init(io, config)` → Installs Zig's monotonic `.awake` clock automatically
  - `initWithClock(config, clock)` → Injects a custom clock for deterministic tests
  - `ManualClock` → Built-in manually controlled testing clock

## Usage

```zig
const std = @import("std");
const zimit = @import("zimit");

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;

    var limiter = try zimit.RateLimiter([]const u8).init(io, .{
        .allocator = gpa,
        .limit = .perSecond(5),
        .burst = 2,
    });
    defer limiter.deinit();

    const key = "127.0.0.1";

    var i: usize = 0;
    while (i < 5) : (i += 1) {
        const decision = try limiter.allow(key);
        switch (decision) {
            .allowed => std.debug.print("allowed\n", .{}),
            .denied => {
                std.debug.print("retry in {d}ms\n", .{
                    decision.retryAfterMillisecondsCeil().?,
                });
            },
        }
    }
}

```

Use `Limit.perSecond`, `Limit.perMinute`, or `Limit.perHour` for common rates.
For an arbitrary period, initialize `Limit` directly with `count` and
`period_ns`.

See [examples](examples) for more.

## Key types

String keys are copied automatically, and integer keys work directly.
Applications using structured keys with custom equality or memory ownership can
use `RateLimiterWithContext`.


## Installation

Run:

```shell
zig fetch --save git+https://github.com/minhqdao/zimit.git#0.2.1
```

Then in your `build.zig`:

```zig
const zimit_dep = b.dependency("zimit", .{
    .target = target,
    .optimize = optimize,
});

const exe = b.addExecutable(.{
    .name = "yourapp",
    .root_module = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "zimit", .module = zimit_dep.module("zimit") },
        },
    }),
});
```

## Format

Format all Zig files before committing to prevent the CI from failing:

```shell
zig fmt src/ examples/ build.zig
```

## Linting

Install ZLS 0.16.0 and ensure `zls` is available in `PATH`, then run:

```shell
python3 tools/zls_lint.py
```

The script sends every project Zig source file to ZLS and fails on any diagnostic, using the same results reported by editor integrations.

## Testing

Run the test suite with:

```shell
zig build test
```

## License
[MIT](LICENSE)
