# zimit

A zero-dependency GCRA-based rate limiter with a token-bucket-like API for Zig 0.16.0+.

## Installation

Add to your `build.zig.zon`:

```zig
.dependencies = .{
    .zimit = .{
        .url = "https://github.com/minhqdao/zimit/archive/0.2.1.tar.gz",
        .hash = "zimit-0.2.1-PtOTg1WIAQA_8l-Qv1udeHK7ATsY86s-P8vFAyyb_qOK",
    },
},
```

Then in `build.zig`:

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

## Usage

```zig
const std = @import("std");
const zimit = @import("zimit");

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;

    var sys = zimit.SystemClock.init(io);

    var limiter = try zimit.RateLimiter([]const u8).init(.{
        .allocator = gpa,
        .rate = 5,
        .per = .second,
        .burst = 2,
        .clock = sys.clock(),
    });
    defer limiter.deinit();

    const key = "127.0.0.1";

    var i: usize = 0;
    while (i < 5) : (i += 1) {
        switch (try limiter.allow(key)) {
            .allowed => std.debug.print("allowed\n", .{}),
            .denied => |d| {
                std.debug.print("denied, time until allowed: {d}ms\n", .{d.retry_after_ms_ceil()});
            },
        }
    }
}

```
See [examples](examples) for more.

## Notes

- **Per-key rate limiting:** Each key is tracked independently (e.g. per user ID or IP address).
- **Global limiting:** Use `GlobalLimiter` when you want a single shared limit across all requests (e.g. protect total server throughput).
- **Blocking vs non-blocking:**
  - `allow()` → Immediate decision
  - `wait(io, key)` → Blocks until allowed (uses `std.Io.sleep`)
- **Clocks:**
  - `SystemClock` → Production (requires `std.process.Init.io`)
  - `ManualClock` → Deterministic tests
