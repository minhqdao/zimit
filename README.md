# zimit

A GCRA-based rate limiter for Zig 0.16.0+ with a token-bucket-like API.

Internally uses a single `i64` TAT per key. No floats, deterministic, and allocation-efficient.

## Installation

Add to your `build.zig.zon`:

```zig
.dependencies = .{
    .zimit = .{
        .url = "https://github.com/minhqdao/zimit/archive/0.1.0.tar.gz",
        .hash = "zimit-0.1.0-PtOTgxF8AQBT7d_YrB1ObB4p5-Gpv0qDYmJ1siYn2iF0",
    },
},
```

Then in `build.zig`:

```zig
const zimit = b.dependency("zimit", .{ .target = target, .optimize = optimize });
exe.root_module.addImport("zimit", zimit.module("zimit"));
```

## Example

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

    switch (try limiter.allow(key)) {
        .allowed => std.debug.print("Allowed!\n", .{}),
        .denied => |d| {
            std.debug.print("Denied, retrying in {d}ms...\n", .{d.retry_after_ms_ceil()});
        },
    }
}
```

## Notes

- **Per-key limiting:** Each key is tracked independently (e.g. per user ID or IP address).
- **Global limiting:** Use `GlobalLimiter` when you want a single shared limit across all requests (e.g. protect total server throughput).
- **String keys are copied:** you can pass temporary `[]const u8` safely.
- **Blocking vs non-blocking:**
  - `allow()` → immediate decision
  - `wait(io, key)` → blocks until allowed (uses `std.Io.sleep`)
- **Clocks:**
  - `SystemClock` → production (requires `std.Io`)
  - `ManualClock` → deterministic tests
