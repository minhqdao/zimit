# zimit

A GCRA-based rate limiter for Zig 0.15+ with a token-bucket-like API.

Internally uses a single `i64` TAT per key. No floats, deterministic, and allocation-efficient.

## Installation

Add to your `build.zig.zon`:

```zig
.dependencies = .{
    .zimit = .{
        .url = "https://github.com/minhqdao/zimit/archive/0.1.0.tar.gz",
        .hash = "...", // run `zig build` once and paste the reported hash
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

var gpa = std.heap.GeneralPurposeAllocator(.{}){};
defer _ = gpa.deinit();

var sys = zimit.SystemClock{};
var limiter = try zimit.RateLimiter([]const u8).init(.{
    .allocator = gpa.allocator(),
    .rate      = 100,
    .per       = .second,
    .burst     = 20,
    .clock     = sys.clock(),
});
defer limiter.deinit();

switch (try limiter.allow("user")) {
    .allowed => handle(),
    .denied  => |d| std.Thread.sleep(@intCast(d.retry_after_ns)),
}
```

## Notes

- **Per-key limiting:** Each key is tracked independently (e.g. per user ID or IP address).
- **Global limiting:** Use `GlobalLimiter` when you want a single shared limit across all requests (e.g. protect total server throughput).
- **String keys are copied:** you can pass temporary `[]const u8` safely.
- **Blocking vs non-blocking:**
  - `allow()` → immediate decision
  - `wait()` → blocks until allowed
- **Clocks:**
  - `SystemClock` → production
  - `ManualClock` → deterministic tests
