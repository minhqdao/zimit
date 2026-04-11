# zimit

A GCRA-based rate limiter for Zig 0.15+.

Exposes a token-bucket-flavored API while running a Generic Cell Rate Algorithm
(GCRA) engine underneath — single `i64` TAT per key, no floats, atomic-friendly.

## Features

- Pure-function GCRA core — no allocations, no global state, trivially testable
- Multi-key `HashMap`-backed limiter for strings, integers, or any hashable type
- Manual clock injection — deterministic tests without sleeping
- Atomic batch requests via `allow_n`
- `retry_after_ns` / `retry_after_ms` on every denial — caller decides how to wait

## Installation

Add to your `build.zig.zon`:

```zig
.dependencies = .{
    .zimit = .{
        .url = "https://github.com/minhqdao/zimit/archive/0.1.0.tar.gz",
        .hash = "...", // paste the hash zig tells you on first build
    },
},
```

Then in `build.zig`:

```zig
const zimit = b.dependency("zimit", .{ .target = target, .optimize = optimize });
exe.root_module.addImport("zimit", zimit.module("zimit"));
```

## Usage

```zig
const zimit = @import("zimit");

var sys_clock = zimit.SystemClock{};
var limiter = try zimit.RateLimiter([]const u8).init(.{
    .allocator = allocator,
    .rate      = 100,
    .per       = .second,
    .burst     = 20,
    .clock     = sys_clock.clock(),
});
defer limiter.deinit();

switch (try limiter.allow("192.168.1.1")) {
    .allowed => handleRequest(),
    .denied  => |d| sendTooManyRequests(d.retry_after_ms()),
}
```
