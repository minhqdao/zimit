const std = @import("std");
const zimit = @import("zimit");

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;

    var sys = zimit.SystemClock.init(io);

    var limiter = try zimit.RateLimiter([]const u8).init(.{
        .allocator = gpa,
        .limit = .perSecond(5),
        .burst = 2,
        .clock = sys.clock(),
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
