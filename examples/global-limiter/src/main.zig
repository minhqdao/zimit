const std = @import("std");
const zimit = @import("zimit");

pub fn main(init: std.process.Init) !void {
    const io = init.io;

    var sys = zimit.SystemClock.init(io);

    var limiter = try zimit.GlobalLimiter.init(.{
        .rate = 5,
        .per = .second,
        .burst = 2,
        .clock = sys.clock(),
    });

    var i: usize = 0;
    while (i < 5) : (i += 1) {
        switch (limiter.allow()) {
            .allowed => std.debug.print("allowed\n", .{}),
            .denied => |d| {
                std.debug.print("denied, time until allowed: {d} ms\n", .{d.retry_after_ms_ceil()});
            },
        }
    }
}
