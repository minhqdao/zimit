const std = @import("std");
const zimit = @import("zimit");

pub fn main(init: std.process.Init) !void {
    const io = init.io;

    var sys = zimit.SystemClock.init(io);

    var limiter = try zimit.GlobalLimiter.init(.{
        .rate = 1,
        .per = .second,
        .burst = 2,
        .clock = sys.clock(),
    });

    var i: usize = 0;
    while (i < 6) : (i += 1) {
        try limiter.wait(io);
        std.debug.print("Request {d} passed\n", .{i + 1});
    }
}
