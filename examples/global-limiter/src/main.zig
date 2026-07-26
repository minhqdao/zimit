const std = @import("std");
const zimit = @import("zimit");

pub fn main(init: std.process.Init) !void {
    const io = init.io;

    var limiter = try zimit.GlobalLimiter.init(io, .{
        .limit = .perSecond(5),
        .burst = 2,
    });

    var i: usize = 0;
    while (i < 5) : (i += 1) {
        const decision = try limiter.allow();
        switch (decision) {
            .allowed => std.debug.print("allowed\n", .{}),
            .denied => {
                std.debug.print("retry in {d} ms\n", .{
                    decision.retryAfterMillisecondsCeil().?,
                });
            },
        }
    }
}
