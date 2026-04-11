//! zimit — a GCRA-based rate limiter for Zig.
//!
//! Start here:
//!     const zimit = @import("zimit");
//!     const Limit = zimit.Limit;
//!     const ManualClock = zimit.ManualClock;

pub const Limit = @import("types.zig").Limit;
pub const Decision = @import("types.zig").Decision;
pub const Clock = @import("types.zig").Clock;
pub const SystemClock = @import("types.zig").SystemClock;
pub const ManualClock = @import("types.zig").ManualClock;
pub const ZimitError = @import("types.zig").ZimitError;
