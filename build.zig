const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // ── Library module (what dependents import) ───────────────────────────────
    //
    // This is the only thing a consumer of zimit actually needs.
    const zimit_mod = b.addModule("zimit", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });

    // ── Static library artifact (optional — for C consumers) ─────────────────
    //
    // Not needed for pure-Zig dependents, but useful if someone wants to
    // link zimit into a C project. Produces zig-out/lib/libzimit.a.
    const lib = b.addLibrary(.{
        .name = "zimit",
        .root_module = zimit_mod,
        .linkage = .static,
    });
    b.installArtifact(lib);

    // ── Tests ─────────────────────────────────────────────────────────────────
    //
    // Tests imported by root.zig are discovered transitively by Zig.
    const test_step = b.step("test", "Run all zimit tests");
    const tests = b.addTest(.{
        .root_module = zimit_mod,
    });
    const run_tests = b.addRunArtifact(tests);
    test_step.dependOn(&run_tests.step);

    // ── Examples ─────────────────────────────────────────────────────────────
    //
    // Compile examples without executing them. Use `zig build smoke` to run
    // them, including examples that deliberately wait between admissions.
    const examples_step = b.step("examples", "Compile all examples");
    const smoke_step = b.step("smoke", "Build and run all examples");

    const example_names = [_][]const u8{
        "global-limiter",
        "global-limiter-wait",
        "rate-limiter",
    };

    for (example_names) |name| {
        const exe = b.addExecutable(.{
            .name = name,
            .root_module = b.createModule(.{
                .root_source_file = b.path(b.fmt("examples/{s}/src/main.zig", .{name})),
                .target = target,
                .optimize = optimize,
                .imports = &.{
                    .{ .name = "zimit", .module = zimit_mod },
                },
            }),
        });

        examples_step.dependOn(&exe.step);

        const run = b.addRunArtifact(exe);
        smoke_step.dependOn(&run.step);
    }

    // ── Docs ──────────────────────────────────────────────────────────────────
    //
    // `zig build docs` emits HTML documentation into zig-out/docs/.
    // Point your CI at this and upload to GitHub Pages / Codeberg Pages.
    const docs = b.addInstallDirectory(.{
        .source_dir = lib.getEmittedDocs(),
        .install_dir = .prefix,
        .install_subdir = "docs",
    });
    const docs_step = b.step("docs", "Emit HTML documentation to zig-out/docs/");
    docs_step.dependOn(&docs.step);

    // ── Benchmarks ────────────────────────────────────────────────────────────
    const bench_iterations = b.option(
        usize,
        "bench-iterations",
        "Operations per admission benchmark",
    ) orelse 1_000_000;
    const bench_keys = b.option(
        usize,
        "bench-keys",
        "Keys used by storage and pruning benchmarks",
    ) orelse 20_000;
    const bench_threads_requested = b.option(
        usize,
        "bench-threads",
        "Threads used by the contention benchmark (1-64)",
    ) orelse 4;
    const bench_threads = std.math.clamp(bench_threads_requested, 1, 64);

    const bench_options = b.addOptions();
    bench_options.addOption(usize, "iterations", @max(bench_iterations, 1));
    bench_options.addOption(usize, "keys", @max(bench_keys, 1));
    bench_options.addOption(usize, "threads", bench_threads);

    const bench_zimit_mod = b.createModule(.{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    const bench_exe = b.addExecutable(.{
        .name = "zimit-bench",
        .root_module = b.createModule(.{
            .root_source_file = b.path("bench/main.zig"),
            .target = target,
            .optimize = .ReleaseFast,
            .imports = &.{
                .{ .name = "zimit", .module = bench_zimit_mod },
                .{ .name = "bench_options", .module = bench_options.createModule() },
            },
        }),
    });
    const run_bench = b.addRunArtifact(bench_exe);
    const bench_step = b.step("bench", "Run limiter benchmarks (ReleaseFast)");
    bench_step.dependOn(&run_bench.step);
}
