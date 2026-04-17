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
    // `zig build test` runs all tests across every source file.
    // We add each file explicitly so new files are never silently skipped.
    const test_step = b.step("test", "Run all zimit tests");

    const test_files = [_][]const u8{
        "src/types.zig",
        "src/gcra.zig",
        "src/root.zig",
    };

    for (test_files) |file| {
        const t = b.addTest(.{
            .root_module = b.createModule(.{
                .root_source_file = b.path(file),
                .target = target,
                .optimize = optimize,
            }),
        });
        t.root_module.addImport("zimit", zimit_mod);

        const run_t = b.addRunArtifact(t);
        run_t.has_side_effects = true; // always re-run, never cache
        test_step.dependOn(&run_t.step);
    }

    // ── Examples ─────────────────────────────────────────────────────────────
    //
    // Run all examples with `zig build examples`.
    const examples_step = b.step("examples", "Build and run all examples");

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

        const run = b.addRunArtifact(exe);
        examples_step.dependOn(&run.step);
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

    // ── Benchmark stub ────────────────────────────────────────────────────────
    //
    // `zig build bench` — placeholder for a future bench/main.zig.
    // Add it now so CI can run it without changing build.zig later.
    const bench_step = b.step("bench", "Run benchmarks (see bench/main.zig)");
    _ = bench_step; // remove the _ when bench/main.zig exists
}
