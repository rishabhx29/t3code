import { assert, expect, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as NodeFS from "node:fs";

import { ensureTailscaleServe } from "./tailscale.ts";

// A stand-in for a hung `tailscale` CLI: reports its pid, swallows SIGTERM, and
// keeps running. A wedged real daemon behaves the same way, which is why the
// probe path escalates to SIGKILL.
const HUNG_TAILSCALE_STUB = `#!/usr/bin/env node
const fs = require("node:fs");
try {
  fs.writeFileSync(process.env.TAILSCALE_STUB_PID_FILE, String(process.pid));
} catch {}
process.on("SIGTERM", () => {
  // Hung: a wedged CLI does not exit on SIGTERM.
});
setInterval(() => {}, 60_000);
`;

const isAlive = (pid: number): boolean => {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
};

// Live clock: the bug's whole shape is real time — the serve call's 10 s
// timeout, the spawner's SIGTERM grace, and the SIGKILL escalation — none of
// which exist on the frozen TestClock that it.effect runs on.
it.live("escalates a hung serve command to SIGKILL", () =>
  Effect.gen(function* () {
    // The gap is POSIX-only: the Windows spawner's taskkill /F already
    // force-kills the group, so a leak can only be demonstrated where SIGTERM
    // must be escalated. The Platform reference's narrower type hides win32,
    // but its default is the real process platform, hence the string cast.
    const platform: string = yield* HostProcess.Platform;
    if (platform === "win32") return;

    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const stubDir = yield* fs.makeTempDirectory();
    const pidFile = path.join(stubDir, "stub.pid");
    const stubPath = path.join(stubDir, "tailscale");
    yield* fs.writeFileString(stubPath, HUNG_TAILSCALE_STUB);
    yield* fs.chmod(stubPath, 0o755);
    process.env.TAILSCALE_STUB_PID_FILE = pidFile;
    process.env.PATH = `${stubDir}:${process.env.PATH ?? ""}`;

    // Kill the stub last so a failed assertion cannot wedge this test on the
    // hung cleanup the same way the bug wedges the real caller.
    const killStub = Effect.gen(function* () {
      const text = yield* fs.readFileString(pidFile).pipe(Effect.orElseSucceed(() => ""));
      const pid = Number.parseInt(text, 10);
      if (Number.isInteger(pid)) {
        yield* Effect.try(() => process.kill(pid, "SIGKILL")).pipe(Effect.ignore);
      }
      yield* fs.remove(stubDir, { recursive: true }).pipe(Effect.ignore);
    });

    yield* Effect.gen(function* () {
      // The serve call's own 10 s timeout interrupts it and runs the spawner's
      // cleanup: SIGTERM to the process group, then — with forceKillAfter —
      // SIGKILL after the grace.
      const fiber = yield* Effect.forkChild(
        ensureTailscaleServe({ localPort: 45_678 }).pipe(Effect.result),
      );

      yield* Effect.promise(() =>
        expect
          .poll(() => Number.parseInt(NodeFS.readFileSync(pidFile, "utf8"), 10), {
            interval: 200,
            timeout: 5_000,
          })
          .toBeGreaterThan(0),
      );
      const pid = Number.parseInt(NodeFS.readFileSync(pidFile, "utf8"), 10);

      // With the escalation the call returns after its timeout; without it the
      // SIGTERM-ignoring process wedges the cleanup and the call never returns.
      const returned = yield* Fiber.join(fiber).pipe(
        Effect.timeoutOption("14 seconds"),
        Effect.map(Option.isSome),
      );
      assert.isTrue(returned, `the serve call never returned; process ${pid} wedged the cleanup`);
      assert.isFalse(isAlive(pid), `hung serve process ${pid} survived the call`);
    }).pipe(Effect.ensuring(killStub));
  }).pipe(Effect.provide(NodeServices.layer)),
);
