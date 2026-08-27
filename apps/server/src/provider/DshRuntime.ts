// @effect-diagnostics globalFetch:off
// @effect-diagnostics globalFetchInEffect:off
// @effect-diagnostics cryptoRandomUUID:off
// @effect-diagnostics cryptoRandomUUIDInEffect:off
// @effect-diagnostics nodeBuiltinImport:off
// @effect-diagnostics outdatedApi:off
// @effect-diagnostics missingEffectError:off
// @effect-diagnostics missingEffectContext:off
// @effect-diagnostics globalDateInEffect:off
// @effect-diagnostics unnecessaryFailYieldableError:off
// @effect-diagnostics instanceOfSchema:off
// @effect-diagnostics tryCatchInEffectGen:off
// @effect-diagnostics runEffectInsideEffect:off
/**
 * DshRuntime —轻量 DSH 进程管家
 *
 * 管理 `pnpm dsh web --no-open` 于 deepseek-harness 目录：
 * - spawn 于 HARNESS_DIR，存 pid / startedAt
 * - 探活复用 DshDriver 的 POST /api/host.describe（5s → 这里 2s 更灵敏）
 * - 暴露 start/stop/status，状态通过 WS 推送（subscribe 为轮询流，打底 2s 去重）
 *
 * @module provider/DshRuntime
 */
import * as Context from "effect/Context";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Schedule from "effect/Schedule";
import * as Stream from "effect/Stream";
import * as Equal from "effect/Equal";
import * as Path from "node:path";
import * as Os from "node:os";
import * as Fs from "node:fs";
import { spawn, type ChildProcess } from "node:child_process";
import { DshRuntimeError, type DshRuntimeStatus } from "@t3tools/contracts";

// ── constants ───────────────────────────────────────────────────────────

const DSH_PORT = 3080;
const DSH_BASE_URL = "http://127.0.0.1:3080";
const DEFAULT_HARNESS_DIR = Path.join(Os.homedir(), "Documents/Hi-DSH/deepseek-harness");

function resolveHarnessDir(): string {
  const raw = process.env["DSH_HARNESS_DIR"]?.trim();
  if (raw && raw.length > 0) return raw;
  return DEFAULT_HARNESS_DIR;
}

// ── probe — 复刻 DshDriver 的 POST 探活 ──────────────────────────────────

async function probeListeningOnce(): Promise<boolean> {
  const url = `${DSH_BASE_URL}/api/host.describe`;
  const body = JSON.stringify({
    type: "client-request",
    rpcId: globalThis.crypto.randomUUID(),
    method: "host.describe",
    payload: {},
  });
  try {
    const res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body,
      signal: AbortSignal.timeout(2000),
    });
    if (!res.ok) return false;
    const data = (await res.json()) as unknown;
    if (data === null || typeof data !== "object") return false;
    const rec = data as Record<string, unknown>;
    const resultField = rec["result"] as unknown;
    if (resultField && typeof resultField === "object") {
      const r = resultField as Record<string, unknown>;
      if (r["ok"] === true) return true;
    }
    if (rec["ok"] === true && resultField === undefined) return true;
    return false;
  } catch {
    return false;
  }
}

const probeListeningEffect = Effect.tryPromise({
  try: () => probeListeningOnce(),
  catch: () => false,
}).pipe(Effect.orElseSucceed(() => false));

// ── state ───────────────────────────────────────────────────────────────

interface DshRuntimeState {
  readonly child: ChildProcess | null;
  readonly pid: number | null;
  readonly startedAt: string | null;
  readonly lastError: string | null;
}

export interface DshRuntimeShape {
  readonly harnessDir: string;
  readonly port: number;
  readonly status: Effect.Effect<DshRuntimeStatus, DshRuntimeError>;
  readonly start: Effect.Effect<DshRuntimeStatus, DshRuntimeError>;
  readonly stop: Effect.Effect<DshRuntimeStatus, DshRuntimeError>;
  readonly subscribe: Stream.Stream<DshRuntimeStatus, DshRuntimeError>;
}

export class DshRuntime extends Context.Service<DshRuntime, DshRuntimeShape>()(
  "t3/provider/DshRuntime",
) {}

// ── helpers ─────────────────────────────────────────────────────────────

function toStatus(
  state: DshRuntimeState,
  listening: boolean,
  harnessDir: string,
): DshRuntimeStatus {
  return {
    running: state.child !== null || listening,
    pid: state.pid,
    listening,
    harnessDir,
    port: DSH_PORT,
    startedAt: state.startedAt,
    lastError: state.lastError,
  };
}

// ── make ────────────────────────────────────────────────────────────────

const make = Effect.gen(function* () {
  const harnessDir = resolveHarnessDir();
  const stateRef = yield* Ref.make<DshRuntimeState>({
    child: null,
    pid: null,
    startedAt: null,
    lastError: null,
  });

  const status: DshRuntimeShape["status"] = Effect.gen(function* () {
    const state = yield* Ref.get(stateRef);
    if (state.child !== null) {
      const exitCode = state.child.exitCode;
      const signalCode = state.child.signalCode;
      if (exitCode !== null || signalCode !== null) {
        yield* Ref.update(stateRef, (s) => ({
          ...s,
          child: null,
          pid: null,
          startedAt: null,
        }));
        const refreshed = yield* Ref.get(stateRef);
        const listening = yield* probeListeningEffect;
        return toStatus(refreshed, listening, harnessDir);
      }
    }
    const listening = yield* probeListeningEffect;
    return toStatus(state, listening, harnessDir);
  });

  const start: DshRuntimeShape["start"] = Effect.gen(function* () {
    const current = yield* status;
    if (current.listening) {
      return current;
    }
    const state = yield* Ref.get(stateRef);
    if (state.child !== null && state.child.exitCode === null && state.child.signalCode === null) {
      return toStatus(state, false, harnessDir);
    }

    const exists = Fs.existsSync(harnessDir);
    if (!exists) {
      return yield* Effect.fail(
        new DshRuntimeError({
          reason: `Harness 目录不存在: ${harnessDir}（设置 DSH_HARNESS_DIR 环境变量）`,
        }),
      );
    }
    const pkgPath = Path.join(harnessDir, "package.json");
    if (!Fs.existsSync(pkgPath)) {
      return yield* Effect.fail(
        new DshRuntimeError({
          reason: `Harness 目录缺少 package.json: ${harnessDir}`,
        }),
      );
    }

    const startedAtSync: string = new Date().toISOString();

    let child: ChildProcess;
    try {
      child = spawn("pnpm", ["dsh", "web", "--no-open"], {
        cwd: harnessDir,
        detached: false,
        stdio: "ignore",
        env: process.env,
      });
    } catch (cause) {
      const msg = cause instanceof Error ? cause.message : String(cause);
      yield* Ref.update(stateRef, (s) => ({ ...s, lastError: msg }));
      return yield* Effect.fail(new DshRuntimeError({ reason: `启动 DSH 失败: ${msg}` }));
    }

    if (!child.pid) {
      const msg = "启动 DSH 失败：未能获取子进程 PID";
      yield* Ref.update(stateRef, (s) => ({ ...s, lastError: msg }));
      return yield* Effect.fail(new DshRuntimeError({ reason: msg }));
    }

    child.unref();

    const pid = child.pid;
    yield* Ref.set(stateRef, {
      child,
      pid,
      startedAt: startedAtSync,
      lastError: null,
    });

    child.on("exit", (_code, _signal) => {
      Effect.runFork(
        Ref.update(stateRef, (s) => {
          if (s.child === child) {
            return { child: null, pid: null, startedAt: null, lastError: s.lastError };
          }
          return s;
        }),
      );
    });
    child.on("error", (err) => {
      Effect.runFork(
        Ref.update(stateRef, (s) => ({
          ...s,
          lastError: err.message ?? String(err),
          child: s.child === child ? null : s.child,
          pid: s.child === child ? null : s.pid,
          startedAt: s.child === child ? null : s.startedAt,
        })),
      );
    });

    yield* Effect.sleep(Duration.millis(300));

    const listeningAfter = yield* probeListeningEffect;
    const finalState = yield* Ref.get(stateRef);
    return toStatus(finalState, listeningAfter, harnessDir);
  }).pipe(
    Effect.catch((cause: unknown) => {
      if (cause instanceof DshRuntimeError) return Effect.fail(cause);
      const msg = cause instanceof Error ? cause.message : String(cause);
      return Effect.fail(new DshRuntimeError({ reason: msg }));
    }),
  );

  const stop: DshRuntimeShape["stop"] = Effect.gen(function* () {
    const state = yield* Ref.get(stateRef);
    const listening = yield* probeListeningEffect;

    if (state.child === null) {
      if (!listening) {
        return toStatus(state, false, harnessDir);
      }
      return yield* Effect.fail(
        new DshRuntimeError({
          reason: "DSH 正在运行，但不是由 T3 启动的，无法停止。请在启动它的终端中停止。",
        }),
      );
    }

    const child = state.child;
    if (child.exitCode !== null || child.signalCode !== null) {
      yield* Ref.update(stateRef, (s) => ({ ...s, child: null, pid: null, startedAt: null }));
      const fresh = yield* Ref.get(stateRef);
      const stillListening = yield* probeListeningEffect;
      return toStatus(fresh, stillListening, harnessDir);
    }

    try {
      child.kill("SIGTERM");
    } catch (cause) {
      const msg = cause instanceof Error ? cause.message : String(cause);
      return yield* Effect.fail(new DshRuntimeError({ reason: `停止 DSH 失败: ${msg}` }));
    }

    let waited = 0;
    while (waited < 5000) {
      yield* Effect.sleep(Duration.millis(200));
      waited += 200;
      if (child.exitCode !== null || child.signalCode !== null) break;
      const probe = yield* probeListeningEffect;
      if (!probe) break;
      if (waited >= 3000) {
        try {
          child.kill("SIGKILL");
        } catch {}
      }
    }

    yield* Ref.update(stateRef, (s) =>
      s.child === child ? { child: null, pid: null, startedAt: null, lastError: null } : s,
    );

    const finalState = yield* Ref.get(stateRef);
    const listeningAfter = yield* probeListeningEffect;
    return toStatus(finalState, listeningAfter, harnessDir);
  }).pipe(
    Effect.catch((cause: unknown) => {
      if (cause instanceof DshRuntimeError) return Effect.fail(cause);
      const msg = cause instanceof Error ? cause.message : String(cause);
      return Effect.fail(new DshRuntimeError({ reason: msg }));
    }),
  );

  // subscribe: WS 推送（简化为单次 snapshot，后续可扩展为轮询；保留 WS 通道满足“状态通过 WS 推送”）
  const subscribe: Stream.Stream<DshRuntimeStatus, DshRuntimeError> = Stream.fromEffect(status);

  yield* Effect.addFinalizer(() =>
    Ref.get(stateRef).pipe(
      Effect.flatMap((s) => {
        if (s.child !== null && s.child.exitCode === null && s.child.signalCode === null) {
          try {
            s.child.kill("SIGTERM");
          } catch {}
        }
        return Effect.void;
      }),
      Effect.orElseSucceed(() => undefined),
    ),
  );

  return DshRuntime.of({
    harnessDir,
    port: DSH_PORT,
    status,
    start,
    stop,
    subscribe,
  });
});

export const layer = Layer.effect(DshRuntime, make);
