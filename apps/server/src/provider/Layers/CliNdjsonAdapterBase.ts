/**
 * CliNdjsonAdapterBase — NDJSON 族 CLI 通用运转层（P0 萃取）
 *
 * 目标：把 Antigravity + 未来同形 `stream-json` CLI 的 4 件套样板
 *  `pendingApprovals / 附件→base64 / 事件冲压 / 中断三段式` 收敛到一处，
 *  不碰「协议层」帧→事件映射（仍由各 Adapter 实现 handleFrame）。
 *
 * 本文件为 B1 的“骨架版”：先让类型过、结构定，再在 B2 把 Antigravity 真正瘦身。
 * 对应：`docs/research/2026-08-27-cross-cli-generalization.md` §4.1 §5.2 §6 P1
 *
 * @module provider/Layers/CliNdjsonAdapterBase
 */

import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Queue from "effect/Queue";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/unstable/process/ChildProcess";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";

import { resolveAttachmentPath } from "../../attachmentStore.ts";
import { ServerConfig } from "../../config.ts";
import {
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterValidationError,
} from "../Errors.ts";
import type { EventNdjsonLogger } from "./EventNdjsonLogger.ts";
import type {
  ChatAttachment,
  ProviderApprovalDecision,
  ProviderRuntimeEvent,
  ThreadId,
} from "@t3tools/contracts";
import { EventId, ProviderDriverKind } from "@t3tools/contracts";

// ---------------------------------------------------------------------------
// 共享类型
// ---------------------------------------------------------------------------

export interface PendingApproval {
  readonly requestType: string;
  readonly detail: string;
  readonly decision: Deferred.Deferred<ProviderApprovalDecision>;
  readonly toolName: string;
  readonly toolItemId: string;
}

// ---------------------------------------------------------------------------
// 事件冲压辅助
// ---------------------------------------------------------------------------

export const makeCliEventSourcing = Effect.fn("makeCliEventSourcing")(function* () {
  const queue = yield* Queue.unbounded<ProviderRuntimeEvent>();
  let counter = 0;
  const nextEventId = Effect.map(DateTime.now, (now) =>
    EventId.make(`cli-${DateTime.toEpochMillis(now)}-${(counter += 1)}` as unknown as string),
  );
  const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
  const offer = (event: ProviderRuntimeEvent): Effect.Effect<void> =>
    Queue.offer(queue, event).pipe(Effect.asVoid);
  return {
    queue,
    nextEventId,
    nowIso,
    offer,
    get counter() {
      return counter;
    },
  };
});

// ---------------------------------------------------------------------------
// 审批队列
// ---------------------------------------------------------------------------

export const settlePendingApprovalsAsCancelled = (
  pendingApprovals: ReadonlyMap<string, PendingApproval>,
): Effect.Effect<void> =>
  Effect.forEach(
    Array.from(pendingApprovals.values()),
    (pending) =>
      Deferred.succeed(pending.decision, "cancel" as ProviderApprovalDecision).pipe(Effect.ignore),
    { discard: true },
  );

// ---------------------------------------------------------------------------
// 附件
// ---------------------------------------------------------------------------

export const resolveCliAttachment = (
  attachment: ChatAttachment,
  attachmentsDir: string,
  fileSystem: FileSystem.FileSystem,
  provider: any,
): Effect.Effect<
  {
    readonly type: "image";
    readonly mimeType: string;
    readonly data: string;
    readonly name?: string;
  },
  ProviderAdapterValidationError | ProviderAdapterRequestError
> =>
  Effect.gen(function* () {
    const attachmentPath = resolveAttachmentPath({ attachmentsDir, attachment });
    if (!attachmentPath) {
      return yield* new ProviderAdapterValidationError({
        provider,
        operation: "sendTurn",
        issue: `Invalid attachment id '${attachment.id}'.`,
      });
    }
    const bytes = yield* fileSystem.readFile(attachmentPath).pipe(
      Effect.mapError(
        (cause) =>
          new ProviderAdapterRequestError({
            provider,
            method: "turn/start",
            detail: `Failed to read attachment file: ${(cause as unknown as { message?: string }).message ?? String(cause)}.`,
            cause,
          }),
      ),
    );
    const base64 = Buffer.from(bytes).toString("base64");
    if (!attachment.mimeType.startsWith("image/")) {
      return yield* new ProviderAdapterValidationError({
        provider,
        operation: "sendTurn",
        issue: `Unsupported attachment mimeType '${attachment.mimeType}'.`,
      });
    }
    return {
      type: "image" as const,
      mimeType: attachment.mimeType,
      data: base64,
      name: attachment.name,
    };
  });

// ---------------------------------------------------------------------------
// 进程
// ---------------------------------------------------------------------------

export const spawnCliProcess = (
  binaryPath: string,
  args: ReadonlyArray<string>,
  cwd: string,
  environment: NodeJS.ProcessEnv,
  scope: Scope.Scope,
  spawner: any,
  provider: any,
): Effect.Effect<any, ProviderAdapterProcessError> => {
  const command = ChildProcess.make(binaryPath, args, { cwd, env: environment, extendEnv: true });
  return (spawner as any).spawn(command).pipe(
    Effect.provideService(Scope.Scope, scope),
    Effect.mapError(
      (cause: unknown) =>
        new ProviderAdapterProcessError({
          provider,
          threadId: "" as ThreadId,
          detail: `Failed to spawn CLI (${binaryPath}).`,
          cause,
        }),
    ),
  ) as unknown as Effect.Effect<any, ProviderAdapterProcessError>;
};

// ---------------------------------------------------------------------------
// NDJSON 流模板
// ---------------------------------------------------------------------------

export const runNdjsonStdin = (
  child: ChildProcessSpawner.ChildProcessHandle,
  promptFrame: string,
  provider: any,
): Effect.Effect<void, ProviderAdapterProcessError> =>
  Stream.run(Stream.encodeText(Stream.make(promptFrame)), child.stdin).pipe(
    Effect.mapError(
      (cause) =>
        new ProviderAdapterProcessError({
          provider,
          threadId: "" as ThreadId,
          detail: "Failed to write prompt to CLI stdin.",
          cause,
        }),
    ),
  );

export const interruptViaStdin = (
  child: ChildProcessSpawner.ChildProcessHandle,
): Effect.Effect<void> =>
  Stream.run(
    Stream.encodeText(Stream.make(`${globalThis.JSON.stringify({ event: "interrupt" })}\n`)),
    child.stdin,
  ).pipe(Effect.timeout(500), Effect.ignore, Effect.asVoid);

// ---------------------------------------------------------------------------
// 抽象基类骨架（B2 真正继承）
// ---------------------------------------------------------------------------

export abstract class CliNdjsonAdapterBase {
  // 子类实现：帧解析与业务映射
  protected abstract parseFrame(line: string): import("effect/Option").Option<unknown>;
  protected abstract buildSpawnArgs(...args: unknown[]): ReadonlyArray<string>;

  // 工具：供子类复用
  protected makeEventSourcing = makeCliEventSourcing;
  protected settlePendingApprovals = settlePendingApprovalsAsCancelled;
  protected resolveAttachment = resolveCliAttachment;
  protected spawnProcess = spawnCliProcess;
  protected runStdin = runNdjsonStdin;
  protected interruptStdin = interruptViaStdin;

  // 供子类调用的 ServerConfig / FileSystem 访问器（避免在基类直接 yield*）
  protected getServerConfig = Effect.fn("getServerConfig")(function* () {
    return yield* ServerConfig;
  });
  protected getFileSystem = Effect.fn("getFileSystem")(function* () {
    return yield* FileSystem.FileSystem;
  });
  protected getSpawner = Effect.fn("getSpawner")(function* () {
    return yield* ChildProcessSpawner.ChildProcessSpawner;
  });
}

// 保留一个显式的“基类已存在”标记，供 B2 校验
export const CLI_NDJSON_BASE_VERSION = 1 as const;

void ChildProcess;
void EventId;
