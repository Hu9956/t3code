/**
 * AntigravityAdapter — `ProviderAdapterShape` over the Antigravity CLI's
 * stream-json NDJSON protocol.
 *
 * The CLI is spawned once per session:
 *
 *   agy --input-format stream-json --output-format stream-json [--model X] [--effort Y] [--conversation ID]
 *
 * Protocol (verified against agy 1.1.21):
 *   - stdin:  one NDJSON message per prompt —
 *             {"event":"user","message":{"content":[{"type":"text","text":"…"}]}}
 *   - stdout: `init` (tool inventory) → `step_update`* → `result` per turn
 *   - the process serves one conversation; a new conversation means a new
 *     process or a respawn with --conversation for resume / in-session model switch.
 *
 * Full closed-loop scope (B1..B10):
 *   1. incremental stream (step_update → content.delta / item.*)
 *   2. attachments (image blocks via resolveAttachmentPath + FileSystem)
 *   3. in-session 切模型 (capabilities in-session + respawn preserving conversationId)
 *   4. effort 透传 (normalizeAgyEffort → --effort)
 *   5. resume (resumeCursor schemaVersion:1 conversationId via --conversation)
 *   6. approvals (pendingApprovals + Deferred + request.opened/resolved)
 *   7. graceful interrupt (stdin {event:"interrupt"} → kill fallback + respawn)
 *   8. error classification (classifyAgyErrorMessage → runtime.error/warning)
 *   9. token usage (result.usage → thread.token-usage.updated)
 *  10. permissions allow / dangerously-skip-permissions via runtimeMode
 *
 * @module provider/Layers/AntigravityAdapter
 */
import {
  ApprovalRequestId,
  type CanonicalItemType,
  type AntigravitySettings,
  type ChatAttachment,
  EventId,
  type ProviderApprovalDecision,
  type ProviderRuntimeEvent,
  type ProviderSession,
  ProviderDriverKind,
  type ProviderTurnStartResult,
  RuntimeItemId,
  RuntimeRequestId,
  type ThreadId,
  TurnId,
} from "@t3tools/contracts";
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Scope from "effect/Scope";
import * as Duration from "effect/Duration";
import * as Exit from "effect/Exit";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/unstable/process/ChildProcess";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";

import { resolveAttachmentPath } from "../../attachmentStore.ts";
import { ServerConfig } from "../../config.ts";
import {
  resolveCliAttachment as resolveCliAttachmentBase,
  settlePendingApprovalsAsCancelled as settlePendingBase,
  spawnCliProcess,
} from "./CliNdjsonAdapterBase.ts";
import {
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
} from "../Errors.ts";
import type { AntigravityAdapterShape } from "../Services/AntigravityAdapter.ts";

const PROVIDER = ProviderDriverKind.make("antigravity");

/** Version tag for resumeCursor — bump if shape changes (mirrors GROK_RESUME_VERSION). */
const AGY_RESUME_VERSION = 1 as const;

/** Persisted resume cursor shape: { schemaVersion:1, conversationId:string } */
interface AgyResumeCursor {
  readonly schemaVersion: typeof AGY_RESUME_VERSION;
  readonly conversationId: string;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function parseAgyResumeCursor(raw: unknown): AgyResumeCursor | undefined {
  if (!isRecord(raw)) return undefined;
  if (raw.schemaVersion !== AGY_RESUME_VERSION) return undefined;
  if (typeof raw.conversationId !== "string" || !raw.conversationId.trim()) return undefined;
  return { schemaVersion: AGY_RESUME_VERSION, conversationId: raw.conversationId.trim() };
}

function makeResumeCursor(conversationId: string): AgyResumeCursor {
  return { schemaVersion: AGY_RESUME_VERSION, conversationId };
}

function normalizeAgyEffort(raw: string | undefined): "low" | "medium" | "high" | undefined {
  if (!raw) return undefined;
  const v = raw.trim().toLowerCase();
  if (v === "low" || v === "l" || v === "effort_low") return "low";
  if (v === "medium" || v === "m" || v === "effort_medium") return "medium";
  if (v === "high" || v === "h" || v === "effort_high") return "high";
  return undefined;
}

type AgyEffort = NonNullable<ReturnType<typeof normalizeAgyEffort>>;

function classifyAgyErrorMessage(
  message: string,
): "permission_error" | "transport_error" | "validation_error" | "provider_error" {
  const lower = message.toLowerCase();
  if (lower.includes("permission") || lower.includes("not allowed") || lower.includes("denied")) {
    return "permission_error";
  }
  if (
    lower.includes("transport") ||
    lower.includes("enoent") ||
    lower.includes("not found") ||
    lower.includes("connection") ||
    lower.includes("timeout") ||
    lower.includes("network")
  ) {
    return "transport_error";
  }
  if (
    lower.includes("validation") ||
    lower.includes("invalid") ||
    lower.includes("bad request") ||
    lower.includes("bad input")
  ) {
    return "validation_error";
  }
  return "provider_error";
}

/** Whether a tool name is considered privileged and should surface an approval gate. */
function isPrivilegedTool(toolName: string): boolean {
  const lower = toolName.toLowerCase();
  return (
    lower.includes("command") ||
    lower.includes("bash") ||
    lower.includes("shell") ||
    lower.includes("write") ||
    lower.includes("edit") ||
    lower.includes("apply_patch") ||
    lower.includes("delete") ||
    lower.includes("remove") ||
    lower.includes("exec") ||
    toolName === "run_command" ||
    toolName === "execute_bash"
  );
}

function canonicalRequestTypeForTool(
  toolName: string,
):
  | "command_execution_approval"
  | "file_change_approval"
  | "file_read_approval"
  | "dynamic_tool_call" {
  const lower = toolName.toLowerCase();
  if (
    lower.includes("command") ||
    lower.includes("bash") ||
    lower.includes("shell") ||
    lower.includes("exec")
  ) {
    return "command_execution_approval";
  }
  if (
    lower.includes("write") ||
    lower.includes("edit") ||
    lower.includes("patch") ||
    lower.includes("apply") ||
    lower.includes("replace") ||
    lower.includes("create") ||
    lower.includes("sed") ||
    lower.includes("notebook") ||
    lower.includes("delete")
  ) {
    return "file_change_approval";
  }
  if (
    lower.includes("read") ||
    lower.includes("view") ||
    lower.includes("cat") ||
    lower.includes("grep") ||
    lower.includes("search") ||
    lower.includes("browser") ||
    lower.includes("list")
  ) {
    return "file_read_approval";
  }
  return "dynamic_tool_call";
}

function getAgyToolItemType(toolName: string): CanonicalItemType {
  const lower = toolName.toLowerCase();
  if (
    lower.includes("command") ||
    lower.includes("bash") ||
    lower.includes("shell") ||
    lower.includes("exec") ||
    toolName === "run_command" ||
    toolName === "send_command_input" ||
    toolName === "command_status"
  ) {
    return "command_execution";
  }
  if (
    lower.includes("write") ||
    lower.includes("edit") ||
    lower.includes("patch") ||
    lower.includes("apply") ||
    lower.includes("create") ||
    lower.includes("replace") ||
    lower.includes("sed") ||
    lower.includes("notebook") ||
    lower.includes("delete_knowledge")
  ) {
    return "file_change";
  }
  if (
    lower.includes("read") ||
    lower.includes("view") ||
    lower.includes("cat") ||
    lower.includes("grep") ||
    lower.includes("search") ||
    lower.includes("list_dir") ||
    lower.includes("list_resources") ||
    lower.includes("read_resource") ||
    lower.includes("browser") ||
    lower.includes("mcp") ||
    lower === "view_file" ||
    lower === "grep_search" ||
    lower === "search_web" ||
    lower === "list_dir"
  ) {
    return "mcp_tool_call";
  }
  if (lower.includes("agent")) {
    return "collab_agent_tool_call";
  }
  if (lower.includes("websearch") || lower.includes("web_search")) {
    return "web_search";
  }
  if (lower.includes("image") || toolName === "generate_image") {
    return "image_view";
  }
  return "dynamic_tool_call";
}

function summarizeAgyToolRequest(toolName: string, params: unknown): string {
  if (!isRecord(params)) {
    const raw = jsonStringify(params);
    return raw.length <= 400 ? `${toolName}: ${raw}` : `${toolName}: ${raw.slice(0, 397)}...`;
  }
  const rec = params as Record<string, unknown>;
  const commandValue = rec.CommandLine ?? rec.cmd ?? rec.command ?? rec.Command;
  if (typeof commandValue === "string" && commandValue.trim().length > 0) {
    return `${toolName}: ${commandValue.trim().slice(0, 400)}`;
  }
  const fileValue =
    rec.TargetFile ?? rec.AbsolutePath ?? rec.FilePath ?? rec.path ?? rec.file ?? rec.File;
  if (typeof fileValue === "string" && fileValue.trim().length > 0) {
    const base = fileValue.trim().split("/").pop() ?? fileValue.trim();
    const full = fileValue.trim();
    if (full.length <= 300) {
      return `${toolName}: ${base}`;
    }
    return `${toolName}: ${base} — ${full.slice(0, 300)}`;
  }
  const query = rec.Query ?? rec.query ?? rec.SearchQuery ?? rec.searchQuery;
  const searchPath = rec.SearchPath ?? rec.searchPath ?? rec.path;
  if (typeof query === "string" && query.trim().length > 0) {
    const qp =
      typeof searchPath === "string" && searchPath.trim().length > 0
        ? `${query} in ${searchPath}`
        : query;
    return `${toolName}: ${qp.slice(0, 400)}`;
  }
  const serialized = jsonStringify(params);
  if (serialized.length <= 400) {
    return `${toolName}: ${serialized}`;
  }
  return `${toolName}: ${serialized.slice(0, 397)}...`;
}

function extractAgyCommand(params: unknown): string | undefined {
  if (!isRecord(params)) return undefined;
  const rec = params as Record<string, unknown>;
  const v = rec.CommandLine ?? rec.cmd ?? rec.command ?? rec.Command;
  return typeof v === "string" && v.trim().length > 0 ? v.trim().slice(0, 4000) : undefined;
}

function extractAgyChangedFiles(params: unknown): string[] | undefined {
  if (!isRecord(params)) return undefined;
  const rec = params as Record<string, unknown>;
  const v = rec.TargetFile ?? rec.AbsolutePath ?? rec.FilePath ?? rec.path ?? rec.file;
  if (typeof v === "string" && v.trim().length > 0) {
    return [v.trim()];
  }
  return undefined;
}

function truncateDetail(value: string, maxLen: number): string {
  return value.length <= maxLen ? value : `${value.slice(0, maxLen - 3)}...`;
}

interface AgyResultFrame {
  readonly conversation_id?: string;
  readonly status?: string;
  readonly response?: string;
  readonly error?: string;
  readonly output?: string;
}

interface AgyStepUpdate {
  readonly conversation_id?: string;
  readonly step_index?: number;
  readonly state?: string;
  readonly step_type?: string;
  readonly tool_name?: string;
  readonly text_delta?: string;
  readonly thinking?: string;
  readonly duration_seconds?: number;
  readonly tool_info?: {
    readonly name?: string;
    readonly parameters?: unknown;
    readonly output?: string;
    readonly error?: { readonly message?: string; readonly type?: string };
  };
  readonly usage?: {
    readonly input_tokens?: number;
    readonly output_tokens?: number;
    readonly thinking_tokens?: number;
    readonly total_tokens?: number;
    readonly cache_read_tokens?: number;
    readonly cachedInputTokens?: number;
  };
}

interface AgyNativeFrameLike {
  readonly event?: string;
  readonly conversation_id?: string;
  readonly result?: AgyResultFrame;
  readonly step_update?: AgyStepUpdate;
  readonly init?: unknown;
}

interface PendingApproval {
  readonly requestType: ReturnType<typeof canonicalRequestTypeForTool>;
  readonly detail: string;
  readonly decision: Deferred.Deferred<ProviderApprovalDecision>;
  readonly toolName: string;
  readonly toolItemId: string;
}

interface AgySessionContext {
  readonly threadId: ThreadId;
  conversationId: string | undefined;
  readonly child: ChildProcessSpawner.ChildProcessHandle;
  readonly scope: Scope.Closeable;
  model: string | undefined;
  effort: AgyEffort | undefined;
  readonly runtimeMode: string;
  readonly pendingApprovals: Map<string, PendingApproval>;
  readonly createdAt: string;
}

function buildAgySpawnArgs(
  model: string | undefined,
  effort: AgyEffort | undefined,
  conversationId: string | undefined,
  runtimeMode: string | undefined,
): ReadonlyArray<string> {
  const args: Array<string> = ["--input-format", "stream-json", "--output-format", "stream-json"];
  if (model) {
    args.push("--model", model);
  }
  if (effort) {
    args.push("--effort", effort);
  }
  if (conversationId) {
    args.push("--conversation", conversationId);
  }
  if (runtimeMode === "full-access") {
    args.push("--dangerously-skip-permissions");
  }
  if (!runtimeMode && !args.includes("--dangerously-skip-permissions")) {
    args.push("--dangerously-skip-permissions");
  }
  return args;
}

function buildUserMessageFrame(
  prompt: string,
  attachments?: ReadonlyArray<{
    readonly type: "image";
    readonly mimeType: string;
    readonly data: string;
    readonly name?: string;
  }>,
): string {
  if (!attachments || attachments.length === 0) {
    return `${globalThis.JSON.stringify({
      event: "user",
      message: {
        content: [{ type: "text", text: prompt }],
      },
    })}\n`;
  }
  const contentBlocks: Array<Record<string, unknown>> = [{ type: "text", text: prompt }];
  for (const att of attachments) {
    // agy expects image as { type: "image", source: { type: "base64", media_type, data } } or similar
    contentBlocks.push({
      type: "image",
      source: {
        type: "base64",
        media_type: att.mimeType,
        data: att.data,
      },
    });
    // also support alternative shape for compatibility
    // { type: "image", mimeType, dataUrl }
  }
  return `${globalThis.JSON.stringify({
    event: "user",
    message: {
      content: contentBlocks,
    },
  })}\n`;
}

/** Parse one NDJSON line; malformed lines are dropped (diagnostics may interleave). */
function parseFrame(line: string): Option.Option<AgyNativeFrameLike> {
  try {
    // oxlint-disable-next-line no-restricted-syntax -- NDJSON is genuinely unknown
    return Option.some(globalThis.JSON.parse(line) as AgyNativeFrameLike);
  } catch {
    return Option.none();
  }
}

const jsonStringify = (value: unknown): string => globalThis.JSON.stringify(value);

function settlePendingApprovalsAsCancelled(
  pendingApprovals: ReadonlyMap<string, PendingApproval>,
): Effect.Effect<void> {
  // 委托给 CliNdjsonAdapterBase 的通用实现（P0 萃取）
  return settlePendingBase(
    pendingApprovals as unknown as ReadonlyMap<
      string,
      import("./CliNdjsonAdapterBase.ts").PendingApproval
    >,
  ) as unknown as Effect.Effect<void>;
}

export const makeAntigravityAdapter = Effect.fn("makeAntigravityAdapter")(function* (
  antigravitySettings: AntigravitySettings,
  options?: {
    readonly instanceId?: string;
    readonly environment?: NodeJS.ProcessEnv;
  },
) {
  const serverConfig = yield* ServerConfig;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const fileSystem = yield* FileSystem.FileSystem;
  const environment = options?.environment ?? process.env;
  const binaryPath = antigravitySettings.binaryPath?.trim() || "agy";

  const sessions = new Map<ThreadId, AgySessionContext>();
  const runtimeEventQueue = yield* Queue.unbounded<ProviderRuntimeEvent>();

  let eventCounter = 0;
  const nextEventId = Effect.map(DateTime.now, (now) =>
    EventId.make(`agy-${DateTime.toEpochMillis(now)}-${(eventCounter += 1)}`),
  );
  const nowIso = Effect.map(DateTime.now, DateTime.formatIso);

  const offerRuntimeEvent = (event: ProviderRuntimeEvent): Effect.Effect<void> =>
    Queue.offer(runtimeEventQueue, event).pipe(Effect.asVoid);

  // 进程产卵已收敛到 CliNdjsonAdapterBase.spawnCliProcess，当前保留薄包装以兼容 Scope 注入（B2 下一步将完全委托）
  const spawnSessionProcess = (
    model: string | undefined,
    effort: AgyEffort | undefined,
    conversationId: string | undefined,
    runtimeMode: string | undefined,
    scope: Scope.Scope,
  ) => {
    const command = ChildProcess.make(
      binaryPath,
      buildAgySpawnArgs(model, effort, conversationId, runtimeMode),
      {
        cwd: serverConfig.cwd,
        env: environment,
        extendEnv: true,
      },
    );
    return spawner.spawn(command).pipe(
      Effect.provideService(Scope.Scope, scope),
      Effect.mapError(
        (cause) =>
          new ProviderAdapterProcessError({
            provider: PROVIDER,
            threadId: "" as ThreadId,
            detail: `Failed to spawn the Antigravity CLI (${binaryPath}).`,
            cause,
          }),
      ),
    );
  };

  // 附件解析已收敛到 CliNdjsonAdapterBase.resolveCliAttachment（300MiB 限与 mime 校验统一在 attachmentStore）
  const resolveAttachment = Effect.fn("resolveAttachment")(function* (attachment: ChatAttachment) {
    return yield* resolveCliAttachmentBase(
      attachment,
      serverConfig.attachmentsDir,
      fileSystem,
      PROVIDER,
    );
  });

  /**
   * Write the prompt frame, then consume stdout until the turn's `result`
   * frame. Emits incremental `content.delta` / `item.started` / `item.completed`
   * as `step_update` arrives (B1) and surfaces privileged tools via
   * `request.opened` + Deferred awaiting `respondToRequest` (B2).
   */
  const runTurnToResult = (
    context: AgySessionContext,
    prompt: string,
    turnId: TurnId,
    attachments?: ReadonlyArray<{
      readonly type: "image";
      readonly mimeType: string;
      readonly data: string;
      readonly name?: string;
    }>,
  ): Effect.Effect<
    AgyResultFrame,
    ProviderAdapterProcessError | ProviderAdapterRequestError | ProviderAdapterValidationError
  > =>
    Effect.gen(function* () {
      const imageBlocks = attachments ?? [];
      yield* Stream.run(
        Stream.encodeText(Stream.make(buildUserMessageFrame(prompt, imageBlocks))),
        context.child.stdin,
      ).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderAdapterProcessError({
              provider: PROVIDER,
              threadId: context.threadId,
              detail: "Failed to write the prompt to the Antigravity CLI.",
              cause,
            }),
        ),
      );

      let resultFrame: AgyResultFrame | undefined;
      let hasEmittedDelta = false;
      // Track in-flight tool items for deduplication
      const toolItemStarted = new Set<string>();
      const reasoningItemStarted = new Set<string>();

      yield* context.child.stdout.pipe(
        Stream.decodeText(),
        Stream.splitLines,
        Stream.map((line) => line.trim()),
        Stream.filter((line) => line.length > 0),
        Stream.map(parseFrame),
        Stream.filter(Option.isSome),
        Stream.map((frameOption) => frameOption.value),
        Stream.runForEach((frame) =>
          Effect.gen(function* () {
            if (frame.event === "init" && frame.conversation_id) {
              context.conversationId = frame.conversation_id;
              return;
            }
            if (frame.event === "step_update" && frame.step_update) {
              const su = frame.step_update;
              // agent_response incremental text
              if (su.step_type === "agent_response" && su.text_delta) {
                hasEmittedDelta = true;
                yield* offerRuntimeEvent({
                  eventId: yield* nextEventId,
                  createdAt: yield* nowIso,
                  provider: PROVIDER,
                  threadId: context.threadId,
                  turnId,
                  itemId: RuntimeItemId.make(`agy-text-${turnId}-${su.step_index ?? 0}`),
                  type: "content.delta",
                  payload: {
                    streamKind: "assistant_text",
                    delta: su.text_delta,
                  },
                });
              }
              if (su.thinking && su.thinking.trim().length > 0) {
                hasEmittedDelta = true;
                const reasoningItemId = RuntimeItemId.make(
                  `agy-think-${turnId}-${su.step_index ?? 0}`,
                );
                const reasoningKey = reasoningItemId as unknown as string;
                if (!reasoningItemStarted.has(reasoningKey)) {
                  reasoningItemStarted.add(reasoningKey);
                  yield* offerRuntimeEvent({
                    eventId: yield* nextEventId,
                    createdAt: yield* nowIso,
                    provider: PROVIDER,
                    threadId: context.threadId,
                    turnId,
                    itemId: reasoningItemId,
                    type: "item.started",
                    payload: {
                      itemType: "reasoning",
                      status: "inProgress",
                      title: "Thinking",
                    },
                  });
                }
                yield* offerRuntimeEvent({
                  eventId: yield* nextEventId,
                  createdAt: yield* nowIso,
                  provider: PROVIDER,
                  threadId: context.threadId,
                  turnId,
                  itemId: reasoningItemId,
                  type: "content.delta",
                  payload: {
                    streamKind: "reasoning_text",
                    delta: su.thinking,
                  },
                });
              }
              // Complete reasoning items — batch close for this turn to avoid leak when thinking spans multiple steps
              if ((su.state === "DONE" || su.state === "ERROR") && reasoningItemStarted.size > 0) {
                const prefix = `agy-think-${turnId}-`;
                const toClose = Array.from(reasoningItemStarted).filter((k) =>
                  k.startsWith(prefix),
                );
                if (toClose.length > 0) {
                  for (const rkey of toClose) {
                    reasoningItemStarted.delete(rkey);
                    const rid = RuntimeItemId.make(rkey as unknown as string);
                    yield* offerRuntimeEvent({
                      eventId: yield* nextEventId,
                      createdAt: yield* nowIso,
                      provider: PROVIDER,
                      threadId: context.threadId,
                      turnId,
                      itemId: rid,
                      type: "item.completed",
                      payload: {
                        itemType: "reasoning",
                        status: su.state === "DONE" ? "completed" : "failed",
                      },
                    });
                  }
                } else {
                  // fallback exact match (covers legacy key shape)
                  const rid = RuntimeItemId.make(`agy-think-${turnId}-${su.step_index ?? 0}`);
                  const rkey = rid as unknown as string;
                  if (reasoningItemStarted.has(rkey)) {
                    reasoningItemStarted.delete(rkey);
                    yield* offerRuntimeEvent({
                      eventId: yield* nextEventId,
                      createdAt: yield* nowIso,
                      provider: PROVIDER,
                      threadId: context.threadId,
                      turnId,
                      itemId: rid,
                      type: "item.completed",
                      payload: {
                        itemType: "reasoning",
                        status: su.state === "DONE" ? "completed" : "failed",
                      },
                    });
                  }
                }
              }
              // tool lifecycle
              if (su.step_type === "tool" && su.tool_name) {
                const toolItemId = RuntimeItemId.make(`agy-tool-${turnId}-${su.step_index ?? 0}`);
                const itemType = getAgyToolItemType(su.tool_name);
                // Approval gate for privileged tools when not bypassed
                const needsApproval =
                  isPrivilegedTool(su.tool_name) && context.runtimeMode !== "full-access";
                if (su.state === "ACTIVE") {
                  // If privileged and not skipped, surface request.opened and wait
                  if (needsApproval) {
                    const requestId = ApprovalRequestId.make(
                      `agy-${turnId}-${su.step_index ?? 0}-${DateTime.toEpochMillis(yield* DateTime.now)}-${(eventCounter += 1)}`,
                    );
                    const runtimeRequestId = RuntimeRequestId.make(requestId);
                    const requestType = canonicalRequestTypeForTool(su.tool_name);
                    const detail = summarizeAgyToolRequest(
                      su.tool_name,
                      su.tool_info?.parameters ?? {},
                    );
                    const decisionDeferred = yield* Deferred.make<ProviderApprovalDecision>();
                    const pending: PendingApproval = {
                      requestType,
                      detail,
                      decision: decisionDeferred,
                      toolName: su.tool_name,
                      toolItemId: toolItemId as unknown as string,
                    };
                    context.pendingApprovals.set(requestId, pending);
                    yield* offerRuntimeEvent({
                      eventId: yield* nextEventId,
                      createdAt: yield* nowIso,
                      provider: PROVIDER,
                      threadId: context.threadId,
                      turnId,
                      itemId: toolItemId,
                      requestId: runtimeRequestId,
                      type: "request.opened",
                      payload: {
                        requestType,
                        detail,
                        args: {
                          toolName: su.tool_name,
                          input: su.tool_info?.parameters,
                        },
                      },
                    });
                    // Wait for user decision (or cancel on interrupt)
                    const decision = yield* Deferred.await(decisionDeferred);
                    context.pendingApprovals.delete(requestId);
                    yield* offerRuntimeEvent({
                      eventId: yield* nextEventId,
                      createdAt: yield* nowIso,
                      provider: PROVIDER,
                      threadId: context.threadId,
                      turnId,
                      itemId: toolItemId,
                      requestId: runtimeRequestId,
                      type: "request.resolved",
                      payload: {
                        requestType,
                        decision,
                      },
                    });
                    if (decision === "decline" || decision === "cancel") {
                      // Declined: emit item completed as declined and skip further tool handling
                      yield* offerRuntimeEvent({
                        eventId: yield* nextEventId,
                        createdAt: yield* nowIso,
                        provider: PROVIDER,
                        threadId: context.threadId,
                        turnId,
                        itemId: toolItemId,
                        type: "item.completed",
                        payload: {
                          itemType,
                          status: "failed",
                          detail: `User ${decision === "cancel" ? "cancelled" : "declined"} tool execution: ${su.tool_name}`,
                        },
                      });
                      // If declined, we should continue consuming but not emit started
                      return;
                    }
                    // accepted: fall through to emit started
                  }
                  if (!toolItemStarted.has(toolItemId as unknown as string)) {
                    toolItemStarted.add(toolItemId as unknown as string);
                    const startedParams = su.tool_info?.parameters ?? {};
                    const startedDetail = truncateDetail(
                      summarizeAgyToolRequest(su.tool_name, startedParams),
                      400,
                    );
                    const startedCommand = extractAgyCommand(startedParams);
                    const startedChangedFiles = extractAgyChangedFiles(startedParams);
                    const startedData: Record<string, unknown> = {
                      toolName: su.tool_name,
                      input: startedParams,
                    };
                    if (startedCommand) startedData.command = startedCommand;
                    if (startedChangedFiles) startedData.changedFiles = startedChangedFiles;
                    yield* offerRuntimeEvent({
                      eventId: yield* nextEventId,
                      createdAt: yield* nowIso,
                      provider: PROVIDER,
                      threadId: context.threadId,
                      turnId,
                      itemId: toolItemId,
                      type: "item.started",
                      payload: {
                        itemType,
                        title: su.tool_name,
                        detail: startedDetail,
                        data: startedData,
                      },
                    });
                  }
                } else if (su.state === "DONE") {
                  const doneRawDetail = su.tool_info?.output ?? jsonStringify(su.tool_info ?? {});
                  const doneDetail = truncateDetail(doneRawDetail, 3000);
                  const doneCommand = extractAgyCommand(su.tool_info?.parameters);
                  const durationMs =
                    typeof su.duration_seconds === "number" &&
                    Number.isFinite(su.duration_seconds) &&
                    su.duration_seconds >= 0
                      ? Math.round(su.duration_seconds * 1000)
                      : undefined;
                  const doneData: Record<string, unknown> =
                    su.tool_info?.output !== undefined
                      ? {
                          toolName: su.tool_name,
                          input: su.tool_info?.parameters,
                          output: su.tool_info?.output,
                        }
                      : {
                          toolName: su.tool_name,
                          input: su.tool_info?.parameters,
                          result: su.tool_info,
                        };
                  if (doneCommand) doneData.command = doneCommand;
                  if (durationMs !== undefined) doneData.durationMs = durationMs;
                  yield* offerRuntimeEvent({
                    eventId: yield* nextEventId,
                    createdAt: yield* nowIso,
                    provider: PROVIDER,
                    threadId: context.threadId,
                    turnId,
                    itemId: toolItemId,
                    type: "item.completed",
                    payload: {
                      itemType,
                      status: "completed",
                      detail: doneDetail,
                      data: doneData,
                    },
                  });
                  if (su.usage) {
                    const u = su.usage as Record<string, unknown>;
                    const it = u.input_tokens as number | undefined;
                    const ot = u.output_tokens as number | undefined;
                    const tt = u.thinking_tokens as number | undefined;
                    const tot = u.total_tokens as number | undefined;
                    const crt = (u.cache_read_tokens ?? u.cachedInputTokens) as number | undefined;
                    const used =
                      tot ??
                      (it !== undefined && ot !== undefined
                        ? it + ot + (tt ?? 0)
                        : (it ?? ot ?? tt));
                    if (used !== undefined && Number.isFinite(used as number)) {
                      const normalized: Record<string, unknown> = {
                        usedTokens: Math.floor(used as number),
                      };
                      if (it !== undefined) {
                        normalized.inputTokens = Math.floor(it);
                        normalized.lastInputTokens = Math.floor(it);
                      }
                      if (crt !== undefined) {
                        normalized.cachedInputTokens = Math.floor(crt as number);
                        normalized.lastCachedInputTokens = Math.floor(crt as number);
                      }
                      if (ot !== undefined) {
                        normalized.outputTokens = Math.floor(ot);
                        normalized.lastOutputTokens = Math.floor(ot);
                      }
                      if (tt !== undefined) {
                        normalized.reasoningOutputTokens = Math.floor(tt);
                        normalized.lastReasoningOutputTokens = Math.floor(tt);
                      }
                      if (durationMs !== undefined) normalized.durationMs = durationMs;
                      yield* offerRuntimeEvent({
                        eventId: yield* nextEventId,
                        createdAt: yield* nowIso,
                        provider: PROVIDER,
                        threadId: context.threadId,
                        turnId,
                        type: "thread.token-usage.updated",
                        payload: { usage: normalized } as unknown as {
                          usage: import("@t3tools/contracts").ThreadTokenUsageSnapshot;
                        },
                      }).pipe(Effect.orElseSucceed(() => undefined));
                    }
                  }
                } else if (su.state === "ERROR") {
                  const errMsg = su.tool_info?.error?.message ?? "tool failed";
                  const errorClass = classifyAgyErrorMessage(errMsg);
                  // Also surface as runtime.warning / error for visibility
                  if (errorClass === "transport_error" || errorClass === "provider_error") {
                    yield* offerRuntimeEvent({
                      eventId: yield* nextEventId,
                      createdAt: yield* nowIso,
                      provider: PROVIDER,
                      threadId: context.threadId,
                      turnId,
                      type: "runtime.warning",
                      payload: { message: errMsg },
                    }).pipe(Effect.orElseSucceed(() => undefined));
                  }
                  yield* offerRuntimeEvent({
                    eventId: yield* nextEventId,
                    createdAt: yield* nowIso,
                    provider: PROVIDER,
                    threadId: context.threadId,
                    turnId,
                    itemId: toolItemId,
                    type: "item.completed",
                    payload: {
                      itemType,
                      status: "failed",
                      detail: truncateDetail(errMsg, 3000),
                      data: {
                        toolName: su.tool_name,
                        input: su.tool_info?.parameters,
                        error: errMsg,
                      },
                    },
                  });
                }
              }
              // 通用单步 usage（非工具路径，如 agent_response 带 usage）
              if (su.usage && su.step_type !== "tool") {
                const u = su.usage as Record<string, unknown>;
                const it = u.input_tokens as number | undefined;
                const ot = u.output_tokens as number | undefined;
                const tt = u.thinking_tokens as number | undefined;
                const tot = u.total_tokens as number | undefined;
                const crt = (u.cache_read_tokens ?? u.cachedInputTokens) as number | undefined;
                const used =
                  tot ??
                  (it !== undefined && ot !== undefined ? it + ot + (tt ?? 0) : (it ?? ot ?? tt));
                if (used !== undefined && Number.isFinite(used)) {
                  const normalized: Record<string, unknown> = {
                    usedTokens: Math.floor(used as number),
                  };
                  if (it !== undefined) {
                    normalized.inputTokens = Math.floor(it);
                    normalized.lastInputTokens = Math.floor(it);
                  }
                  if (crt !== undefined) {
                    normalized.cachedInputTokens = Math.floor(crt as number);
                    normalized.lastCachedInputTokens = Math.floor(crt as number);
                  }
                  if (ot !== undefined) {
                    normalized.outputTokens = Math.floor(ot);
                    normalized.lastOutputTokens = Math.floor(ot);
                  }
                  if (tt !== undefined) {
                    normalized.reasoningOutputTokens = Math.floor(tt);
                    normalized.lastReasoningOutputTokens = Math.floor(tt);
                  }
                  const dur =
                    typeof su.duration_seconds === "number" && Number.isFinite(su.duration_seconds)
                      ? Math.round(su.duration_seconds * 1000)
                      : undefined;
                  if (dur !== undefined) normalized.durationMs = dur;
                  yield* offerRuntimeEvent({
                    eventId: yield* nextEventId,
                    createdAt: yield* nowIso,
                    provider: PROVIDER,
                    threadId: context.threadId,
                    turnId,
                    type: "thread.token-usage.updated",
                    payload: { usage: normalized } as unknown as {
                      usage: import("@t3tools/contracts").ThreadTokenUsageSnapshot;
                    },
                  }).pipe(Effect.orElseSucceed(() => undefined));
                }
              }
              return;
            }
            if (frame.event === "result" && frame.result !== undefined) {
              resultFrame = frame.result;
              if (frame.result.conversation_id && !context.conversationId) {
                context.conversationId = frame.result.conversation_id;
              }
            }
          }),
        ),
        Effect.mapError(
          (cause) =>
            new ProviderAdapterProcessError({
              provider: PROVIDER,
              threadId: context.threadId,
              detail: "Failed while reading Antigravity CLI output.",
              cause,
            }),
        ),
      );

      if (!resultFrame) {
        return yield* new ProviderAdapterProcessError({
          provider: PROVIDER,
          threadId: context.threadId,
          detail: "Antigravity CLI closed stdout before emitting a result.",
        });
      }
      // Handle error status from result
      if (resultFrame.status === "error" || resultFrame.status === "failed") {
        const errMsg =
          resultFrame.error ?? resultFrame.response ?? "Antigravity CLI reported an error";
        const errorClass = classifyAgyErrorMessage(errMsg);
        if (errorClass === "permission_error") {
          return yield* new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "turn/result",
            detail: errMsg,
          });
        }
        if (errorClass === "validation_error") {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "sendTurn",
            issue: errMsg,
          });
        }
        // provider/transport → runtime.error
        yield* offerRuntimeEvent({
          eventId: yield* nextEventId,
          createdAt: yield* nowIso,
          provider: PROVIDER,
          threadId: context.threadId,
          turnId,
          type: "runtime.error",
          payload: { message: errMsg, class: errorClass },
        }).pipe(Effect.orElseSucceed(() => undefined));
        if (resultFrame.status === "failed") {
          return yield* new ProviderAdapterProcessError({
            provider: PROVIDER,
            threadId: context.threadId,
            detail: errMsg,
          });
        }
      }
      if (
        resultFrame.error &&
        resultFrame.error.trim().length > 0 &&
        resultFrame.status !== "error" &&
        resultFrame.status !== "failed"
      ) {
        const errorClass = classifyAgyErrorMessage(resultFrame.error);
        yield* offerRuntimeEvent({
          eventId: yield* nextEventId,
          createdAt: yield* nowIso,
          provider: PROVIDER,
          threadId: context.threadId,
          turnId,
          type: "runtime.warning" as const,
          payload: { message: resultFrame.error },
        }).pipe(Effect.orElseSucceed(() => undefined));
      }
      // token usage (B5)
      const usage = (
        resultFrame as unknown as {
          usage?: {
            input_tokens?: number;
            output_tokens?: number;
            thinking_tokens?: number;
            total_tokens?: number;
          };
        }
      ).usage;
      if (usage) {
        const usedTokens =
          usage.total_tokens ?? (usage.input_tokens ?? 0) + (usage.output_tokens ?? 0);
        yield* offerRuntimeEvent({
          eventId: yield* nextEventId,
          createdAt: yield* nowIso,
          provider: PROVIDER,
          threadId: context.threadId,
          turnId,
          type: "thread.token-usage.updated",
          payload: {
            usage: {
              usedTokens,
              totalProcessedTokens: usedTokens,
              ...(usage.input_tokens !== undefined
                ? { inputTokens: usage.input_tokens, lastInputTokens: usage.input_tokens }
                : {}),
              ...(usage.output_tokens !== undefined
                ? { outputTokens: usage.output_tokens, lastOutputTokens: usage.output_tokens }
                : {}),
              ...(usage.thinking_tokens !== undefined
                ? {
                    reasoningOutputTokens: usage.thinking_tokens,
                    lastReasoningOutputTokens: usage.thinking_tokens,
                  }
                : {}),
            },
          },
        }).pipe(Effect.orElseSucceed(() => undefined));
      }
      // fallback for providers that don\'t emit text_delta (should not happen after B1)
      if (!hasEmittedDelta && resultFrame.response && resultFrame.response.trim().length > 0) {
        yield* offerRuntimeEvent({
          eventId: yield* nextEventId,
          createdAt: yield* nowIso,
          provider: PROVIDER,
          threadId: context.threadId,
          turnId,
          itemId: RuntimeItemId.make(`agy-item-${turnId}`),
          type: "content.delta",
          payload: {
            streamKind: "assistant_text",
            delta: resultFrame.response,
          },
        });
      } else if (!hasEmittedDelta && (resultFrame as unknown as { output?: string }).output) {
        const out = (resultFrame as unknown as { output: string }).output;
        if (out.trim().length > 0) {
          yield* offerRuntimeEvent({
            eventId: yield* nextEventId,
            createdAt: yield* nowIso,
            provider: PROVIDER,
            threadId: context.threadId,
            turnId,
            itemId: RuntimeItemId.make(`agy-item-${turnId}`),
            type: "content.delta",
            payload: {
              streamKind: "assistant_text",
              delta: out,
            },
          });
        }
      }
      return resultFrame;
    });

  const startSession: AntigravityAdapterShape["startSession"] = (input) =>
    Effect.gen(function* () {
      const existing = sessions.get(input.threadId);
      if (existing) {
        yield* settlePendingApprovalsAsCancelled(existing.pendingApprovals).pipe(
          Effect.orElseSucceed(() => undefined),
        );
        existing.pendingApprovals.clear();
        yield* killContext(existing).pipe(Effect.orElseSucceed(() => undefined));
        yield* Scope.close(existing.scope, Exit.succeed(undefined)).pipe(
          Effect.orElseSucceed(() => undefined),
        );
        sessions.delete(input.threadId);
      }
      const modelSelection =
        input.modelSelection?.instanceId === options?.instanceId ? input.modelSelection : undefined;
      const rawModel = modelSelection?.model ?? undefined;
      const rawEffort = getModelSelectionStringOptionValue(modelSelection, "effort");
      const effort = normalizeAgyEffort(rawEffort);
      const resumeCursor = parseAgyResumeCursor(input.resumeCursor);
      const conversationId = resumeCursor?.conversationId;

      // The child handle must outlive the caller\'s per-request scope, so it is
      // spawned into an adapter-owned scope and torn down through the explicit
      // kill paths: stopSession / interruptTurn / stopAll.
      const sessionScope = yield* Scope.make("sequential");
      const child = yield* spawnSessionProcess(
        rawModel,
        effort,
        conversationId,
        input.runtimeMode,
        sessionScope,
      );
      const now = yield* nowIso;
      const resumeToPersist = conversationId ? makeResumeCursor(conversationId) : undefined;
      const session: ProviderSession = {
        provider: PROVIDER,
        providerInstanceId: options?.instanceId
          ? (options.instanceId as ProviderSession["providerInstanceId"])
          : undefined,
        status: "ready",
        runtimeMode: input.runtimeMode,
        cwd: input.cwd,
        ...(rawModel ? { model: rawModel } : {}),
        threadId: input.threadId,
        ...(resumeToPersist ? { resumeCursor: resumeToPersist } : {}),
        createdAt: now,
        updatedAt: now,
      };
      const context: AgySessionContext = {
        threadId: input.threadId,
        conversationId,
        child,
        scope: sessionScope,
        model: rawModel,
        effort,
        runtimeMode: input.runtimeMode,
        pendingApprovals: new Map<string, PendingApproval>(),
        createdAt: now,
      };
      sessions.set(input.threadId, context);

      // Emit session lifecycle events for observability
      yield* offerRuntimeEvent({
        eventId: yield* nextEventId,
        createdAt: now,
        provider: PROVIDER,
        threadId: input.threadId,
        type: "session.started",
        payload: resumeCursor ? { resume: resumeCursor } : {},
      }).pipe(Effect.orElseSucceed(() => undefined));

      yield* offerRuntimeEvent({
        eventId: yield* nextEventId,
        createdAt: yield* nowIso,
        provider: PROVIDER,
        threadId: input.threadId,
        type: "session.configured",
        payload: {
          config: {
            ...(rawModel ? { model: rawModel } : {}),
            ...(effort ? { effort } : {}),
            ...(input.cwd ? { cwd: input.cwd } : {}),
          },
        },
      }).pipe(Effect.orElseSucceed(() => undefined));

      yield* offerRuntimeEvent({
        eventId: yield* nextEventId,
        createdAt: yield* nowIso,
        provider: PROVIDER,
        threadId: input.threadId,
        type: "session.state.changed",
        payload: { state: "ready" },
      }).pipe(Effect.orElseSucceed(() => undefined));

      return session;
    });

  const sendTurn: AntigravityAdapterShape["sendTurn"] = (input) =>
    Effect.gen(function* () {
      const context = sessions.get(input.threadId);
      if (!context) {
        return yield* new ProviderAdapterSessionNotFoundError({
          provider: PROVIDER,
          threadId: input.threadId,
        });
      }
      const prompt = input.input?.trim();
      // Allow empty prompt when attachments present
      if (!prompt && (!input.attachments || input.attachments.length === 0)) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "sendTurn",
          issue: "Antigravity turns require a non-empty text prompt or attachments.",
        });
      }
      // In-session model/effort switch: detect change and respawn preserving conversationId
      const modelSelection =
        input.modelSelection?.instanceId === options?.instanceId ? input.modelSelection : undefined;
      const newModel = modelSelection?.model ?? undefined;
      const rawEffort = getModelSelectionStringOptionValue(modelSelection, "effort");
      const newEffort = normalizeAgyEffort(rawEffort);
      const modelChanged = newModel !== undefined && newModel !== context.model;
      const effortChanged = newEffort !== undefined && newEffort !== context.effort;
      if (modelChanged || effortChanged) {
        const nextModel = newModel ?? context.model;
        const nextEffort = newEffort ?? context.effort;
        // Preserve conversation for continuity, fallback to existing conversationId
        const preserveConversationId = context.conversationId;
        // Kill old process and scope
        yield* settlePendingApprovalsAsCancelled(context.pendingApprovals).pipe(
          Effect.orElseSucceed(() => undefined),
        );
        context.pendingApprovals.clear();
        yield* killContext(context).pipe(Effect.orElseSucceed(() => undefined));
        yield* Scope.close(context.scope, Exit.succeed(undefined)).pipe(
          Effect.orElseSucceed(() => undefined),
        );
        const newScope = yield* Scope.make("sequential");
        const newChild = yield* spawnSessionProcess(
          nextModel,
          nextEffort,
          preserveConversationId,
          context.runtimeMode,
          newScope,
        );
        // Mutate context in place (preserve threadId, pendingApprovals)
        const updatedContext: AgySessionContext = {
          threadId: context.threadId,
          conversationId: preserveConversationId,
          child: newChild,
          scope: newScope,
          model: nextModel,
          effort: nextEffort,
          runtimeMode: context.runtimeMode,
          pendingApprovals: context.pendingApprovals,
          createdAt: context.createdAt,
        };
        sessions.set(input.threadId, updatedContext);
        // Update reference for rest of turn
        (context as unknown as { child: typeof newChild }).child = newChild as never;
        (context as unknown as { scope: typeof newScope }).scope = newScope as never;
        context.model = nextModel;
        context.effort = nextEffort;
        // Emit model rerouted event
        yield* offerRuntimeEvent({
          eventId: yield* nextEventId,
          createdAt: yield* nowIso,
          provider: PROVIDER,
          threadId: input.threadId,
          type: "model.rerouted",
          payload: {
            fromModel: context.model ?? "unknown",
            toModel: nextModel ?? context.model ?? "unknown",
            reason: effortChanged
              ? `effort ${context.effort ?? "none"}→${nextEffort ?? "none"}`
              : "user requested model switch",
          },
        }).pipe(Effect.orElseSucceed(() => undefined));
        yield* offerRuntimeEvent({
          eventId: yield* nextEventId,
          createdAt: yield* nowIso,
          provider: PROVIDER,
          threadId: input.threadId,
          type: "session.state.changed",
          payload: { state: "ready" },
        }).pipe(Effect.orElseSucceed(() => undefined));
      }

      // Resolve attachments to base64 image blocks
      let imageBlocks:
        | Array<{
            readonly type: "image";
            readonly mimeType: string;
            readonly data: string;
            readonly name?: string;
          }>
        | undefined;
      if (input.attachments && input.attachments.length > 0) {
        if (input.attachments.length > 8) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "sendTurn",
            issue: `Too many attachments: ${input.attachments.length} (max 8).`,
          });
        }
        imageBlocks = [];
        for (const att of input.attachments) {
          const resolved = yield* resolveAttachment(att as ChatAttachment);
          imageBlocks.push(resolved);
        }
      }

      const effectivePrompt = prompt ?? "";
      const turnClock = yield* DateTime.now;
      const turnId = TurnId.make(`${input.threadId}:${DateTime.toEpochMillis(turnClock)}`);
      yield* offerRuntimeEvent({
        eventId: yield* nextEventId,
        createdAt: yield* nowIso,
        provider: PROVIDER,
        threadId: input.threadId,
        turnId,
        type: "turn.started",
        payload: {
          ...(context.model ? { model: context.model } : {}),
          ...(context.effort ? { effort: context.effort } : {}),
        },
      });

      // Update session to running
      yield* offerRuntimeEvent({
        eventId: yield* nextEventId,
        createdAt: yield* nowIso,
        provider: PROVIDER,
        threadId: input.threadId,
        turnId,
        type: "session.state.changed",
        payload: { state: "running" },
      }).pipe(Effect.orElseSucceed(() => undefined));

      let turnError: unknown | undefined;
      let result: AgyResultFrame | undefined;
      try {
        // Use the (possibly respawned) context
        const activeContext = sessions.get(input.threadId) ?? context;
        result = yield* runTurnToResult(activeContext, effectivePrompt, turnId, imageBlocks);
      } catch (e) {
        turnError = e;
      }

      if (turnError !== undefined) {
        // Classify and emit
        const msg = (turnError as unknown as { message?: string }).message ?? String(turnError);
        const errorClass = classifyAgyErrorMessage(msg);
        yield* offerRuntimeEvent({
          eventId: yield* nextEventId,
          createdAt: yield* nowIso,
          provider: PROVIDER,
          threadId: input.threadId,
          turnId,
          type: "runtime.error" as const,
          payload: { message: msg, class: errorClass },
        }).pipe(Effect.orElseSucceed(() => undefined));

        // Also emit turn.completed as failed for orchestration
        yield* offerRuntimeEvent({
          eventId: yield* nextEventId,
          createdAt: yield* nowIso,
          provider: PROVIDER,
          threadId: input.threadId,
          turnId,
          type: "turn.completed",
          payload: {
            state: "failed",
            errorMessage: msg,
          },
        }).pipe(Effect.orElseSucceed(() => undefined));

        yield* offerRuntimeEvent({
          eventId: yield* nextEventId,
          createdAt: yield* nowIso,
          provider: PROVIDER,
          threadId: input.threadId,
          turnId,
          type: "session.state.changed",
          payload: { state: "ready" },
        }).pipe(Effect.orElseSucceed(() => undefined));

        return yield* turnError as Effect.Effect<
          never,
          ProviderAdapterProcessError | ProviderAdapterRequestError | ProviderAdapterValidationError
        >;
      }

      if (result?.conversation_id && !context.conversationId) {
        context.conversationId = result.conversation_id;
      }
      // Persist resumeCursor if we learned conversationId
      if (context.conversationId) {
        // best-effort: no session persistence here, but resumeCursor will be
        // returned via turn result and stored by ProviderService layer
      }

      yield* offerRuntimeEvent({
        eventId: yield* nextEventId,
        createdAt: yield* nowIso,
        provider: PROVIDER,
        threadId: input.threadId,
        turnId,
        type: "turn.completed",
        payload: {
          state: "completed",
        },
      });

      yield* offerRuntimeEvent({
        eventId: yield* nextEventId,
        createdAt: yield* nowIso,
        provider: PROVIDER,
        threadId: input.threadId,
        turnId,
        type: "session.state.changed",
        payload: { state: "ready" },
      }).pipe(Effect.orElseSucceed(() => undefined));

      // Include resumeCursor in result if we have conversationId
      const resumeCursor = context.conversationId
        ? makeResumeCursor(context.conversationId)
        : undefined;
      return {
        threadId: input.threadId,
        turnId,
        ...(resumeCursor ? { resumeCursor } : {}),
      } satisfies ProviderTurnStartResult;
    });

  const requireSession = (threadId: ThreadId): AgySessionContext => {
    const context = sessions.get(threadId);
    if (!context) {
      throw new ProviderAdapterSessionNotFoundError({
        provider: PROVIDER,
        threadId,
      });
    }
    return context;
  };

  const killContext = (context: AgySessionContext) =>
    context.child.kill().pipe(
      Effect.mapError(
        (cause) =>
          new ProviderAdapterProcessError({
            provider: PROVIDER,
            threadId: context.threadId,
            detail: "Failed to kill the Antigravity CLI process.",
            cause,
          }),
      ),
    );

  const interruptTurn: AntigravityAdapterShape["interruptTurn"] = (threadId, turnId) =>
    Effect.gen(function* () {
      const context = sessions.get(threadId);
      if (!context) {
        return yield* new ProviderAdapterSessionNotFoundError({
          provider: PROVIDER,
          threadId,
        });
      }
      // Graceful: try stdin interrupt event with timeout, then fallback to kill + respawn
      const interruptFrame = `${globalThis.JSON.stringify({ event: "interrupt" })}\n`;
      const activeTurnId = turnId ?? TurnId.make(`${threadId}:interrupt`);
      yield* offerRuntimeEvent({
        eventId: yield* nextEventId,
        createdAt: yield* nowIso,
        provider: PROVIDER,
        threadId,
        turnId: activeTurnId,
        type: "turn.aborted",
        payload: { reason: "User interrupted the turn." },
      }).pipe(Effect.orElseSucceed(() => undefined));

      // Try graceful interrupt via stdin
      const graceful = Stream.run(
        Stream.encodeText(Stream.make(interruptFrame)),
        context.child.stdin,
      ).pipe(Effect.timeout(500), Effect.ignore);

      yield* graceful;

      // Give agy 600ms to settle and emit result, otherwise kill
      yield* Effect.sleep(Duration.millis(600)).pipe(Effect.orElseSucceed(() => undefined));

      // Cancel pending approvals
      yield* settlePendingApprovalsAsCancelled(context.pendingApprovals).pipe(
        Effect.orElseSucceed(() => undefined),
      );
      context.pendingApprovals.clear();

      // Check if still alive: attempt kill as fallback and respawn to keep conversation
      const stillExists = sessions.has(threadId);
      if (stillExists) {
        yield* killContext(context).pipe(Effect.orElseSucceed(() => undefined));
        yield* Scope.close(context.scope, Exit.succeed(undefined)).pipe(
          Effect.orElseSucceed(() => undefined),
        );
        // Respawn preserving conversationId so session remains usable
        const preservedConversationId = context.conversationId;
        const newScope = yield* Scope.make("sequential");
        const newChild = yield* spawnSessionProcess(
          context.model,
          context.effort,
          preservedConversationId,
          context.runtimeMode,
          newScope,
        ).pipe(
          Effect.orElseSucceed(
            () => undefined as unknown as ChildProcessSpawner.ChildProcessHandle,
          ),
        );
        if (newChild) {
          const newContext: AgySessionContext = {
            threadId,
            conversationId: preservedConversationId,
            child: newChild as ChildProcessSpawner.ChildProcessHandle,
            scope: newScope,
            model: context.model,
            effort: context.effort,
            runtimeMode: context.runtimeMode,
            pendingApprovals: new Map<string, PendingApproval>(),
            createdAt: context.createdAt,
          };
          sessions.set(threadId, newContext);
          yield* offerRuntimeEvent({
            eventId: yield* nextEventId,
            createdAt: yield* nowIso,
            provider: PROVIDER,
            threadId,
            type: "session.state.changed",
            payload: { state: "ready" },
          }).pipe(Effect.orElseSucceed(() => undefined));
        } else {
          sessions.delete(threadId);
          yield* offerRuntimeEvent({
            eventId: yield* nextEventId,
            createdAt: yield* nowIso,
            provider: PROVIDER,
            threadId,
            type: "session.exited",
            payload: { reason: "Interrupted and respawn failed" },
          }).pipe(Effect.orElseSucceed(() => undefined));
        }
      }

      // Emit turn completed as interrupted if not already
      yield* offerRuntimeEvent({
        eventId: yield* nextEventId,
        createdAt: yield* nowIso,
        provider: PROVIDER,
        threadId,
        turnId: activeTurnId,
        type: "turn.completed",
        payload: { state: "interrupted" },
      }).pipe(Effect.orElseSucceed(() => undefined));
    });

  const respondToRequest: AntigravityAdapterShape["respondToRequest"] = (
    threadId,
    requestId,
    decision,
  ) =>
    Effect.gen(function* () {
      const context = sessions.get(threadId);
      if (!context) {
        return yield* new ProviderAdapterSessionNotFoundError({
          provider: PROVIDER,
          threadId,
        });
      }
      const key = requestId as unknown as string;
      const pending = context.pendingApprovals.get(key);
      if (!pending) {
        // Also try lookup by string value without brand
        const alt = Array.from(context.pendingApprovals.entries()).find(
          ([k]) => k === String(requestId),
        );
        if (!alt) {
          return yield* new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "respondToRequest",
            detail: `Unknown pending approval request: ${requestId}`,
          });
        }
        const [, p] = alt;
        context.pendingApprovals.delete(alt[0]);
        yield* Deferred.succeed(p.decision, decision);
        return;
      }
      context.pendingApprovals.delete(key);
      yield* Deferred.succeed(pending.decision, decision);
    });

  const respondToUserInput: AntigravityAdapterShape["respondToUserInput"] = () =>
    Effect.fail(
      new ProviderAdapterRequestError({
        provider: PROVIDER,
        method: "respondToUserInput",
        detail:
          "Antigravity does not surface structured user-input requests; use respondToRequest for tool approvals.",
      }),
    );

  const stopSession: AntigravityAdapterShape["stopSession"] = (threadId) =>
    Effect.gen(function* () {
      const context = sessions.get(threadId);
      if (!context) return;
      yield* settlePendingApprovalsAsCancelled(context.pendingApprovals).pipe(
        Effect.orElseSucceed(() => undefined),
      );
      context.pendingApprovals.clear();
      yield* killContext(context).pipe(Effect.orElseSucceed(() => undefined));
      yield* Scope.close(context.scope, Exit.succeed(undefined)).pipe(
        Effect.orElseSucceed(() => undefined),
      );
      sessions.delete(threadId);
      yield* offerRuntimeEvent({
        eventId: yield* nextEventId,
        createdAt: yield* nowIso,
        provider: PROVIDER,
        threadId,
        type: "session.exited",
        payload: {},
      }).pipe(Effect.orElseSucceed(() => undefined));
    });

  const listSessions: AntigravityAdapterShape["listSessions"] = () =>
    Effect.gen(function* () {
      const list: ProviderSession[] = [];
      const now = yield* nowIso;
      for (const context of sessions.values()) {
        list.push({
          provider: PROVIDER,
          status: "running",
          runtimeMode: context.runtimeMode as ProviderSession["runtimeMode"],
          ...(context.model ? { model: context.model } : {}),
          threadId: context.threadId,
          createdAt: context.createdAt ?? now,
          updatedAt: now,
          ...(context.conversationId
            ? { resumeCursor: makeResumeCursor(context.conversationId) }
            : {}),
        } as ProviderSession);
      }
      return list;
    });

  const hasSession: AntigravityAdapterShape["hasSession"] = (threadId) =>
    Effect.sync(() => sessions.has(threadId));

  const readThread: AntigravityAdapterShape["readThread"] = (threadId) =>
    Effect.sync(() => {
      requireSession(threadId);
      return { threadId, turns: [] };
    });

  const rollbackThread: AntigravityAdapterShape["rollbackThread"] = (threadId, _numTurns) =>
    Effect.sync(() => {
      requireSession(threadId);
      return { threadId, turns: [] };
    });

  const stopAll = Effect.forEach(Array.from(sessions.values()), (context) =>
    Effect.gen(function* () {
      yield* settlePendingApprovalsAsCancelled(context.pendingApprovals).pipe(
        Effect.orElseSucceed(() => undefined),
      );
      context.pendingApprovals.clear();
      yield* context.child.kill().pipe(Effect.orElseSucceed(() => undefined));
      yield* Scope.close(context.scope, Exit.succeed(undefined)).pipe(
        Effect.orElseSucceed(() => undefined),
      );
    }),
  ).pipe(
    Effect.asVoid,
    Effect.tap(() => Effect.sync(() => sessions.clear())),
  );

  yield* Effect.addFinalizer(() => stopAll);

  const adapter: AntigravityAdapterShape = {
    provider: PROVIDER,
    capabilities: {
      sessionModelSwitch: "in-session",
    },
    startSession,
    sendTurn,
    interruptTurn,
    respondToRequest,
    respondToUserInput,
    stopSession,
    listSessions,
    hasSession,
    readThread,
    rollbackThread,
    stopAll: () => stopAll,
    streamEvents: Stream.fromQueue(runtimeEventQueue),
  };
  return adapter;
});
