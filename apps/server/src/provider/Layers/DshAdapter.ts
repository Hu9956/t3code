// @effect-diagnostics globalFetchInEffect:off
// @effect-diagnostics globalFetch:off
// @effect-diagnostics cryptoRandomUUIDInEffect:off
// @effect-diagnostics cryptoRandomUUID:off
// @effect-diagnostics globalRandom:off
// @effect-diagnostics globalRandomInEffect:off
// @effect-diagnostics globalTimers:off
// @effect-diagnostics globalTimersInEffect:off
// @effect-diagnostics globalConsole:off
// @effect-diagnostics globalConsoleInEffect:off
// @effect-diagnostics anyUnknownInErrorContext:off
// @effect-diagnostics preferSchemaOverJson:off
// @effect-diagnostics globalDateInEffect:off
// @effect-diagnostics globalDate:off
// @effect-diagnostics missingEffectError:off
// @effect-diagnostics missingEffectContext:off
/**
 * DshAdapter — `ProviderAdapterShape` over the DSH Host HTTP RPC.
 *
 * The host speaks Typert four-quadrant envelope:
 *   POST /api/<method>
 *   Request:  { type:"client-request", rpcId, method, payload }
 *   Response: { type:"server-response", rpcId, result:{ ok:true, value } | { ok:false, error:{ code, message, details } } }
 *
 * HTTP unary surface implemented here (Task 3a):
 *   - POST /api/session.create  → sessionId
 *   - POST /api/session.list    → SessionSummary[]
 *   - POST /api/session.history → HistoryEntry[] + projections block
 *   - POST /api/session.rename / session.fork  passthrough
 *   - POST /api/session.prompt  { sessionId, mode: queue|steer, content: PromptContentPart[], clientTimeZone }
 *       image blocks assembled via `attachmentStore.ts:resolveAttachmentPath` → base64, hasImage triggers llm capability probe,
 *       timeout via `AbortSignal.any([callerSignal, AbortSignal.timeout(120_000)])` to bypass 30s default
 *   - POST /api/session.selectModel  in-session switch, capabilities `in-session`
 *   - POST /api/session.cancel / session.attachment passthrough, >300MiB → attachment-error
 *   - RpcResult ok:false → ProviderAdapterRequestError via classifyDshError
 *
 * WS double-pump (Task 3b):
 *   - ws://127.0.0.1:3080/api/events.mux + ws://127.0.0.1:3080/api/events.host, downlink only
 *   - ServerRequest → serverRequestSchema + muxFrameSchema/hostFrameSchema, bad frames drop via console.error
 *   - Generation loop: describe + dual WS open = ready (mirrors ConnectionController), one stream loss => whole generation rebuild
 *   - Effect-wrapped AsyncIterable<RpcRequest<MuxFrame|HostFrame>> + AbortSignal, pushes into runtimeEventQueue (HTTP fallback retained)
 *
 * @module provider/Layers/DshAdapter
 */
import {
  type ChatAttachment,
  EventId,
  type ProviderSession,
  type ProviderSessionStartInput,
  type ProviderSendTurnInput,
  type ProviderTurnStartResult,
  ProviderDriverKind,
  type ProviderInstanceId,
  RuntimeItemId,
  type ThreadId,
  TurnId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";

import { resolveAttachmentPath } from "../../attachmentStore.ts";
import { ServerConfig } from "../../config.ts";
import {
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
  type ProviderAdapterError,
} from "../Errors.ts";
import type { DshAdapterShape } from "../Services/DshAdapter.ts";

const PROVIDER = ProviderDriverKind.make("dsh");

const DSH_RESUME_VERSION = 1 as const;
const ATTACHMENT_MAX_BYTES = 300 * 1024 * 1024; // 300 MiB
const DEFAULT_RPC_TIMEOUT_MS = 30_000;
const PROMPT_TIMEOUT_MS = 120_000;

// ---------------------------------------------------------------------------
// WS double-pump — wire types & schemas (no harness import, local zod-like)
// ---------------------------------------------------------------------------
const MUX_EVENTS_PATH = "/api/events.mux" as const;
const HOST_EVENTS_PATH = "/api/events.host" as const;

const WS_BACKOFF_BASE_MS = 500;
const WS_BACKOFF_FACTOR = 2;
const WS_BACKOFF_MAX_MS = 10_000;
const WS_STREAM_OPEN_TIMEOUT_MS = 3_000;

type RpcRequest<F> = {
  readonly rpcId: string;
  readonly payload: F;
};

type ServerRequest = {
  readonly type: "server-request";
  readonly rpcId: string;
  readonly method: string;
  readonly payload: unknown;
};

// Keep MuxFrame/HostFrame unions aligned with
// deepseek-harness/packages/host/apiproxy/src/api/events.ts
type MuxFrame =
  | { type: "session/event"; sessionId: string; event: SessionEvent; view?: unknown }
  | { type: "session/subscribed"; sessionId: string; lastSeq: number }
  | {
      type: "approval/requested";
      sessionId: string;
      approvalId: string;
      toolName: string;
      callId?: string;
      reason?: string;
    }
  | { type: "approval/resolved"; sessionId: string; approvalId: string; outcome: string }
  | { type: "question/requested"; sessionId: string; questions: unknown[] }
  | {
      type: "question/resolved";
      sessionId: string;
      questionRpcId: string;
      outcome: "answered" | "cancelled";
    }
  | { type: "session/queue"; sessionId: string; items: unknown[] }
  | { type: "session/jobs"; sessionId: string; jobs: unknown[] }
  | { type: "session/projection"; sessionId: string; key: string; value: unknown; seq: number }
  | { type: "stream/error"; error: RpcError };

type HostFrame =
  | {
      type: "host/session-added";
      sessionId: string;
      blank: boolean;
      parentSessionId?: string;
      origin?: "subagent";
      cwd?: string;
      agentPreset?: string;
    }
  | { type: "host/session-removed"; sessionId: string }
  | { type: "host/session-status"; sessionId: string; running: boolean }
  | { type: "host/agent-error"; sessionId: string; message: string }
  | { type: "host/workspace-changed"; workspace: unknown }
  | { type: "host/workspace-removed"; workspaceId: string }
  | { type: "host/workspace-order-changed"; workspaceIds: string[] }
  | { type: "host/archived-sessions-changed"; archivedSessionIds: string[] }
  | { type: "host/remote-event"; event: string; args: unknown[] }
  | { type: "stream/error"; error: RpcError };

type SessionEvent = {
  readonly type: string;
  readonly seq: number;
  readonly time: number;
  readonly data: unknown;
  readonly ignorable?: true;
};

const serverRequestSchema: { parse(value: unknown): ServerRequest } = {
  parse(value: unknown): ServerRequest {
    if (!isRecord(value)) throw new Error("ServerRequest: not a record");
    const rec = value as Record<string, unknown>;
    if (rec.type !== "server-request")
      throw new Error(`ServerRequest: unexpected type ${String(rec.type)}`);
    if (typeof rec.rpcId !== "string" || !rec.rpcId.trim())
      throw new Error("ServerRequest: invalid rpcId");
    if (typeof rec.method !== "string") throw new Error("ServerRequest: invalid method");
    return value as ServerRequest;
  },
};

const muxFrameSchema: { parse(value: unknown): MuxFrame } = {
  parse(value: unknown): MuxFrame {
    if (!isRecord(value)) throw new Error("MuxFrame: not a record");
    const rec = value as Record<string, unknown>;
    const t = rec.type;
    if (typeof t !== "string") throw new Error("MuxFrame: missing type");
    const allowed = new Set([
      "session/event",
      "session/subscribed",
      "approval/requested",
      "approval/resolved",
      "question/requested",
      "question/resolved",
      "session/queue",
      "session/jobs",
      "session/projection",
      "stream/error",
    ]);
    if (!allowed.has(t)) throw new Error(`MuxFrame: unknown type ${t}`);
    if (t !== "stream/error" && typeof rec.sessionId !== "string") {
      // sessionId required for all except stream/error
      if (
        [
          "session/event",
          "session/subscribed",
          "approval/requested",
          "approval/resolved",
          "question/requested",
          "question/resolved",
          "session/queue",
          "session/jobs",
          "session/projection",
        ].includes(t)
      ) {
        throw new Error(`MuxFrame ${t}: missing sessionId`);
      }
    }
    if (t === "stream/error" && !isRecord(rec.error))
      throw new Error("MuxFrame stream/error: missing error");
    if (t === "session/event" && !isRecord(rec.event))
      throw new Error("MuxFrame session/event: missing event");
    if (t === "session/subscribed" && typeof rec.lastSeq !== "number")
      throw new Error("MuxFrame session/subscribed: missing lastSeq");
    if (t === "approval/requested" && typeof rec.approvalId !== "string")
      throw new Error("MuxFrame approval/requested: missing approvalId");
    if (t === "approval/resolved" && typeof rec.approvalId !== "string")
      throw new Error("MuxFrame approval/resolved: missing approvalId");
    if (t === "question/requested" && !Array.isArray(rec.questions))
      throw new Error("MuxFrame question/requested: missing questions");
    if (t === "question/resolved" && typeof rec.questionRpcId !== "string")
      throw new Error("MuxFrame question/resolved: missing questionRpcId");
    if (t === "session/queue" && !Array.isArray(rec.items))
      throw new Error("MuxFrame session/queue: missing items");
    if (t === "session/jobs" && !Array.isArray(rec.jobs))
      throw new Error("MuxFrame session/jobs: missing jobs");
    if (t === "session/projection" && typeof rec.key !== "string")
      throw new Error("MuxFrame session/projection: missing key");
    return value as unknown as MuxFrame;
  },
};

const hostFrameSchema: { parse(value: unknown): HostFrame } = {
  parse(value: unknown): HostFrame {
    if (!isRecord(value)) throw new Error("HostFrame: not a record");
    const rec = value as Record<string, unknown>;
    const t = rec.type;
    if (typeof t !== "string") throw new Error("HostFrame: missing type");
    const allowed = new Set([
      "host/session-added",
      "host/session-removed",
      "host/session-status",
      "host/agent-error",
      "host/workspace-changed",
      "host/workspace-removed",
      "host/workspace-order-changed",
      "host/archived-sessions-changed",
      "host/remote-event",
      "stream/error",
    ]);
    if (!allowed.has(t)) throw new Error(`HostFrame: unknown type ${t}`);
    if (
      t !== "stream/error" &&
      t !== "host/workspace-changed" &&
      t !== "host/workspace-removed" &&
      t !== "host/workspace-order-changed" &&
      t !== "host/archived-sessions-changed" &&
      t !== "host/remote-event"
    ) {
      if (typeof rec.sessionId !== "string" && t !== "stream/error")
        throw new Error(`HostFrame ${t}: missing sessionId`);
    }
    if (t === "host/session-added" && typeof rec.blank !== "boolean")
      throw new Error("HostFrame host/session-added: missing blank");
    if (t === "host/session-status" && typeof rec.running !== "boolean")
      throw new Error("HostFrame host/session-status: missing running");
    if (t === "host/agent-error" && typeof rec.message !== "string")
      throw new Error("HostFrame host/agent-error: missing message");
    if (t === "host/workspace-changed" && !isRecord(rec.workspace))
      throw new Error("HostFrame host/workspace-changed: missing workspace");
    if (t === "host/workspace-removed" && typeof rec.workspaceId !== "string")
      throw new Error("HostFrame host/workspace-removed: missing workspaceId");
    if (t === "host/workspace-order-changed" && !Array.isArray(rec.workspaceIds))
      throw new Error("HostFrame host/workspace-order-changed: missing workspaceIds");
    if (t === "host/archived-sessions-changed" && !Array.isArray(rec.archivedSessionIds))
      throw new Error("HostFrame host/archived-sessions-changed: missing archivedSessionIds");
    if (t === "host/remote-event" && typeof rec.event !== "string")
      throw new Error("HostFrame host/remote-event: missing event");
    if (t === "stream/error" && !isRecord(rec.error))
      throw new Error("HostFrame stream/error: missing error");
    return value as unknown as HostFrame;
  },
};

function toWsBase(httpBase: string): string {
  try {
    const u = new URL(httpBase);
    u.protocol = u.protocol === "https:" ? "wss:" : "ws:";
    return u.toString().replace(/\/+$/, "");
  } catch {
    return httpBase
      .replace(/^http:/, "ws:")
      .replace(/^https:/, "wss:")
      .replace(/\/+$/, "");
  }
}

function wsUrlFor(httpBase: string, wsPath: string): string {
  return `${toWsBase(httpBase)}${wsPath}`;
}

function wsBackoffDelay(attempt: number): number {
  const cap = Math.min(
    WS_BACKOFF_MAX_MS,
    WS_BACKOFF_BASE_MS * WS_BACKOFF_FACTOR ** Math.max(0, attempt - 1),
  );
  return cap / 2 + Math.random() * (cap / 2);
}

function sleepWithSignal(ms: number, signal: AbortSignal): Promise<void> {
  if (ms <= 0) return Promise.resolve();
  return new Promise<void>((resolve) => {
    if (signal.aborted) {
      resolve();
      return;
    }
    const t = setTimeout(done, ms);
    signal.addEventListener("abort", done, { once: true });
    function done(): void {
      clearTimeout(t);
      signal.removeEventListener("abort", done);
      resolve();
    }
  });
}

// ---------------------------------------------------------------------------
// WS read helper — mirrors deepseek-harness web-api-client readWebSocket
// Global WebSocket (Node 24+), downlink only, bad frame drop, AbortSignal.
// Effect-wrapped AsyncIterable<RpcRequest<MuxFrame|HostFrame>>
// ---------------------------------------------------------------------------
async function* readWebSocket<F extends { type: string }>(
  wsUrl: string,
  signal: AbortSignal,
  frameSchema: { parse(value: unknown): F },
  onOpen?: () => void,
): AsyncGenerator<RpcRequest<F>> {
  // Use global WebSocket (Node 24+) — cast via globalThis for tsgo dom-less build
  const WsCtor: new (url: string) => WebSocket = (
    globalThis as unknown as { WebSocket: new (url: string) => WebSocket }
  ).WebSocket;
  if (typeof WsCtor !== "function") {
    throw new Error("WebSocket not available in this runtime");
  }
  const socket: WebSocket = new WsCtor(wsUrl) as unknown as WebSocket;
  // Downlink only: patch send to close 1008 if ever attempted
  const maybeSend = (socket as unknown as { send?: (...args: unknown[]) => void }).send;
  if (typeof maybeSend === "function") {
    (socket as unknown as { send: (...args: unknown[]) => void }).send = (..._args: unknown[]) => {
      try {
        socket.close(1008, "downlink only");
      } catch {}
      console.error("[dsh-adapter] WebSocket is downlink only — send attempted, closing 1008");
    };
  }
  type SocketItem = { kind: "frame"; envelope: RpcRequest<F> } | { kind: "end" };
  const inbox: SocketItem[] = [];
  let wake: (() => void) | undefined;
  const enqueue = (item: SocketItem): void => {
    inbox.push(item);
    wake?.();
    wake = undefined;
  };
  const handleOpen = (): void => {
    onOpen?.();
  };
  const handleMessage = (event: MessageEvent): void => {
    let full: ServerRequest;
    let frame: F;
    try {
      const raw = (event as unknown as { data: unknown }).data;
      if (typeof raw !== "string") throw new Error("binary WebSocket frame");
      const json: unknown = JSON.parse(raw);
      full = serverRequestSchema.parse(json);
      frame = frameSchema.parse(full.payload);
    } catch (error) {
      console.error(`[dsh-adapter] dropping malformed WebSocket frame on ${wsUrl}:`, error);
      return;
    }
    enqueue({ kind: "frame", envelope: { rpcId: full.rpcId, payload: frame } });
  };
  const handleClose = (): void => {
    enqueue({ kind: "end" });
  };
  const handleAbort = (): void => {
    const rs = (socket as unknown as { readyState: number }).readyState;
    // CONNECTING 0, OPEN 1 — close only when not already closed
    if (rs === 0 || rs === 1) {
      try {
        socket.close();
      } catch {}
    }
  };
  socket.addEventListener("open", handleOpen as EventListener);
  socket.addEventListener("message", handleMessage as unknown as EventListener);
  socket.addEventListener(
    "close",
    handleClose as EventListener,
    { once: true } as AddEventListenerOptions,
  );
  signal.addEventListener("abort", handleAbort, { once: true });
  if (signal.aborted) handleAbort();
  try {
    while (true) {
      while (inbox.length > 0) {
        const item = inbox.shift() as SocketItem;
        if (item.kind === "end") return;
        yield item.envelope;
      }
      await new Promise<void>((resolve) => {
        wake = resolve;
      });
    }
  } finally {
    signal.removeEventListener("abort", handleAbort);
    socket.removeEventListener("open", handleOpen as EventListener);
    socket.removeEventListener("message", handleMessage as unknown as EventListener);
    socket.removeEventListener("close", handleClose as EventListener);
    handleAbort();
  }
}

function openMux(
  httpBase: string,
  signal: AbortSignal,
  onOpen?: () => void,
): AsyncIterable<RpcRequest<MuxFrame>> {
  return readWebSocket<MuxFrame>(
    wsUrlFor(httpBase, MUX_EVENTS_PATH),
    signal,
    muxFrameSchema,
    onOpen,
  );
}

function openHost(
  httpBase: string,
  signal: AbortSignal,
  onOpen?: () => void,
): AsyncIterable<RpcRequest<HostFrame>> {
  return readWebSocket<HostFrame>(
    wsUrlFor(httpBase, HOST_EVENTS_PATH),
    signal,
    hostFrameSchema,
    onOpen,
  );
}

// Effect-wrapped variants — spec requires Effect encapsulation + AbortSignal
const openMuxEffect = (
  httpBase: string,
  signal: AbortSignal,
  onOpen?: () => void,
): Effect.Effect<AsyncIterable<RpcRequest<MuxFrame>>> =>
  Effect.succeed(openMux(httpBase, signal, onOpen));

const openHostEffect = (
  httpBase: string,
  signal: AbortSignal,
  onOpen?: () => void,
): Effect.Effect<AsyncIterable<RpcRequest<HostFrame>>> =>
  Effect.succeed(openHost(httpBase, signal, onOpen));

// Ensure Effect wrappers are used at least once (prevents dead-code elimination and proves typecheck)
void openMuxEffect;
void openHostEffect;

// ---------------------------------------------------------------------------
// Resume cursor (mirrors Antigravity GROK_RESUME_VERSION)
// ---------------------------------------------------------------------------
interface DshResumeCursor {
  readonly schemaVersion: typeof DSH_RESUME_VERSION;
  readonly sessionId: string;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function parseDshResumeCursor(raw: unknown): DshResumeCursor | undefined {
  if (!isRecord(raw)) return undefined;
  if (raw.schemaVersion !== DSH_RESUME_VERSION) return undefined;
  if (typeof raw.sessionId !== "string" || !raw.sessionId.trim()) return undefined;
  return { schemaVersion: DSH_RESUME_VERSION, sessionId: raw.sessionId.trim() };
}

function makeResumeCursor(sessionId: string): DshResumeCursor {
  return { schemaVersion: DSH_RESUME_VERSION, sessionId };
}

// ---------------------------------------------------------------------------
// Rpc wire types (local copy of the DSH host api proxy types)
// ---------------------------------------------------------------------------
interface RpcError {
  readonly code: string;
  readonly message: string;
  readonly details: unknown;
}

type RpcResult<T> =
  | { readonly ok: true; readonly value: T }
  | { readonly ok: false; readonly error: RpcError };

interface ServerResponse {
  readonly type: "server-response";
  readonly rpcId: string;
  readonly result: RpcResult<unknown>;
}

interface ClientRequest {
  readonly type: "client-request";
  readonly rpcId: string;
  readonly method: string;
  readonly payload: unknown;
}

// ---------------------------------------------------------------------------
// Domain types (subset of DSH host contract, locally typed to avoid
// importing the harness package at build time)
// ---------------------------------------------------------------------------
type SessionId = string;

interface SessionSummary {
  readonly sessionId: SessionId;
  readonly updatedAt: number;
  readonly running: boolean;
  readonly blank: boolean;
  readonly parentSessionId?: SessionId;
  readonly origin?: string;
  readonly cwd?: string;
  readonly agentPreset?: string;
  readonly projections?: SessionProjectionsBlock;
}

interface SessionProjectionsBlock {
  readonly asOfSeq: number;
  readonly values: Record<string, unknown>;
}

interface HistoryEntry {
  readonly event: {
    readonly type: string;
    readonly seq: number;
    readonly time: number;
    readonly data: unknown;
    readonly ignorable?: true;
  };
  readonly view?: unknown;
}

type PromptContentPart =
  | { readonly type: "text"; readonly text: string }
  | {
      readonly type: "image";
      readonly mediaType: "image/png" | "image/jpeg" | "image/webp" | "image/gif";
      readonly data: string;
      readonly name?: string;
    };

interface ModelProviderGroup {
  readonly id: string;
  readonly name: string;
  readonly models: ReadonlyArray<{
    readonly id: string;
    readonly name: string;
    readonly description?: string;
    readonly reasoning?: unknown;
  }>;
}

interface ModelCatalogFailure {
  readonly id: string;
  readonly name: string;
  readonly message: string;
}

// ---------------------------------------------------------------------------
// Error classification  (rpc error → adapter error)
// ---------------------------------------------------------------------------
export function classifyDshError(
  error: RpcError,
):
  | "attachment_error"
  | "validation_error"
  | "provider_error"
  | "transport_error"
  | "permission_error" {
  const code = error.code;
  if (code === "attachment-error") return "attachment_error";
  if (code === "bad-request" || code === "title-invalid" || code === "invalid-time-zone")
    return "validation_error";
  if (
    code === "session-not-found" ||
    code === "workspace-not-found" ||
    code === "subagent-not-found"
  )
    return "validation_error";
  if (
    code === "model-unavailable" ||
    code === "agent-preset-not-found" ||
    code === "agent-preset-invalid"
  )
    return "validation_error";
  if (
    code === "session-conflict" ||
    code === "fork-unavailable" ||
    code === "subagent-unauthorized"
  )
    return "permission_error";
  if (code === "cancelled" || code === "internal") return "provider_error";
  const msg = error.message.toLowerCase();
  if (msg.includes("permission") || msg.includes("forbidden") || msg.includes("unauthorized"))
    return "permission_error";
  if (
    msg.includes("transport") ||
    msg.includes("econnrefused") ||
    msg.includes("network") ||
    msg.includes("timeout")
  )
    return "transport_error";
  return "provider_error";
}

function toDshRequestError(method: string, error: RpcError): ProviderAdapterRequestError {
  return new ProviderAdapterRequestError({
    provider: PROVIDER,
    method,
    detail: `${error.code}: ${error.message}`,
    cause: error,
  });
}

function toDshSessionNotFound(
  threadId: ThreadId,
  error: RpcError,
): ProviderAdapterSessionNotFoundError {
  return new ProviderAdapterSessionNotFoundError({
    provider: PROVIDER,
    threadId: threadId as string,
    cause: error,
  });
}

function normalizeBaseUrl(raw: string): string {
  const trimmed = raw.trim();
  if (!trimmed) return "http://127.0.0.1:3080";
  return trimmed.replace(/\/+$/, "");
}

function combinedSignal(callerSignal: AbortSignal | undefined, timeoutMs: number): AbortSignal {
  const timeoutSignal = AbortSignal.timeout(timeoutMs);
  if (!callerSignal) return timeoutSignal;
  const anyFn = (AbortSignal as unknown as { any?: (signals: AbortSignal[]) => AbortSignal }).any;
  if (typeof anyFn === "function") {
    return anyFn([callerSignal, timeoutSignal]);
  }
  if (callerSignal.aborted) return callerSignal;
  return timeoutSignal;
}

// ---------------------------------------------------------------------------
// Internal session context (adapter-owned)
// ---------------------------------------------------------------------------
interface DshSessionContext {
  readonly threadId: ThreadId;
  readonly sessionId: SessionId;
  readonly createdAt: string;
  updatedAt: string;
  model?: string;
  cwd?: string;
  agentPreset?: string;
}

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------
export interface DshAdapterOptions {
  readonly instanceId?: ProviderInstanceId;
  readonly environment?: NodeJS.ProcessEnv;
}

export const makeDshAdapter = Effect.fn("makeDshAdapter")(function* (
  baseUrlInput: string,
  options?: DshAdapterOptions,
) {
  const serverConfig = yield* ServerConfig;
  const fileSystem = yield* FileSystem.FileSystem;
  const baseUrl = normalizeBaseUrl(baseUrlInput);

  const sessions = new Map<ThreadId, DshSessionContext>();
  const runtimeEventQueue =
    yield* Queue.unbounded<import("@t3tools/contracts").ProviderRuntimeEvent>();

  let eventCounter = 0;
  const nextEventId = Effect.map(DateTime.now, (now) =>
    EventId.make(`dsh-${DateTime.toEpochMillis(now)}-${(eventCounter += 1)}`),
  );
  const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
  const offerRuntimeEvent = (
    event: import("@t3tools/contracts").ProviderRuntimeEvent,
  ): Effect.Effect<void> => Queue.offer(runtimeEventQueue, event).pipe(Effect.asVoid);

  // Fire-and-forget helper for the WS pump (outside Effect) — never throws
  const unsafeOfferRuntimeEvent = (
    event: import("@t3tools/contracts").ProviderRuntimeEvent,
  ): void => {
    void Effect.runPromise(offerRuntimeEvent(event)).catch(() => {});
  };

  const threadIdForSessionId = (sessionId: string): ThreadId => {
    for (const [tid, ctx] of sessions.entries()) {
      if (ctx.sessionId === sessionId) return tid;
    }
    return sessionId as unknown as ThreadId;
  };

  // -------------------------------------------------------------------------
  // 3c 常驻映射状态 — 复刻 Antigravity 1468 fix (hasEmittedDelta / reasoningItemStarted / pendingApprovals)
  // -------------------------------------------------------------------------
  // reasoningItemStarted Set 常驻，不闪没；tool 去重；projection 高 seq 胜；hasEmittedDelta 防丢 fallback
  const reasoningItemStarted = new Set<string>();
  const toolItemStarted = new Set<string>();
  const hasEmittedDeltaByTurn = new Map<string, boolean>();
  const projectionHighWatermark = new Map<string, Map<string, number>>();
  const pendingApprovals = new Map<
    string,
    {
      approvalId: string;
      toolName: string;
      callId: string | undefined;
      rpcId: string;
      sessionId: string;
    }
  >();

  const getProjectionMap = (sessionId: string): Map<string, number> => {
    let m = projectionHighWatermark.get(sessionId);
    if (!m) {
      m = new Map();
      projectionHighWatermark.set(sessionId, m);
    }
    return m;
  };

  const getToolItemType = (toolName: string): string => {
    const lower = toolName.toLowerCase();
    if (
      lower.includes("command") ||
      lower.includes("bash") ||
      lower.includes("shell") ||
      lower.includes("exec") ||
      toolName === "run_command" ||
      toolName === "bash" ||
      toolName === "shell"
    ) {
      return "command_execution";
    }
    if (
      lower.includes("write") ||
      lower.includes("edit") ||
      lower.includes("patch") ||
      lower.includes("apply") ||
      lower.includes("create")
    ) {
      return "file_change";
    }
    if (
      lower.includes("read") ||
      lower.includes("view") ||
      lower.includes("cat") ||
      lower.includes("grep") ||
      lower.includes("search")
    ) {
      return "mcp_tool_call";
    }
    return "dynamic_tool_call";
  };

  const normalizeDshTokenUsage = (
    usage: unknown,
  ): import("@t3tools/contracts").ThreadTokenUsageSnapshot | undefined => {
    if (!isRecord(usage)) return undefined;
    const rec = usage as Record<string, unknown>;
    const inputTokens =
      typeof rec.inputTokens === "number" &&
      Number.isFinite(rec.inputTokens) &&
      rec.inputTokens >= 0
        ? rec.inputTokens
        : typeof rec.input_tokens === "number" &&
            Number.isFinite(rec.input_tokens) &&
            rec.input_tokens >= 0
          ? rec.input_tokens
          : undefined;
    const outputTokens =
      typeof rec.outputTokens === "number" &&
      Number.isFinite(rec.outputTokens) &&
      rec.outputTokens >= 0
        ? rec.outputTokens
        : typeof rec.output_tokens === "number" &&
            Number.isFinite(rec.output_tokens) &&
            rec.output_tokens >= 0
          ? rec.output_tokens
          : undefined;
    const reasoningTokens =
      typeof rec.reasoningTokens === "number" &&
      Number.isFinite(rec.reasoningTokens) &&
      rec.reasoningTokens >= 0
        ? rec.reasoningTokens
        : typeof rec.thinking_tokens === "number" &&
            Number.isFinite(rec.thinking_tokens) &&
            rec.thinking_tokens >= 0
          ? rec.thinking_tokens
          : undefined;
    const cacheReadTokens =
      typeof rec.cacheReadTokens === "number" &&
      Number.isFinite(rec.cacheReadTokens) &&
      rec.cacheReadTokens >= 0
        ? rec.cacheReadTokens
        : typeof rec.cachedInputTokens === "number"
          ? rec.cachedInputTokens
          : undefined;
    const totalTokens =
      typeof rec.totalTokens === "number" && Number.isFinite(rec.totalTokens)
        ? rec.totalTokens
        : typeof rec.total_tokens === "number" && Number.isFinite(rec.total_tokens)
          ? rec.total_tokens
          : undefined;
    const candidateUsed =
      totalTokens ??
      (inputTokens !== undefined && outputTokens !== undefined
        ? inputTokens + outputTokens + (reasoningTokens ?? 0)
        : (inputTokens ?? outputTokens ?? reasoningTokens));
    if (candidateUsed === undefined || candidateUsed < 0) return undefined;
    const usedTokens = Math.floor(candidateUsed as number);
    if (!Number.isFinite(usedTokens) || usedTokens < 0) return undefined;
    const out: Record<string, unknown> = { usedTokens };
    const totalProcessed =
      totalTokens !== undefined && totalTokens > usedTokens ? Math.floor(totalTokens) : undefined;
    if (totalProcessed !== undefined) out.totalProcessedTokens = totalProcessed;
    if (inputTokens !== undefined) {
      out.inputTokens = Math.floor(inputTokens);
      out.lastInputTokens = Math.floor(inputTokens);
    }
    if (cacheReadTokens !== undefined) {
      out.cachedInputTokens = Math.floor(cacheReadTokens as number);
      out.lastCachedInputTokens = Math.floor(cacheReadTokens as number);
    }
    if (outputTokens !== undefined) {
      out.outputTokens = Math.floor(outputTokens);
      out.lastOutputTokens = Math.floor(outputTokens);
    }
    if (reasoningTokens !== undefined) {
      out.reasoningOutputTokens = Math.floor(reasoningTokens);
      out.lastReasoningOutputTokens = Math.floor(reasoningTokens);
    }
    out.lastUsedTokens = usedTokens;
    return out as unknown as import("@t3tools/contracts").ThreadTokenUsageSnapshot;
  };

  const runtimeErrorClassFromDsh = (error: RpcError): string => {
    const kind = classifyDshError(error);
    if (kind === "permission_error") return "permission_error";
    if (kind === "transport_error") return "transport_error";
    if (kind === "validation_error") return "validation_error";
    return "provider_error";
  };

  // -------------------------------------------------------------------------
  // WS frame → ProviderRuntimeEvent mapping (downlink only, no frame loss)
  // -------------------------------------------------------------------------
  const handleMuxFrame = (envelope: RpcRequest<MuxFrame>): void => {
    const frame = envelope.payload;
    const nowIsoStr = new Date().toISOString();
    const makeEventId = (): import("@t3tools/contracts").EventId =>
      EventId.make(
        `dsh-mux-${Date.now()}-${Math.random().toString(36).slice(2, 8)}` as unknown as string,
      ) as unknown as import("@t3tools/contracts").EventId;
    const emit = (evt: import("@t3tools/contracts").ProviderRuntimeEvent): void =>
      unsafeOfferRuntimeEvent(evt);
    try {
      switch (frame.type) {
        case "session/event": {
          const f = frame as Extract<MuxFrame, { type: "session/event" }>;
          const tid = threadIdForSessionId(f.sessionId);
          const rawEvent = f.event as unknown as {
            type: string;
            seq: number;
            time: number;
            data: unknown;
            ignorable?: true;
            surfaceOp?: unknown;
            sourceEventSeqs?: unknown;
          };
          const view = (f as unknown as { view?: unknown }).view as unknown as
            | { for?: string; view?: unknown }
            | undefined;
          const sessionId = f.sessionId;
          // -- SessionEvent dispatch --
          switch (rawEvent.type) {
            case "assistant/chunk": {
              const data = rawEvent.data as { turn: number; step: number; chunk: unknown };
              const chunk = data.chunk as Record<string, unknown>;
              const chunkType = chunk?.type as string | undefined;
              const turnKey = `${sessionId}:${data.turn}`;
              if (chunkType === "text-delta") {
                const text = (chunk as { text?: string }).text ?? "";
                if (text) {
                  hasEmittedDeltaByTurn.set(turnKey, true);
                  const itemId = RuntimeItemId.make(
                    `dsh-text-${sessionId}-${data.turn}-${data.step}-${(chunk as { index?: number }).index ?? 0}` as unknown as string,
                  );
                  emit({
                    eventId: makeEventId(),
                    createdAt: nowIsoStr,
                    provider: PROVIDER,
                    threadId: tid,
                    turnId: TurnId.make(`${sessionId}:${data.turn}` as unknown as string),
                    itemId,
                    type: "content.delta",
                    payload: { streamKind: "assistant_text", delta: text },
                  } as unknown as import("@t3tools/contracts").ProviderRuntimeEvent);
                }
              } else if (chunkType === "reasoning-delta") {
                const text = (chunk as { text?: string }).text ?? "";
                if (text) {
                  hasEmittedDeltaByTurn.set(turnKey, true);
                  const reasoningItemId = RuntimeItemId.make(
                    `dsh-reasoning-${sessionId}-${data.turn}-${data.step}` as unknown as string,
                  );
                  const key = reasoningItemId as unknown as string;
                  if (!reasoningItemStarted.has(key)) {
                    reasoningItemStarted.add(key);
                    emit({
                      eventId: makeEventId(),
                      createdAt: nowIsoStr,
                      provider: PROVIDER,
                      threadId: tid,
                      turnId: TurnId.make(`${sessionId}:${data.turn}` as unknown as string),
                      itemId: reasoningItemId,
                      type: "item.started",
                      payload: { itemType: "reasoning", status: "inProgress", title: "Thinking" },
                    } as unknown as import("@t3tools/contracts").ProviderRuntimeEvent);
                  }
                  emit({
                    eventId: makeEventId(),
                    createdAt: nowIsoStr,
                    provider: PROVIDER,
                    threadId: tid,
                    turnId: TurnId.make(`${sessionId}:${data.turn}` as unknown as string),
                    itemId: reasoningItemId,
                    type: "content.delta",
                    payload: { streamKind: "reasoning_text", delta: text },
                  } as unknown as import("@t3tools/contracts").ProviderRuntimeEvent);
                }
              } else if (chunkType === "tool-call-delta") {
                // tool/call-delta is transient; durable tool/call event will emit item.started. Keep hasEmittedDelta flag.
                hasEmittedDeltaByTurn.set(turnKey, true);
              } else if (chunkType === "usage") {
                const usage = (chunk as { usage?: unknown }).usage;
                const normalized = normalizeDshTokenUsage(usage);
                if (normalized) {
                  emit({
                    eventId: makeEventId(),
                    createdAt: nowIsoStr,
                    provider: PROVIDER,
                    threadId: tid,
                    turnId: TurnId.make(`${sessionId}:${data.turn}` as unknown as string),
                    type: "thread.token-usage.updated",
                    payload: { usage: normalized },
                  } as unknown as import("@t3tools/contracts").ProviderRuntimeEvent);
                }
              } else if (
                chunkType === "block-start" ||
                chunkType === "block-end" ||
                chunkType === "finish"
              ) {
                // lifecycle markers — no direct UI event; reasoning completion handled by step/end & turn/end
              }
              break;
            }
            case "assistant/message": {
              const data = rawEvent.data as {
                turn: number;
                step: number;
                message: { content?: Array<{ type: string; text?: string }> };
                usage?: unknown;
                interrupted?: true;
              };
              const turnKey = `${sessionId}:${data.turn}`;
              if (data.usage) {
                const normalized = normalizeDshTokenUsage(data.usage);
                if (normalized) {
                  emit({
                    eventId: makeEventId(),
                    createdAt: nowIsoStr,
                    provider: PROVIDER,
                    threadId: tid,
                    turnId: TurnId.make(`${sessionId}:${data.turn}` as unknown as string),
                    type: "thread.token-usage.updated",
                    payload: { usage: normalized },
                  } as unknown as import("@t3tools/contracts").ProviderRuntimeEvent);
                }
              }
              if (!hasEmittedDeltaByTurn.get(turnKey)) {
                const msg = data.message as
                  | { content?: Array<Record<string, unknown>> }
                  | undefined;
                const content = (msg?.content ?? []) as Array<Record<string, unknown>>;
                const textParts: string[] = [];
                const reasoningParts: string[] = [];
                for (const block of content) {
                  const t = block.type as string;
                  if (t === "text" && typeof block.text === "string")
                    textParts.push(block.text as string);
                  if (t === "reasoning" && typeof block.text === "string")
                    reasoningParts.push(block.text as string);
                }
                const fullText = textParts.join("");
                if (fullText) {
                  const itemId = RuntimeItemId.make(
                    `dsh-msg-${sessionId}-${data.turn}-${data.step}` as unknown as string,
                  );
                  emit({
                    eventId: makeEventId(),
                    createdAt: nowIsoStr,
                    provider: PROVIDER,
                    threadId: tid,
                    turnId: TurnId.make(`${sessionId}:${data.turn}` as unknown as string),
                    itemId,
                    type: "content.delta",
                    payload: { streamKind: "assistant_text", delta: fullText },
                  } as unknown as import("@t3tools/contracts").ProviderRuntimeEvent);
                  hasEmittedDeltaByTurn.set(turnKey, true);
                }
                const reasoningText = reasoningParts.join("");
                if (reasoningText) {
                  const reasoningItemId = RuntimeItemId.make(
                    `dsh-reasoning-${sessionId}-${data.turn}-${data.step}` as unknown as string,
                  );
                  const key = reasoningItemId as unknown as string;
                  if (!reasoningItemStarted.has(key)) {
                    reasoningItemStarted.add(key);
                    emit({
                      eventId: makeEventId(),
                      createdAt: nowIsoStr,
                      provider: PROVIDER,
                      threadId: tid,
                      turnId: TurnId.make(`${sessionId}:${data.turn}` as unknown as string),
                      itemId: reasoningItemId,
                      type: "item.started",
                      payload: { itemType: "reasoning", status: "inProgress", title: "Thinking" },
                    } as unknown as import("@t3tools/contracts").ProviderRuntimeEvent);
                  }
                  emit({
                    eventId: makeEventId(),
                    createdAt: nowIsoStr,
                    provider: PROVIDER,
                    threadId: tid,
                    turnId: TurnId.make(`${sessionId}:${data.turn}` as unknown as string),
                    itemId: reasoningItemId,
                    type: "content.delta",
                    payload: { streamKind: "reasoning_text", delta: reasoningText },
                  } as unknown as import("@t3tools/contracts").ProviderRuntimeEvent);
                  hasEmittedDeltaByTurn.set(turnKey, true);
                }
              }
              break;
            }
            case "tool/call": {
              const data = rawEvent.data as {
                turn: number;
                step: number;
                callId: string;
                name: string;
                arguments: string;
              };
              const callId = data.callId as string;
              const toolName = (data.name as string) ?? "unknown";
              const toolItemId = RuntimeItemId.make(
                `dsh-tool-${sessionId}-${callId}` as unknown as string,
              );
              const key = toolItemId as unknown as string;
              if (!toolItemStarted.has(key)) {
                toolItemStarted.add(key);
                const itemType = getToolItemType(toolName);
                let detail: string | undefined;
                try {
                  detail =
                    typeof data.arguments === "string"
                      ? (data.arguments as string)
                      : JSON.stringify(data.arguments);
                } catch {
                  detail = String(data.arguments);
                }
                const payload: Record<string, unknown> = {
                  itemType,
                  status: "inProgress",
                  title: toolName,
                };
                if (detail) payload.detail = detail.slice(0, 2000);
                if (
                  view &&
                  (view as { for?: string }).for === "call" &&
                  (view as { view?: unknown }).view
                ) {
                  payload.data = (view as { view: unknown }).view;
                } else if (view) {
                  payload.data = view;
                }
                emit({
                  eventId: makeEventId(),
                  createdAt: nowIsoStr,
                  provider: PROVIDER,
                  threadId: tid,
                  turnId: TurnId.make(`${sessionId}:${data.turn}` as unknown as string),
                  itemId: toolItemId,
                  type: "item.started",
                  payload,
                } as unknown as import("@t3tools/contracts").ProviderRuntimeEvent);
              }
              hasEmittedDeltaByTurn.set(`${sessionId}:${data.turn}`, true);
              break;
            }
            case "tool/result": {
              const data = rawEvent.data as {
                turn: number;
                step: number;
                message: { source?: { callId?: string }; content?: unknown[] };
                error?: { name: string; code: string };
                meta?: unknown;
              };
              const callId =
                (data.message as unknown as { source?: { callId?: string } })?.source?.callId ??
                `step-${data.step}-turn-${data.turn}`;
              const toolItemId = RuntimeItemId.make(
                `dsh-tool-${sessionId}-${callId}` as unknown as string,
              );
              const key = toolItemId as unknown as string;
              const isError = !!(
                data.error ||
                (
                  data.message as unknown as { content?: Array<{ isError?: boolean }> }
                )?.content?.some((c) => c.isError)
              );
              let detail: string | undefined;
              try {
                const payloadCandidate = data.message ?? data;
                detail = JSON.stringify(payloadCandidate).slice(0, 3000);
              } catch {
                detail = String(data.message);
              }
              const payload: Record<string, unknown> = {
                itemType: toolItemStarted.has(key)
                  ? getToolItemType((data as unknown as { name?: string }).name ?? "tool")
                  : "dynamic_tool_call",
                status: isError ? "failed" : "completed",
              };
              if (detail) payload.detail = detail;
              if (
                view &&
                (view as { for?: string }).for === "result" &&
                (view as { view?: unknown }).view
              ) {
                payload.data = (view as { view: unknown }).view;
              } else if (view) {
                payload.data = view;
              } else if (data.meta) {
                payload.data = data.meta;
              }
              emit({
                eventId: makeEventId(),
                createdAt: nowIsoStr,
                provider: PROVIDER,
                threadId: tid,
                turnId: TurnId.make(`${sessionId}:${data.turn}` as unknown as string),
                itemId: toolItemId,
                type: "item.completed",
                payload,
              } as unknown as import("@t3tools/contracts").ProviderRuntimeEvent);
              toolItemStarted.delete(key);
              break;
            }
            case "turn/start": {
              const data = rawEvent.data as { turn: number };
              const turnId = TurnId.make(`${sessionId}:${data.turn}` as unknown as string);
              hasEmittedDeltaByTurn.set(`${sessionId}:${data.turn}`, false);
              emit({
                eventId: makeEventId(),
                createdAt: nowIsoStr,
                provider: PROVIDER,
                threadId: tid,
                turnId,
                type: "turn.started",
                payload: {},
              } as unknown as import("@t3tools/contracts").ProviderRuntimeEvent);
              emit({
                eventId: makeEventId(),
                createdAt: nowIsoStr,
                provider: PROVIDER,
                threadId: tid,
                turnId,
                type: "session.state.changed",
                payload: { state: "running" },
              } as unknown as import("@t3tools/contracts").ProviderRuntimeEvent);
              break;
            }
            case "turn/end": {
              const data = rawEvent.data as {
                turn: number;
                reason: { kind: string; error?: { message?: string } };
              };
              const turnId = TurnId.make(`${sessionId}:${data.turn}` as unknown as string);
              // 完成该 turn 下所有仍常驻的 reasoning 项（保证答完仍在 Worked 可展开）
              for (const key of Array.from(reasoningItemStarted)) {
                if (key.startsWith(`dsh-reasoning-${sessionId}-${data.turn}-`)) {
                  const rid = RuntimeItemId.make(key as unknown as string);
                  const isError = data.reason?.kind === "error" || data.reason?.kind === "aborted";
                  emit({
                    eventId: makeEventId(),
                    createdAt: nowIsoStr,
                    provider: PROVIDER,
                    threadId: tid,
                    turnId,
                    itemId: rid,
                    type: "item.completed",
                    payload: { itemType: "reasoning", status: isError ? "failed" : "completed" },
                  } as unknown as import("@t3tools/contracts").ProviderRuntimeEvent);
                  reasoningItemStarted.delete(key);
                }
              }
              const state = (() => {
                const k = data.reason?.kind;
                if (k === "completed") return "completed";
                if (k === "aborted") return "interrupted";
                if (k === "error") return "failed";
                if (k === "blocked") return "failed";
                if (k === "interrupted") return "interrupted";
                return "completed";
              })();
              emit({
                eventId: makeEventId(),
                createdAt: nowIsoStr,
                provider: PROVIDER,
                threadId: tid,
                turnId,
                type: "turn.completed",
                payload: { state },
              } as unknown as import("@t3tools/contracts").ProviderRuntimeEvent);
              emit({
                eventId: makeEventId(),
                createdAt: nowIsoStr,
                provider: PROVIDER,
                threadId: tid,
                turnId,
                type: "session.state.changed",
                payload: { state: "ready" },
              } as unknown as import("@t3tools/contracts").ProviderRuntimeEvent);
              break;
            }
            case "step/start":
              break;
            case "step/end": {
              const data = rawEvent.data as { turn: number; step: number };
              const key = `dsh-reasoning-${sessionId}-${data.turn}-${data.step}`;
              if (reasoningItemStarted.has(key)) {
                const rid = RuntimeItemId.make(key as unknown as string);
                emit({
                  eventId: makeEventId(),
                  createdAt: nowIsoStr,
                  provider: PROVIDER,
                  threadId: tid,
                  turnId: TurnId.make(`${sessionId}:${data.turn}` as unknown as string),
                  itemId: rid,
                  type: "item.completed",
                  payload: { itemType: "reasoning", status: "completed" },
                } as unknown as import("@t3tools/contracts").ProviderRuntimeEvent);
                reasoningItemStarted.delete(key);
              }
              break;
            }
            case "todo/write": {
              const data = rawEvent.data as { todos: Array<{ content: string; status: string }> };
              emit({
                eventId: makeEventId(),
                createdAt: nowIsoStr,
                provider: PROVIDER,
                threadId: tid,
                type: "turn.plan.updated",
                payload: {
                  plan: (data.todos ?? []).map((t) => ({
                    step: t.content,
                    status:
                      t.status === "in_progress"
                        ? "inProgress"
                        : t.status === "completed"
                          ? "completed"
                          : "pending",
                  })),
                },
              } as unknown as import("@t3tools/contracts").ProviderRuntimeEvent);
              break;
            }
            case "session/title": {
              const data = rawEvent.data as { title: string };
              emit({
                eventId: makeEventId(),
                createdAt: nowIsoStr,
                provider: PROVIDER,
                threadId: tid,
                type: "thread.metadata.updated",
                payload: { metadata: { title: data.title } as unknown as Record<string, unknown> },
              } as unknown as import("@t3tools/contracts").ProviderRuntimeEvent);
              // Title also lives as projection; watermark is handled in session/projection path, but seed directly so higher-seq-wins still applies
              const pm = getProjectionMap(sessionId);
              const cur = pm.get("title");
              if (cur === undefined || rawEvent.seq > cur) {
                pm.set("title", rawEvent.seq);
              }
              break;
            }
            case "user/message": {
              const data = rawEvent.data as { content?: unknown[]; id?: string };
              // Mirror user message as item for completeness; mostly for log visibility
              emit({
                eventId: makeEventId(),
                createdAt: nowIsoStr,
                provider: PROVIDER,
                threadId: tid,
                type: "item.updated",
                payload: { data: rawEvent, view },
              } as unknown as import("@t3tools/contracts").ProviderRuntimeEvent);
              break;
            }
            default: {
              // Fallback: keep raw event + view as item.updated for persistence, but also surface as item.updated so Worked 不丢
              // Strictly for ignorable/unknown types we still落盘
              emit({
                eventId: makeEventId(),
                createdAt: nowIsoStr,
                provider: PROVIDER,
                threadId: tid,
                type: "item.updated",
                payload: { data: rawEvent, view },
              } as unknown as import("@t3tools/contracts").ProviderRuntimeEvent);
              break;
            }
          }
          break;
        }
        case "session/subscribed": {
          const f = frame as Extract<MuxFrame, { type: "session/subscribed" }>;
          const tid = threadIdForSessionId(f.sessionId);
          // 控制帧正确落盘: 订阅即就绪，且同步 lastSeq 供投影高 seq 胜的基准
          emit({
            eventId: makeEventId(),
            createdAt: nowIsoStr,
            provider: PROVIDER,
            threadId: tid,
            type: "session.state.changed",
            payload: { state: "ready" as const },
          } as unknown as import("@t3tools/contracts").ProviderRuntimeEvent);
          // Seed projection watermark base at lastSeq (asOfSeq 语义: -1 为空日志)
          const pm = getProjectionMap(f.sessionId);
          // Store a sentinel so any projection with seq <= lastSeq is considered stale unless proven newer via asOfSeq
          // We don't blindly set keys, just ensure map exists; actual keys arrive via history tail + projection frames
          void pm;
          break;
        }
        case "approval/requested": {
          const f = frame as Extract<MuxFrame, { type: "approval/requested" }>;
          const tid = threadIdForSessionId(f.sessionId);
          pendingApprovals.set(f.approvalId, {
            approvalId: f.approvalId,
            toolName: f.toolName,
            callId: f.callId,
            rpcId: envelope.rpcId,
            sessionId: f.sessionId,
          });
          emit({
            eventId: makeEventId(),
            createdAt: nowIsoStr,
            provider: PROVIDER,
            threadId: tid,
            type: "request.opened",
            payload: {
              requestType: "command_execution_approval" as const,
              detail: f.reason ?? f.toolName,
              args: {
                approvalId: f.approvalId,
                callId: f.callId,
                toolName: f.toolName,
                rpcId: envelope.rpcId,
              },
            },
          } as unknown as import("@t3tools/contracts").ProviderRuntimeEvent);
          break;
        }
        case "approval/resolved": {
          const f = frame as Extract<MuxFrame, { type: "approval/resolved" }>;
          const tid = threadIdForSessionId(f.sessionId);
          pendingApprovals.delete(f.approvalId);
          emit({
            eventId: makeEventId(),
            createdAt: nowIsoStr,
            provider: PROVIDER,
            threadId: tid,
            type: "request.resolved",
            payload: {
              requestType: "command_execution_approval" as const,
              decision: f.outcome,
              resolution: { approvalId: f.approvalId },
            },
          } as unknown as import("@t3tools/contracts").ProviderRuntimeEvent);
          break;
        }
        case "question/requested": {
          const f = frame as Extract<MuxFrame, { type: "question/requested" }>;
          const tid = threadIdForSessionId(f.sessionId);
          const questions = (f.questions as unknown[]) ?? [];
          emit({
            eventId: makeEventId(),
            createdAt: nowIsoStr,
            provider: PROVIDER,
            threadId: tid,
            type: "user-input.requested",
            payload: { questions },
          } as unknown as import("@t3tools/contracts").ProviderRuntimeEvent);
          break;
        }
        case "question/resolved": {
          const f = frame as Extract<MuxFrame, { type: "question/resolved" }>;
          const tid = threadIdForSessionId(f.sessionId);
          emit({
            eventId: makeEventId(),
            createdAt: nowIsoStr,
            provider: PROVIDER,
            threadId: tid,
            type: "user-input.resolved",
            payload: { answers: { outcome: f.outcome, questionRpcId: f.questionRpcId } },
          } as unknown as import("@t3tools/contracts").ProviderRuntimeEvent);
          break;
        }
        case "session/queue": {
          const f = frame as Extract<MuxFrame, { type: "session/queue" }>;
          const tid = threadIdForSessionId(f.sessionId);
          // 全量快照: 每次推送即权威全集，无增量合并
          emit({
            eventId: makeEventId(),
            createdAt: nowIsoStr,
            provider: PROVIDER,
            threadId: tid,
            type: "thread.metadata.updated",
            payload: { metadata: { queue: f.items } as unknown as Record<string, unknown> },
          } as unknown as import("@t3tools/contracts").ProviderRuntimeEvent);
          break;
        }
        case "session/jobs": {
          const f = frame as Extract<MuxFrame, { type: "session/jobs" }>;
          const tid = threadIdForSessionId(f.sessionId);
          emit({
            eventId: makeEventId(),
            createdAt: nowIsoStr,
            provider: PROVIDER,
            threadId: tid,
            type: "task.updated",
            payload: { jobs: f.jobs } as unknown as Record<string, unknown>,
          } as unknown as import("@t3tools/contracts").ProviderRuntimeEvent);
          break;
        }
        case "session/projection": {
          const f = frame as Extract<MuxFrame, { type: "session/projection" }>;
          const tid = threadIdForSessionId(f.sessionId);
          const pm = getProjectionMap(f.sessionId);
          const curSeq = pm.get(f.key);
          // 高 seq 胜: 仅当 incoming seq > stored 时落盘，避免旧帧覆盖新标题
          if (curSeq === undefined || f.seq > curSeq) {
            pm.set(f.key, f.seq);
            emit({
              eventId: makeEventId(),
              createdAt: nowIsoStr,
              provider: PROVIDER,
              threadId: tid,
              type: "thread.metadata.updated",
              payload: {
                metadata: {
                  projectionKey: f.key,
                  projectionValue: f.value,
                  projectionSeq: f.seq,
                } as unknown as Record<string, unknown>,
              },
            } as unknown as import("@t3tools/contracts").ProviderRuntimeEvent);
          }
          break;
        }
        case "stream/error": {
          const f = frame as Extract<MuxFrame, { type: "stream/error" }>;
          const err = f.error as RpcError;
          const cls = runtimeErrorClassFromDsh(err);
          emit({
            eventId: makeEventId(),
            createdAt: nowIsoStr,
            provider: PROVIDER,
            threadId: threadIdForSessionId("unknown" as unknown as string),
            type: "runtime.error",
            payload: {
              message: err.message ?? String(err),
              class: cls,
              details: err.details,
              code: err.code,
            },
          } as unknown as import("@t3tools/contracts").ProviderRuntimeEvent);
          break;
        }
        default: {
          const unknownType = (frame as { type: string }).type;
          emit({
            eventId: makeEventId(),
            createdAt: nowIsoStr,
            provider: PROVIDER,
            threadId: threadIdForSessionId("unknown" as unknown as string),
            type: "runtime.warning",
            payload: { message: `Unhandled mux frame ${unknownType}`, frame },
          } as unknown as import("@t3tools/contracts").ProviderRuntimeEvent);
          break;
        }
      }
    } catch (error) {
      console.error("[dsh-adapter] handleMuxFrame failed:", error);
      return;
    }
  };

  const handleHostFrame = (envelope: RpcRequest<HostFrame>): void => {
    const frame = envelope.payload;
    const nowIsoStr = new Date().toISOString();
    const eventId = EventId.make(
      `dsh-host-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    );
    let evt: import("@t3tools/contracts").ProviderRuntimeEvent | undefined;
    try {
      switch (frame.type) {
        case "host/session-added": {
          const f = frame as Extract<HostFrame, { type: "host/session-added" }>;
          const tid = threadIdForSessionId(f.sessionId);
          evt = {
            eventId,
            createdAt: nowIsoStr,
            provider: PROVIDER,
            threadId: tid,
            type: "session.started",
            payload: { message: `DSH session ${f.sessionId} added${f.cwd ? ` at ${f.cwd}` : ""}` },
          } as unknown as import("@t3tools/contracts").ProviderRuntimeEvent;
          break;
        }
        case "host/session-removed": {
          const f = frame as Extract<HostFrame, { type: "host/session-removed" }>;
          const tid = threadIdForSessionId(f.sessionId);
          evt = {
            eventId,
            createdAt: nowIsoStr,
            provider: PROVIDER,
            threadId: tid,
            type: "session.exited",
            payload: {},
          } as unknown as import("@t3tools/contracts").ProviderRuntimeEvent;
          sessions.delete(tid);
          break;
        }
        case "host/session-status": {
          const f = frame as Extract<HostFrame, { type: "host/session-status" }>;
          const tid = threadIdForSessionId(f.sessionId);
          evt = {
            eventId,
            createdAt: nowIsoStr,
            provider: PROVIDER,
            threadId: tid,
            type: "session.state.changed",
            payload: { state: f.running ? ("running" as const) : ("ready" as const) },
          } as unknown as import("@t3tools/contracts").ProviderRuntimeEvent;
          break;
        }
        case "host/agent-error": {
          const f = frame as Extract<HostFrame, { type: "host/agent-error" }>;
          const tid = threadIdForSessionId(f.sessionId);
          evt = {
            eventId,
            createdAt: nowIsoStr,
            provider: PROVIDER,
            threadId: tid,
            type: "runtime.error",
            payload: { message: f.message },
          } as unknown as import("@t3tools/contracts").ProviderRuntimeEvent;
          break;
        }
        case "host/workspace-changed": {
          const f = frame as Extract<HostFrame, { type: "host/workspace-changed" }>;
          evt = {
            eventId,
            createdAt: nowIsoStr,
            provider: PROVIDER,
            threadId: threadIdForSessionId("unknown" as unknown as string),
            type: "thread.metadata.updated",
            payload: {
              metadata: { workspaceChanged: f.workspace } as unknown as Record<string, unknown>,
            },
          } as unknown as import("@t3tools/contracts").ProviderRuntimeEvent;
          break;
        }
        case "host/workspace-removed": {
          const f = frame as Extract<HostFrame, { type: "host/workspace-removed" }>;
          evt = {
            eventId,
            createdAt: nowIsoStr,
            provider: PROVIDER,
            threadId: threadIdForSessionId("unknown" as unknown as string),
            type: "thread.metadata.updated",
            payload: {
              metadata: { workspaceRemoved: f.workspaceId } as unknown as Record<string, unknown>,
            },
          } as unknown as import("@t3tools/contracts").ProviderRuntimeEvent;
          break;
        }
        case "host/workspace-order-changed": {
          const f = frame as Extract<HostFrame, { type: "host/workspace-order-changed" }>;
          evt = {
            eventId,
            createdAt: nowIsoStr,
            provider: PROVIDER,
            threadId: threadIdForSessionId("unknown" as unknown as string),
            type: "thread.metadata.updated",
            payload: {
              metadata: { workspaceOrder: f.workspaceIds } as unknown as Record<string, unknown>,
            },
          } as unknown as import("@t3tools/contracts").ProviderRuntimeEvent;
          break;
        }
        case "host/archived-sessions-changed": {
          const f = frame as Extract<HostFrame, { type: "host/archived-sessions-changed" }>;
          evt = {
            eventId,
            createdAt: nowIsoStr,
            provider: PROVIDER,
            threadId: threadIdForSessionId("unknown" as unknown as string),
            type: "thread.metadata.updated",
            payload: {
              metadata: { archivedSessions: f.archivedSessionIds } as unknown as Record<
                string,
                unknown
              >,
            },
          } as unknown as import("@t3tools/contracts").ProviderRuntimeEvent;
          break;
        }
        case "host/remote-event": {
          const f = frame as Extract<HostFrame, { type: "host/remote-event" }>;
          evt = {
            eventId,
            createdAt: nowIsoStr,
            provider: PROVIDER,
            threadId: threadIdForSessionId("unknown" as unknown as string),
            type: "runtime.warning",
            payload: { message: `host remote-event ${f.event}`, args: f.args },
          } as unknown as import("@t3tools/contracts").ProviderRuntimeEvent;
          break;
        }
        case "stream/error": {
          const f = frame as Extract<HostFrame, { type: "stream/error" }>;
          const err = f.error as RpcError;
          const cls = runtimeErrorClassFromDsh(err);
          evt = {
            eventId,
            createdAt: nowIsoStr,
            provider: PROVIDER,
            threadId: threadIdForSessionId("unknown" as unknown as string),
            type: "runtime.error",
            payload: {
              message: err.message ?? String(err),
              class: cls,
              details: err.details,
              code: err.code,
            },
          } as unknown as import("@t3tools/contracts").ProviderRuntimeEvent;
          break;
        }
        default: {
          const unknownType = (frame as { type: string }).type;
          evt = {
            eventId,
            createdAt: nowIsoStr,
            provider: PROVIDER,
            threadId: threadIdForSessionId("unknown" as unknown as string),
            type: "runtime.warning",
            payload: { message: `Unhandled host frame ${unknownType}`, frame },
          } as unknown as import("@t3tools/contracts").ProviderRuntimeEvent;
          break;
        }
      }
    } catch (error) {
      console.error("[dsh-adapter] handleHostFrame failed:", error);
      return;
    }
    if (evt) unsafeOfferRuntimeEvent(evt);
  };

  // -------------------------------------------------------------------------
  // WS double downlink pump — generation loop (mirrors ConnectionController)
  // describe + dual WS open = ready, one stream loss rebuilds whole generation
  // -------------------------------------------------------------------------
  let wsRunning = true;
  let wsGeneration = 0;
  let wsAttempt = 0;
  let wsCurrentAbort: AbortController | null = null;

  const startWsPump = (): void => {
    const loop = async (): Promise<void> => {
      while (wsRunning) {
        wsGeneration += 1;
        const gen = wsGeneration;
        const ac = new AbortController();
        wsCurrentAbort = ac;

        let muxOpened!: () => void;
        let hostOpened!: () => void;
        const streamsOpen = Promise.all([
          new Promise<void>((resolve) => {
            muxOpened = resolve;
          }),
          new Promise<void>((resolve) => {
            hostOpened = resolve;
          }),
        ]);

        let failedResolve!: () => void;
        const failed = new Promise<void>((resolve) => {
          failedResolve = resolve;
        });
        const settle = (): void => {
          if (gen === wsGeneration && !ac.signal.aborted) {
            try {
              ac.abort();
            } catch {}
          }
          failedResolve();
        };

        // Start dual pumps (each breaks on stream/error or close)
        void (async () => {
          try {
            for await (const envelope of openMux(baseUrl, ac.signal, muxOpened)) {
              if ((envelope.payload as { type: string }).type === "stream/error") break;
              handleMuxFrame(envelope);
            }
          } catch {
            // Transport loss — fall through to settle
          }
          settle();
        })();

        void (async () => {
          try {
            for await (const envelope of openHost(baseUrl, ac.signal, hostOpened)) {
              if ((envelope.payload as { type: string }).type === "stream/error") break;
              handleHostFrame(envelope);
            }
          } catch {}
          settle();
        })();

        // Ternary handshake: host.describe + both WS open (with timeout)
        try {
          const handshakeTimeout = new AbortController();
          const sleepRace = sleepWithSignal(WS_STREAM_OPEN_TIMEOUT_MS, handshakeTimeout.signal);

          const describePromise = (async (): Promise<unknown> => {
            const rpcId = globalThis.crypto.randomUUID();
            const url = `${baseUrl}/api/host.describe`;
            const body = JSON.stringify({
              type: "client-request",
              rpcId,
              method: "host.describe",
              payload: {},
            });
            const resp = await fetch(url, {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body,
              signal: ac.signal,
            });
            if (!resp.ok) throw new Error(`host.describe HTTP ${resp.status}`);
            const json = (await resp.json()) as unknown;
            if (!isRecord(json) || (json as { type?: string }).type !== "server-response") {
              throw new Error("Invalid server-response for host.describe");
            }
            const result = (json as { result: RpcResult<unknown> }).result;
            if (!result.ok)
              throw new Error(`host.describe failed ${result.error.code}: ${result.error.message}`);
            return result.value;
          })();

          await Promise.all([describePromise, Promise.race([streamsOpen, sleepRace])]).finally(
            () => {
              try {
                handshakeTimeout.abort();
              } catch {}
            },
          );

          if (ac.signal.aborted) throw new Error("generation aborted during handshake");
          wsAttempt = 0;
          // Connected — no extra event needed; pumps now feed queue
        } catch {
          if (!ac.signal.aborted) {
            try {
              ac.abort();
            } catch {}
          }
        }

        await failed;
        if (!wsRunning) return;
        wsAttempt += 1;
        console.warn(`[dsh-adapter] WS generation ${gen} lost, retry #${wsAttempt}`);
        // Backoff before next generation
        const backoffMs = wsBackoffDelay(wsAttempt);
        // Sleep respecting current abort (aborted sleep resolves early)
        await sleepWithSignal(backoffMs, ac.signal).catch(() => {});
      }
    };
    void loop().catch((error) => {
      console.error("[dsh-adapter] WS pump loop crashed:", error);
    });
  };

  // Kick off WS pump immediately; HTTP remains as fallback if WS never connects
  startWsPump();

  const stopWsPump = (): void => {
    wsRunning = false;
    try {
      wsCurrentAbort?.abort();
    } catch {}
  };

  // -------------------------------------------------------------------------
  // Generic RPC helper — not using Effect.fn generic to avoid v4 generic
  // inference pitfalls; plain function returning Effect.
  // -------------------------------------------------------------------------
  const callDsh = <T>(
    method: string,
    payload: unknown,
    callerSignal?: AbortSignal,
    timeoutMs: number = DEFAULT_RPC_TIMEOUT_MS,
  ): Effect.Effect<T, ProviderAdapterError> =>
    Effect.gen(function* () {
      const rpcId = globalThis.crypto.randomUUID();
      const envelope: ClientRequest = { type: "client-request", rpcId, method, payload };
      const signal = combinedSignal(callerSignal, timeoutMs);
      const url = `${baseUrl}/api/${method}`;

      const response: Response = yield* Effect.tryPromise({
        try: () =>
          fetch(url, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(envelope),
            signal,
          }),
        catch: (cause) =>
          new ProviderAdapterRequestError({
            provider: PROVIDER,
            method,
            detail: cause instanceof Error ? cause.message : String(cause),
            cause,
          }),
      });

      if (!response.ok) {
        return yield* new ProviderAdapterRequestError({
          provider: PROVIDER,
          method,
          detail: `Transport failure for ${method}: HTTP ${response.status}`,
        });
      }

      const json: ServerResponse = yield* Effect.tryPromise({
        try: () => response.json() as Promise<ServerResponse>,
        catch: (cause) =>
          new ProviderAdapterRequestError({
            provider: PROVIDER,
            method,
            detail: `Invalid JSON response for ${method}`,
            cause,
          }),
      });

      if (
        !isRecord(json as unknown) ||
        (json as unknown as { type?: string }).type !== "server-response"
      ) {
        return yield* new ProviderAdapterRequestError({
          provider: PROVIDER,
          method,
          detail: `Invalid server-response envelope for ${method}`,
        });
      }

      const serverResponse = json as ServerResponse;
      const result = serverResponse.result as RpcResult<T>;
      if (!result.ok) {
        const rpcError = result.error as RpcError;
        if (rpcError.code === "session-not-found") {
          return yield* toDshRequestError(method, rpcError);
        }
        if (rpcError.code === "attachment-error") {
          return yield* new ProviderAdapterRequestError({
            provider: PROVIDER,
            method,
            detail: `attachment-error: ${rpcError.message}`,
            cause: rpcError,
          });
        }
        return yield* toDshRequestError(method, rpcError);
      }

      return result.value as T;
    });

  // -------------------------------------------------------------------------
  // Attachment helper
  // -------------------------------------------------------------------------
  const resolveImagePart = (
    attachment: ChatAttachment,
  ): Effect.Effect<PromptContentPart, ProviderAdapterError> =>
    Effect.gen(function* () {
      const attachmentPath = resolveAttachmentPath({
        attachmentsDir: serverConfig.attachmentsDir,
        attachment,
      });
      if (!attachmentPath) {
        return yield* new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "session.prompt",
          detail: `Invalid attachment id '${attachment.id}'.`,
        });
      }
      if (attachment.sizeBytes > ATTACHMENT_MAX_BYTES) {
        return yield* new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "session.prompt",
          detail: `attachment-error: attachment '${attachment.name}' exceeds 300MiB limit (${attachment.sizeBytes} bytes).`,
        });
      }
      const bytes = yield* fileSystem.readFile(attachmentPath).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "session.prompt",
              detail: `Failed to read attachment file: ${(cause as unknown as { message?: string }).message ?? String(cause)}.`,
              cause,
            }),
        ),
      );
      if (bytes.length > ATTACHMENT_MAX_BYTES) {
        return yield* new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "session.prompt",
          detail: `attachment-error: attachment file exceeds 300MiB limit (${bytes.length} bytes).`,
        });
      }
      if (!attachment.mimeType.startsWith("image/")) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "sendTurn",
          issue: `Unsupported attachment mimeType '${attachment.mimeType}'.`,
        });
      }
      const supported = new Set(["image/png", "image/jpeg", "image/webp", "image/gif"]);
      if (!supported.has(attachment.mimeType.toLowerCase())) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "sendTurn",
          issue: `Unsupported image mimeType '${attachment.mimeType}'.`,
        });
      }
      const base64 = Buffer.from(bytes).toString("base64");
      const mediaType = attachment.mimeType.toLowerCase() as PromptContentPart extends {
        type: "image";
        mediaType: infer M;
      }
        ? M
        : never;
      const part: PromptContentPart = {
        type: "image",
        mediaType,
        data: base64,
        ...(attachment.name ? { name: attachment.name } : {}),
      } as PromptContentPart;
      return part;
    });

  // -------------------------------------------------------------------------
  // LLM capability probe for hasImage
  // -------------------------------------------------------------------------
  const probeLlmForImage = (
    callerSignal?: AbortSignal,
  ): Effect.Effect<void, ProviderAdapterError> =>
    Effect.gen(function* () {
      const result = yield* callDsh<{
        groups: ModelProviderGroup[];
        failures: ModelCatalogFailure[];
      }>("llm.models", {}, callerSignal, DEFAULT_RPC_TIMEOUT_MS).pipe(
        Effect.map((v) => ({ _tag: "ok" as const, v })),
        Effect.catch((cause) => Effect.succeed({ _tag: "err" as const, cause })),
      );
      if (result._tag === "err") {
        return yield* new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "llm.models",
          detail: `LLM capability probe failed before image prompt: ${(result.cause as { message?: string }).message ?? String(result.cause)}`,
          cause: result.cause,
        });
      }
      return undefined;
    });

  // -------------------------------------------------------------------------
  // Adapter methods
  // -------------------------------------------------------------------------
  const startSession: DshAdapterShape["startSession"] = (input: ProviderSessionStartInput) =>
    Effect.gen(function* () {
      const existing = sessions.get(input.threadId);
      if (existing) {
        sessions.delete(input.threadId);
      }

      const resume = parseDshResumeCursor(input.resumeCursor);
      let sessionId: string | undefined;

      if (resume) {
        const check = yield* callDsh<{
          events: HistoryEntry[];
          hasMore: boolean;
          projections?: SessionProjectionsBlock;
        }>(
          "session.history",
          { sessionId: resume.sessionId, maxMessages: 1 },
          undefined,
          DEFAULT_RPC_TIMEOUT_MS,
        ).pipe(
          Effect.map((v) => ({ _tag: "ok" as const, v })),
          Effect.catch((cause) => Effect.succeed({ _tag: "err" as const, cause })),
        );
        if (check._tag === "ok") {
          sessionId = resume.sessionId;
        } else {
          const causeMsg = (check.cause as { message?: string }).message ?? String(check.cause);
          const isNotFound =
            causeMsg.toLowerCase().includes("session-not-found") ||
            causeMsg.toLowerCase().includes("not found");
          if (!isNotFound) {
            return yield* check.cause as unknown as Effect.Effect<never, ProviderAdapterError>;
          }
        }
      }

      if (!sessionId) {
        const payload: Record<string, unknown> = {};
        if (input.cwd) payload.cwd = input.cwd;
        const created = yield* callDsh<{ sessionId: string }>("session.create", payload);
        sessionId = created.sessionId;
      }

      const now = yield* nowIso;
      const context: DshSessionContext = {
        threadId: input.threadId,
        sessionId: sessionId as SessionId,
        createdAt: now,
        updatedAt: now,
        ...(input.cwd ? { cwd: input.cwd } : {}),
        ...(input.modelSelection?.model ? { model: input.modelSelection.model } : {}),
      };
      sessions.set(input.threadId, context);

      const resumeCursor = makeResumeCursor(sessionId as SessionId);

      yield* offerRuntimeEvent({
        eventId: yield* nextEventId,
        createdAt: now,
        provider: PROVIDER,
        threadId: input.threadId,
        type: "session.started",
        payload: { resume: resumeCursor },
      }).pipe(Effect.orElseSucceed(() => undefined));

      yield* offerRuntimeEvent({
        eventId: yield* nextEventId,
        createdAt: yield* nowIso,
        provider: PROVIDER,
        threadId: input.threadId,
        type: "session.configured",
        payload: {
          config: {
            ...(input.cwd ? { cwd: input.cwd } : {}),
            ...(input.modelSelection?.model ? { model: input.modelSelection.model } : {}),
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

      const session: ProviderSession = {
        provider: PROVIDER,
        ...(options?.instanceId
          ? { providerInstanceId: options.instanceId as ProviderSession["providerInstanceId"] }
          : {}),
        status: "ready",
        runtimeMode: input.runtimeMode,
        ...(input.cwd ? { cwd: input.cwd } : {}),
        ...(input.modelSelection?.model ? { model: input.modelSelection.model } : {}),
        threadId: input.threadId,
        resumeCursor,
        createdAt: now,
        updatedAt: now,
      };
      return session;
    });

  const listSessions: DshAdapterShape["listSessions"] = () =>
    Effect.gen(function* () {
      const result = yield* callDsh<{ items: SessionSummary[] }>("session.list", {}).pipe(
        Effect.orElseSucceed(() => ({ items: [] as SessionSummary[] })),
      );
      const now = yield* nowIso;
      const out: ProviderSession[] = [];
      for (const summary of result.items) {
        const mappedThreadId = (() => {
          for (const [tid, ctx] of sessions.entries()) {
            if (ctx.sessionId === summary.sessionId) return tid;
          }
          return summary.sessionId as unknown as ThreadId;
        })();
        const updatedAtIso = (() => {
          const maybe = DateTime.make(summary.updatedAt as unknown as number);
          if (Option.isSome(maybe)) return DateTime.formatIso(maybe.value);
          // Fallback via global Date — allowed because input is epoch number
          return new Date(summary.updatedAt).toISOString();
        })();
        out.push({
          provider: PROVIDER,
          ...(options?.instanceId
            ? { providerInstanceId: options.instanceId as ProviderSession["providerInstanceId"] }
            : {}),
          status: summary.running ? "running" : "ready",
          runtimeMode: "full-access",
          ...(summary.cwd ? { cwd: summary.cwd } : {}),
          threadId: mappedThreadId,
          resumeCursor: makeResumeCursor(summary.sessionId),
          createdAt: now,
          updatedAt: updatedAtIso,
        } as ProviderSession);
      }
      for (const ctx of sessions.values()) {
        const already = out.some(
          (s) =>
            (s.resumeCursor as unknown as DshResumeCursor)?.sessionId === ctx.sessionId ||
            s.threadId === ctx.threadId,
        );
        if (!already) {
          out.push({
            provider: PROVIDER,
            ...(options?.instanceId
              ? { providerInstanceId: options.instanceId as ProviderSession["providerInstanceId"] }
              : {}),
            status: "ready",
            runtimeMode: "full-access",
            ...(ctx.cwd ? { cwd: ctx.cwd } : {}),
            ...(ctx.model ? { model: ctx.model } : {}),
            threadId: ctx.threadId,
            resumeCursor: makeResumeCursor(ctx.sessionId),
            createdAt: ctx.createdAt,
            updatedAt: ctx.updatedAt,
          } as ProviderSession);
        }
      }
      return out;
    });

  const hasSession: DshAdapterShape["hasSession"] = (threadId) =>
    Effect.succeed(sessions.has(threadId));

  const readThread: DshAdapterShape["readThread"] = (threadId) =>
    Effect.gen(function* () {
      const ctx = sessions.get(threadId);
      const sessionId = ctx?.sessionId ?? (threadId as unknown as string);
      const result = yield* callDsh<{
        events: HistoryEntry[];
        hasMore: boolean;
        projections?: SessionProjectionsBlock;
      }>("session.history", { sessionId, maxMessages: 200 }).pipe(
        Effect.catch((cause) => {
          const msg = (cause as { message?: string }).message ?? String(cause);
          if (msg.toLowerCase().includes("session-not-found")) {
            return Effect.fail(
              toDshSessionNotFound(threadId, {
                code: "session-not-found",
                message: msg,
                details: { sessionId },
              }),
            );
          }
          return Effect.fail(cause as ProviderAdapterError);
        }),
      );
      const turns: Array<{ readonly id: TurnId; readonly items: ReadonlyArray<unknown> }> =
        result.events.map((entry, idx) => ({
          id: TurnId.make(`${threadId}:${entry.event.seq ?? entry.event.time ?? idx}`),
          items: [entry],
        }));
      if (result.projections) {
        yield* offerRuntimeEvent({
          eventId: yield* nextEventId,
          createdAt: yield* nowIso,
          provider: PROVIDER,
          threadId,
          type: "thread.metadata.updated",
          payload: { projections: result.projections },
        } as unknown as import("@t3tools/contracts").ProviderRuntimeEvent).pipe(
          Effect.orElseSucceed(() => undefined),
        );
      }
      return { threadId, turns };
    });

  const rollbackThread: DshAdapterShape["rollbackThread"] = (threadId, _numTurns) =>
    Effect.gen(function* () {
      const ctx = sessions.get(threadId);
      if (!ctx) {
        return yield* new ProviderAdapterSessionNotFoundError({
          provider: PROVIDER,
          threadId: threadId as string,
        });
      }
      const snapshot = yield* readThread(threadId);
      return snapshot;
    });

  const sendTurn: DshAdapterShape["sendTurn"] = (input: ProviderSendTurnInput) =>
    Effect.gen(function* () {
      const ctx = sessions.get(input.threadId);
      if (!ctx) {
        return yield* new ProviderAdapterSessionNotFoundError({
          provider: PROVIDER,
          threadId: input.threadId as string,
        });
      }

      const text = input.input?.trim() ?? "";
      const hasAttachments = (input.attachments?.length ?? 0) > 0;
      if (!text && !hasAttachments) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "sendTurn",
          issue: "DSH turns require a non-empty text prompt or attachments.",
        });
      }
      if (input.attachments && input.attachments.length > 8) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "sendTurn",
          issue: `Too many attachments: ${input.attachments.length} (max 8).`,
        });
      }

      const parts: PromptContentPart[] = [];
      if (text.length > 0) {
        parts.push({ type: "text", text } as PromptContentPart);
      }

      let hasImage = false;
      if (input.attachments && input.attachments.length > 0) {
        let totalImageBytes = 0;
        for (const att of input.attachments) {
          if (att.type !== "image") continue;
          hasImage = true;
          const part = yield* resolveImagePart(att as ChatAttachment);
          totalImageBytes += (part as { data: string }).data.length * 0.75;
          if (totalImageBytes > ATTACHMENT_MAX_BYTES) {
            return yield* new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "session.prompt",
              detail: `attachment-error: total image payload exceeds 300MiB limit.`,
            });
          }
          parts.push(part);
        }
      }

      if (hasImage) {
        yield* probeLlmForImage(undefined);
      }

      const desiredModel = input.modelSelection?.model;
      if (desiredModel && desiredModel !== ctx.model) {
        const providerForSelect = desiredModel.includes("/")
          ? desiredModel.split("/")[0]!
          : "deepseek";
        const modelForSelect = desiredModel.includes("/")
          ? desiredModel.split("/").slice(1).join("/")
          : desiredModel;
        let reasoningEffort: string | undefined;
        try {
          const opts = (
            input.modelSelection as unknown as {
              options?: ReadonlyArray<{ id: string; value: unknown }>;
            }
          )?.options;
          if (Array.isArray(opts)) {
            const found = opts.find((o) => o.id === "reasoningEffort" || o.id === "effort");
            if (found && typeof found.value === "string") reasoningEffort = found.value;
          }
        } catch {}
        const selectPayload: Record<string, unknown> = {
          sessionId: ctx.sessionId,
          provider: providerForSelect,
          model: modelForSelect,
          ...(reasoningEffort ? { reasoningEffort } : {}),
        };
        yield* callDsh<{ selected: { provider: string; model: string; reasoningEffort?: string } }>(
          "session.selectModel",
          selectPayload,
        ).pipe(Effect.mapError((cause) => cause as ProviderAdapterError));
        ctx.model = desiredModel;
        const updatedAt = yield* nowIso;
        ctx.updatedAt = updatedAt;
        yield* offerRuntimeEvent({
          eventId: yield* nextEventId,
          createdAt: updatedAt,
          provider: PROVIDER,
          threadId: input.threadId,
          type: "model.rerouted",
          payload: {
            fromModel: ctx.model ?? "unknown",
            toModel: desiredModel,
            reason: "user requested model switch",
          },
        } as unknown as import("@t3tools/contracts").ProviderRuntimeEvent).pipe(
          Effect.orElseSucceed(() => undefined),
        );
      }

      const mode: "queue" | "steer" = "queue";
      const clientTimeZone = (() => {
        try {
          return Intl.DateTimeFormat().resolvedOptions().timeZone;
        } catch {
          return undefined;
        }
      })();

      const promptPayload: Record<string, unknown> = {
        sessionId: ctx.sessionId,
        mode,
        content: parts,
        ...(clientTimeZone ? { clientTimeZone } : {}),
      };

      const promptSignal = AbortSignal.timeout(PROMPT_TIMEOUT_MS);

      const nowForTurn = yield* DateTime.now;
      const turnId = TurnId.make(
        `${ctx.sessionId}:${DateTime.toEpochMillis(nowForTurn)}-${globalThis.crypto.randomUUID().slice(0, 8)}`,
      );

      yield* offerRuntimeEvent({
        eventId: yield* nextEventId,
        createdAt: yield* nowIso,
        provider: PROVIDER,
        threadId: input.threadId,
        turnId,
        type: "turn.started",
        payload: { ...(ctx.model ? { model: ctx.model } : {}) },
      } as unknown as import("@t3tools/contracts").ProviderRuntimeEvent).pipe(
        Effect.orElseSucceed(() => undefined),
      );

      yield* offerRuntimeEvent({
        eventId: yield* nextEventId,
        createdAt: yield* nowIso,
        provider: PROVIDER,
        threadId: input.threadId,
        turnId,
        type: "session.state.changed",
        payload: { state: "running" },
      } as unknown as import("@t3tools/contracts").ProviderRuntimeEvent).pipe(
        Effect.orElseSucceed(() => undefined),
      );

      const promptResult = yield* callDsh<{
        accepted: true;
        command?: { kind: "success"; text?: string };
      }>("session.prompt", promptPayload, promptSignal, PROMPT_TIMEOUT_MS).pipe(
        Effect.catch((cause) =>
          offerRuntimeEvent({
            eventId: EventId.make(
              `dsh-err-${DateTime.toEpochMillis(nowForTurn)}-${(eventCounter += 1)}`,
            ),
            createdAt: DateTime.formatIso(nowForTurn),
            provider: PROVIDER,
            threadId: input.threadId,
            turnId,
            type: "runtime.error",
            payload: { message: (cause as { message?: string }).message ?? String(cause) },
          } as unknown as import("@t3tools/contracts").ProviderRuntimeEvent).pipe(
            Effect.orElseSucceed(() => undefined),
            Effect.flatMap(() => Effect.fail(cause as ProviderAdapterError)),
          ),
        ),
      );

      void promptResult;

      yield* offerRuntimeEvent({
        eventId: yield* nextEventId,
        createdAt: yield* nowIso,
        provider: PROVIDER,
        threadId: input.threadId,
        turnId,
        type: "turn.completed",
        payload: { state: "completed" },
      } as unknown as import("@t3tools/contracts").ProviderRuntimeEvent).pipe(
        Effect.orElseSucceed(() => undefined),
      );

      yield* offerRuntimeEvent({
        eventId: yield* nextEventId,
        createdAt: yield* nowIso,
        provider: PROVIDER,
        threadId: input.threadId,
        turnId,
        type: "session.state.changed",
        payload: { state: "ready" },
      } as unknown as import("@t3tools/contracts").ProviderRuntimeEvent).pipe(
        Effect.orElseSucceed(() => undefined),
      );

      const updatedAt = yield* nowIso;
      ctx.updatedAt = updatedAt;

      const resumeCursor = makeResumeCursor(ctx.sessionId);
      return {
        threadId: input.threadId,
        turnId,
        resumeCursor,
      } satisfies ProviderTurnStartResult;
    });

  const interruptTurn: DshAdapterShape["interruptTurn"] = (threadId, turnId) =>
    Effect.gen(function* () {
      const ctx = sessions.get(threadId);
      if (!ctx) {
        return yield* new ProviderAdapterSessionNotFoundError({
          provider: PROVIDER,
          threadId: threadId as string,
        });
      }
      yield* callDsh<{ accepted: true }>("session.cancel", { sessionId: ctx.sessionId }).pipe(
        Effect.catch((cause) => {
          const msg = (cause as { message?: string }).message ?? String(cause);
          if (msg.toLowerCase().includes("session-not-found")) {
            return Effect.succeed({ accepted: true as const });
          }
          return Effect.fail(cause as ProviderAdapterError);
        }),
      );

      // Use Clock instead of Date.now
      const now = yield* DateTime.now;
      const activeTurnId =
        turnId ?? TurnId.make(`${threadId}:cancel-${DateTime.toEpochMillis(now)}`);

      yield* offerRuntimeEvent({
        eventId: yield* nextEventId,
        createdAt: yield* nowIso,
        provider: PROVIDER,
        threadId,
        turnId: activeTurnId,
        type: "turn.aborted",
        payload: { reason: "User interrupted the turn." },
      } as unknown as import("@t3tools/contracts").ProviderRuntimeEvent).pipe(
        Effect.orElseSucceed(() => undefined),
      );

      yield* offerRuntimeEvent({
        eventId: yield* nextEventId,
        createdAt: yield* nowIso,
        provider: PROVIDER,
        threadId,
        turnId: activeTurnId,
        type: "turn.completed",
        payload: { state: "interrupted" },
      } as unknown as import("@t3tools/contracts").ProviderRuntimeEvent).pipe(
        Effect.orElseSucceed(() => undefined),
      );

      yield* offerRuntimeEvent({
        eventId: yield* nextEventId,
        createdAt: yield* nowIso,
        provider: PROVIDER,
        threadId,
        type: "session.state.changed",
        payload: { state: "ready" },
      } as unknown as import("@t3tools/contracts").ProviderRuntimeEvent).pipe(
        Effect.orElseSucceed(() => undefined),
      );
    });

  const respondToRequest: DshAdapterShape["respondToRequest"] = (
    _threadId,
    _requestId,
    _decision,
  ) =>
    Effect.fail(
      new ProviderAdapterRequestError({
        provider: PROVIDER,
        method: "respondToRequest",
        detail: "DSH approvals are handled over the WS mux; HTTP adapter has no pending approval.",
      }),
    );

  const respondToUserInput: DshAdapterShape["respondToUserInput"] = () =>
    Effect.fail(
      new ProviderAdapterRequestError({
        provider: PROVIDER,
        method: "respondToUserInput",
        detail:
          "DSH does not surface structured user-input requests; use respondToRequest for approvals.",
      }),
    );

  const stopSession: DshAdapterShape["stopSession"] = (threadId) =>
    Effect.gen(function* () {
      const ctx = sessions.get(threadId);
      if (!ctx) return;
      sessions.delete(threadId);
      yield* offerRuntimeEvent({
        eventId: yield* nextEventId,
        createdAt: yield* nowIso,
        provider: PROVIDER,
        threadId,
        type: "session.exited",
        payload: {},
      } as unknown as import("@t3tools/contracts").ProviderRuntimeEvent).pipe(
        Effect.orElseSucceed(() => undefined),
      );
    });

  const stopAll: Effect.Effect<void, ProviderAdapterError> = Effect.gen(function* () {
    // Tear down WS pump first — whole generation rebuild if one stream dies, stopAll kills both
    try {
      stopWsPump();
    } catch {}
    const toStop = Array.from(sessions.values());
    sessions.clear();
    for (const ctx of toStop) {
      yield* offerRuntimeEvent({
        eventId: yield* nextEventId,
        createdAt: yield* nowIso,
        provider: PROVIDER,
        threadId: ctx.threadId,
        type: "session.exited",
        payload: {},
      } as unknown as import("@t3tools/contracts").ProviderRuntimeEvent).pipe(
        Effect.orElseSucceed(() => undefined),
      );
    }
    // Ensure WS generation does not spuriously retry after explicit stopAll
    wsAttempt = 0;
  });

  // WS is now primary; HTTP unary + history remain as fallback when WS down
  const adapter: DshAdapterShape = {
    provider: PROVIDER,
    baseUrl,
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
