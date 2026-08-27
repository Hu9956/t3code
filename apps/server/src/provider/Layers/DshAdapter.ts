// @effect-diagnostics globalFetchInEffect:off
// @effect-diagnostics globalFetch:off
// @effect-diagnostics cryptoRandomUUIDInEffect:off
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
 * WS double-pump (events.mux / events.host) is Task 3b — not in this file.
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
  });

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
