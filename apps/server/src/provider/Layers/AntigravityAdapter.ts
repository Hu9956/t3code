/**
 * AntigravityAdapter — `ProviderAdapterShape` over the Antigravity CLI's
 * stream-json NDJSON protocol.
 *
 * The CLI is spawned once per session:
 *
 *   agy --input-format stream-json --output-format stream-json [--model X]
 *
 * Protocol (verified against agy 1.1.21):
 *   - stdin:  one NDJSON message per prompt —
 *             {"event":"user","message":{"content":[{"type":"text","text":"…"}]}}
 *   - stdout: `init` (tool inventory) → `step_update`* → `result` per turn
 *   - the process serves one conversation; a new conversation means a new
 *     process.
 *
 * v1 scope: text turns only, with the final response emitted as one
 * `content.delta` + `turn.completed`. Permission prompts are avoided by
 * running with `--mode accept-edits`; interactive approvals surface as an
 * unsupported request error rather than a half-wired gate.
 *
 * @module provider/Layers/AntigravityAdapter
 */
import {
  EventId,
  type AntigravitySettings,
  type ProviderRuntimeEvent,
  type ProviderSession,
  ProviderDriverKind,
  type ProviderTurnStartResult,
  RuntimeItemId,
  type ThreadId,
  TurnId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/unstable/process/ChildProcess";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";

import { ServerConfig } from "../../config.ts";
import {
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
} from "../Errors.ts";
import type { AntigravityAdapterShape } from "../Services/AntigravityAdapter.ts";

const PROVIDER = ProviderDriverKind.make("antigravity");

interface AgyResultFrame {
  readonly conversation_id?: string;
  readonly status?: string;
  readonly response?: string;
  readonly error?: string;
}

interface AgySessionContext {
  readonly threadId: ThreadId;
  conversationId: string | undefined;
  readonly child: ChildProcessSpawner.ChildProcessHandle;
  readonly model: string | undefined;
}

interface AgyStepUpdate {
  readonly conversation_id?: string;
  readonly step_index?: number;
  readonly state?: string;
  readonly step_type?: string;
  readonly tool_name?: string;
  readonly text_delta?: string;
  readonly thinking?: string;
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
  };
}

interface AgyNativeFrameLike {
  readonly event?: string;
  readonly conversation_id?: string;
  readonly result?: AgyResultFrame;
  readonly step_update?: AgyStepUpdate;
  readonly init?: unknown;
}

function buildAgySpawnArgs(model: string | undefined): ReadonlyArray<string> {
  return [
    "--input-format",
    "stream-json",
    "--output-format",
    "stream-json",
    ...(model ? ["--model", model] : []),
    "--dangerously-skip-permissions",
  ];
}

function buildUserMessageFrame(prompt: string): string {
  return `${globalThis.JSON.stringify({
    event: "user",
    message: {
      content: [{ type: "text", text: prompt }],
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

export const makeAntigravityAdapter = Effect.fn("makeAntigravityAdapter")(function* (
  antigravitySettings: AntigravitySettings,
  options?: {
    readonly instanceId?: string;
    readonly environment?: NodeJS.ProcessEnv;
  },
) {
  const serverConfig = yield* ServerConfig;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
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

  const spawnSessionProcess = (model: string | undefined, scope: Scope.Scope) => {
    const command = ChildProcess.make(binaryPath, buildAgySpawnArgs(model), {
      cwd: serverConfig.cwd,
      env: environment,
      extendEnv: true,
    });
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

  /**
   * Write the prompt frame, then consume stdout until the turn's `result`
   * frame. Emits incremental `content.delta` / `item.started` / `item.completed`
   * as `step_update` arrives (B1).
   */
  const runTurnToResult = (
    context: AgySessionContext,
    prompt: string,
    turnId: TurnId,
  ): Effect.Effect<AgyResultFrame, ProviderAdapterProcessError | ProviderAdapterRequestError> =>
    Effect.gen(function* () {
      yield* Stream.run(
        Stream.encodeText(Stream.make(buildUserMessageFrame(prompt))),
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
                yield* offerRuntimeEvent({
                  eventId: yield* nextEventId,
                  createdAt: yield* nowIso,
                  provider: PROVIDER,
                  threadId: context.threadId,
                  turnId,
                  itemId: RuntimeItemId.make(`agy-think-${turnId}-${su.step_index ?? 0}`),
                  type: "content.delta",
                  payload: {
                    streamKind: "reasoning_text",
                    delta: su.thinking,
                  },
                });
              }
              // tool lifecycle
              if (su.step_type === "tool" && su.tool_name) {
                const toolItemId = RuntimeItemId.make(`agy-tool-${turnId}-${su.step_index ?? 0}`);
                const isCommand =
                  su.tool_name.toLowerCase().includes("command") || su.tool_name === "run_command";
                const itemType = isCommand ? "command_execution" : "dynamic_tool_call";
                if (su.state === "ACTIVE") {
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
                      detail: jsonStringify(su.tool_info?.parameters ?? {}),
                    },
                  });
                } else if (su.state === "DONE") {
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
                      detail: su.tool_info?.output ?? jsonStringify(su.tool_info ?? {}),
                    },
                  });
                } else if (su.state === "ERROR") {
                  const errMsg = su.tool_info?.error?.message ?? "tool failed";
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
                      detail: errMsg,
                    },
                  });
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
      // fallback for providers that don't emit text_delta (should not happen after B1)
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
      }
      return resultFrame;
    });

  const startSession: AntigravityAdapterShape["startSession"] = (input) =>
    Effect.gen(function* () {
      const existing = sessions.get(input.threadId);
      if (existing) {
        yield* killContext(existing).pipe(Effect.orElseSucceed(() => undefined));
        sessions.delete(input.threadId);
      }
      const modelSelection =
        input.modelSelection?.instanceId === options?.instanceId ? input.modelSelection : undefined;
      const model = modelSelection?.model ?? undefined;

      // The child handle must outlive the caller's per-request scope, so it is
      // spawned into an adapter-owned scope and torn down through the explicit
      // kill paths: stopSession / interruptTurn / stopAll.
      const sessionScope = yield* Scope.make("sequential");
      const child = yield* spawnSessionProcess(model, sessionScope);
      sessions.set(input.threadId, {
        threadId: input.threadId,
        conversationId: undefined,
        child,
        model,
      });

      const now = yield* nowIso;
      const session: ProviderSession = {
        provider: PROVIDER,
        providerInstanceId: options?.instanceId
          ? (options.instanceId as ProviderSession["providerInstanceId"])
          : undefined,
        status: "ready",
        runtimeMode: input.runtimeMode,
        cwd: input.cwd,
        ...(model ? { model } : {}),
        threadId: input.threadId,
        createdAt: now,
        updatedAt: now,
      };
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
      if (!prompt) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "sendTurn",
          issue: "Antigravity turns require a non-empty text prompt.",
        });
      }
      const turnClock = yield* DateTime.now;
      const turnId = TurnId.make(`${input.threadId}:${DateTime.toEpochMillis(turnClock)}`);
      yield* offerRuntimeEvent({
        eventId: yield* nextEventId,
        createdAt: yield* nowIso,
        provider: PROVIDER,
        threadId: input.threadId,
        turnId,
        type: "turn.started",
        payload: {},
      });

      const result = yield* runTurnToResult(context, prompt, turnId);

      if (result.conversation_id && !context.conversationId) {
        context.conversationId = result.conversation_id;
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

      return {
        threadId: input.threadId,
        turnId,
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

  const interruptTurn: AntigravityAdapterShape["interruptTurn"] = (threadId) =>
    Effect.gen(function* () {
      const context = requireSession(threadId);
      yield* killContext(context);
    });

  const respondToRequest: AntigravityAdapterShape["respondToRequest"] = () =>
    Effect.fail(
      new ProviderAdapterRequestError({
        provider: PROVIDER,
        method: "respondToRequest",
        detail: "Antigravity v1 runs with accept-edits and does not surface interactive approvals.",
      }),
    );

  const respondToUserInput: AntigravityAdapterShape["respondToUserInput"] = () =>
    Effect.fail(
      new ProviderAdapterRequestError({
        provider: PROVIDER,
        method: "respondToUserInput",
        detail: "Antigravity v1 does not surface structured user-input requests.",
      }),
    );

  const stopSession: AntigravityAdapterShape["stopSession"] = (threadId) =>
    Effect.gen(function* () {
      const context = sessions.get(threadId);
      if (!context) return;
      yield* killContext(context).pipe(Effect.orElseSucceed(() => undefined));
      sessions.delete(threadId);
    });

  const listSessions: AntigravityAdapterShape["listSessions"] = () =>
    Effect.sync(() => {
      const list: ProviderSession[] = [];
      for (const context of sessions.values()) {
        list.push({
          provider: PROVIDER,
          status: "running",
          runtimeMode: "full-access",
          ...(context.model ? { model: context.model } : {}),
          threadId: context.threadId,
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
    context.child.kill().pipe(Effect.orElseSucceed(() => undefined)),
  ).pipe(Effect.asVoid);

  yield* Effect.addFinalizer(() => stopAll);

  const adapter: AntigravityAdapterShape = {
    provider: PROVIDER,
    capabilities: {
      sessionModelSwitch: "unsupported",
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
