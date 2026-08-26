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

interface AgyNativeFrameLike {
  readonly event?: string;
  readonly conversation_id?: string;
  readonly result?: AgyResultFrame;
}

function buildAgySpawnArgs(model: string | undefined): ReadonlyArray<string> {
  return [
    "--input-format",
    "stream-json",
    "--output-format",
    "stream-json",
    ...(model ? ["--model", model] : []),
    "--mode",
    "accept-edits",
  ];
}

function buildUserMessageFrame(prompt: string): string {
  return `${JSON.stringify({
    event: "user",
    message: {
      content: [{ type: "text", text: prompt }],
    },
  })}\n`;
}

/** Parse one NDJSON line; malformed lines are dropped (diagnostics may interleave). */
function parseFrame(line: string): Option.Option<AgyNativeFrameLike> {
  try {
    return Option.some(JSON.parse(line) as AgyNativeFrameLike);
  } catch {
    return Option.none();
  }
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
   * frame. The final response is emitted as one `content.delta`; the caller
   * closes the turn with `turn.completed`.
   */
  const runTurnToResult = (
    context: AgySessionContext,
    prompt: string,
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
      yield* context.child.stdout.pipe(
        Stream.decodeText(),
        Stream.splitLines,
        Stream.map((line) => line.trim()),
        Stream.filter((line) => line.length > 0),
        Stream.map(parseFrame),
        Stream.filter(Option.isSome),
        Stream.map((frameOption) => frameOption.value),
        Stream.tap((frame) => {
          if (frame.event === "init" && frame.conversation_id) {
            context.conversationId = frame.conversation_id;
          }
          return Effect.void;
        }),
        Stream.filter((frame) => frame.event === "result" && frame.result !== undefined),
        Stream.map((frame) => frame.result!),
        Stream.runForEach((result) =>
          Effect.sync(() => {
            resultFrame = result;
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
      return resultFrame;
    });

  const startSession: AntigravityAdapterShape["startSession"] = (input) =>
    Effect.gen(function* () {
      if (sessions.has(input.threadId)) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "startSession",
          issue: `Session ${input.threadId} already exists.`,
        });
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

      const result = yield* runTurnToResult(context, prompt);

      if (result.conversation_id && !context.conversationId) {
        context.conversationId = result.conversation_id;
      }

      yield* offerRuntimeEvent({
        eventId: yield* nextEventId,
        createdAt: yield* nowIso,
        provider: PROVIDER,
        threadId: input.threadId,
        turnId,
        itemId: RuntimeItemId.make(`agy-item-${turnId}`),
        type: "content.delta",
        payload: {
          streamKind: "assistant_text",
          delta: result.response ?? "",
        },
      });
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
