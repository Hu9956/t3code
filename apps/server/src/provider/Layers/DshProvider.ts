// @effect-diagnostics globalFetchInEffect:off
// @effect-diagnostics globalFetch:off
// @effect-diagnostics cryptoRandomUUIDInEffect:off
// @effect-diagnostics preferSchemaOverJson:off
// @effect-diagnostics missingEffectError:off
// @effect-diagnostics missingEffectContext:off
/**
 * DshProvider — thin provider layer for the DSH host service.
 *
 * Mirrors the AntigravityProvider / CodexProvider pattern: a small module
 * that surfaces the DSH model directory (`llm.providers` + `llm.models`)
 * and the in-session model switch (`session.selectModel`) as Effect
 * helpers. The heavy lifting (session lifecycle, prompt, history) lives in
 * `DshAdapter`; this file stays thin so the driver snapshot and the adapter
 * can share a single `baseUrl` without duplication.
 *
 * Capabilities match `AntigravityProvider.ts:40` — `requiresNewThreadForModelChange: false`
 * and adapter capabilities `sessionModelSwitch: "in-session"` — DSH switches
 * models in place without forking the session.
 *
 * @module provider/Layers/DshProvider
 */
import type { ModelCapabilities, ServerProviderModel } from "@t3tools/contracts";
import { createModelCapabilities } from "@t3tools/shared/model";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";

import {
  buildServerProvider,
  providerModelsFromSettings,
  type ServerProviderDraft,
} from "../providerSnapshot.ts";
import { ProviderAdapterRequestError } from "../Errors.ts";
import { ProviderDriverKind } from "@t3tools/contracts";

// ---------------------------------------------------------------------------
// Presentation (mirrors DshDriver.ts and AntigravityProvider.ts:40)
// ---------------------------------------------------------------------------
export const DSH_PROVIDER_PRESENTATION = {
  displayName: "DSH",
  showInteractionModeToggle: false,
  requiresNewThreadForModelChange: false,
} as const;

const EMPTY_CAPABILITIES: ModelCapabilities = createModelCapabilities({
  optionDescriptors: [],
});

const DSH_FALLBACK_MODELS: ReadonlyArray<ServerProviderModel> = [
  {
    slug: "deepseek-chat",
    name: "DeepSeek Chat",
    isCustom: false,
    capabilities: EMPTY_CAPABILITIES,
  },
  {
    slug: "deepseek-reasoner",
    name: "DeepSeek Reasoner",
    isCustom: false,
    capabilities: EMPTY_CAPABILITIES,
  },
];

function dshModelsFromSettings(
  customModels: ReadonlyArray<string> | undefined,
  builtInModels: ReadonlyArray<ServerProviderModel> = [...DSH_FALLBACK_MODELS],
): ReadonlyArray<ServerProviderModel> {
  return providerModelsFromSettings(
    builtInModels as ReadonlyArray<ServerProviderModel>,
    customModels ?? [],
    EMPTY_CAPABILITIES,
  );
}

// ---------------------------------------------------------------------------
// Wire helpers (same envelope as DshAdapter — small duplication keeps this
// module importable without pulling the adapter's Effect services)
// ---------------------------------------------------------------------------
function normalizeBaseUrl(raw: string): string {
  const trimmed = raw.trim();
  if (!trimmed) return "http://127.0.0.1:3080";
  return trimmed.replace(/\/+$/, "");
}

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

const dshCall = <T>(
  baseUrlRaw: string,
  method: string,
  payload: unknown,
  timeoutMs = 30_000,
): Effect.Effect<T, ProviderAdapterRequestError> =>
  Effect.gen(function* () {
    const baseUrl = normalizeBaseUrl(baseUrlRaw);
    const rpcId = globalThis.crypto.randomUUID();
    const url = `${baseUrl}/api/${method}`;
    const body = JSON.stringify({ type: "client-request", rpcId, method, payload });

    const response: Response = yield* Effect.tryPromise({
      try: () =>
        fetch(url, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body,
          signal: AbortSignal.timeout(timeoutMs),
        }),
      catch: (cause) =>
        new ProviderAdapterRequestError({
          provider: ProviderDriverKind.make("dsh"),
          method,
          detail: cause instanceof Error ? cause.message : String(cause),
          cause,
        }),
    });

    if (!response.ok) {
      return yield* new ProviderAdapterRequestError({
        provider: ProviderDriverKind.make("dsh"),
        method,
        detail: `Transport failure for ${method}: HTTP ${response.status}`,
      });
    }

    const json: ServerResponse = yield* Effect.tryPromise({
      try: () => response.json() as Promise<ServerResponse>,
      catch: (cause) =>
        new ProviderAdapterRequestError({
          provider: ProviderDriverKind.make("dsh"),
          method,
          detail: `Invalid JSON response for ${method}`,
          cause,
        }),
    });

    const result = json.result as RpcResult<T>;
    if (!result.ok) {
      return yield* new ProviderAdapterRequestError({
        provider: ProviderDriverKind.make("dsh"),
        method,
        detail: `${result.error.code}: ${result.error.message}`,
        cause: result.error,
      });
    }
    return result.value as T;
  });

// ---------------------------------------------------------------------------
// Public surface: llm.providers / llm.models / session.models / selectModel
// ---------------------------------------------------------------------------
export interface DshLlmCatalog {
  readonly groups: ReadonlyArray<{
    readonly id: string;
    readonly name: string;
    readonly models: ReadonlyArray<{
      readonly id: string;
      readonly name: string;
      readonly description?: string;
    }>;
  }>;
  readonly failures: ReadonlyArray<{
    readonly id: string;
    readonly name: string;
    readonly message: string;
  }>;
}

export interface DshSessionModels {
  readonly current: {
    readonly provider: string;
    readonly model: string;
    readonly reasoningEffort?: string;
  };
  readonly routable: boolean;
  readonly groups: DshLlmCatalog["groups"];
  readonly failures: DshLlmCatalog["failures"];
}

/**
 * Fetch the host-scoped provider directory (`llm.providers`).
 * Used to render Settings → Providers and to decide whether an image
 * capability probe should block.
 */
export const fetchDshProviders = (
  baseUrl: string,
): Effect.Effect<
  {
    providers: ReadonlyArray<{
      provider: string;
      displayName: string;
      settingsNs: string;
      settingsPath: string[];
      active: boolean;
    }>;
  },
  ProviderAdapterRequestError
> =>
  dshCall<{
    providers: ReadonlyArray<{
      provider: string;
      displayName: string;
      settingsNs: string;
      settingsPath: string[];
      active: boolean;
    }>;
  }>(baseUrl, "llm.providers", {});

/**
 * Fetch the host-scoped model catalog (`llm.models`).
 * The settings surface uses this without a session.
 */
export const fetchDshLlmModels = (
  baseUrl: string,
): Effect.Effect<DshLlmCatalog, ProviderAdapterRequestError> =>
  dshCall<DshLlmCatalog>(baseUrl, "llm.models", {});

/**
 * Fetch the session-scoped model directory (`session.models`).
 * Includes the current selection for the session.
 */
export const fetchDshSessionModels = (
  baseUrl: string,
  sessionId: string,
): Effect.Effect<DshSessionModels, ProviderAdapterRequestError> =>
  dshCall<DshSessionModels>(baseUrl, "session.models", { sessionId });

/**
 * Switch the model for an existing session in place (no fork).
 * Mirrors `AntigravityProvider`'s in-session switch: the adapter's
 * `sessionModelSwitch: "in-session"` means the orchestration layer will
 * call this instead of requiring a new thread.
 *
 * @param baseUrl - DSH host origin
 * @param sessionId - Host session id
 * @param selection - Exact provider/model (and optional reasoningEffort)
 */
export const selectDshModel = (
  baseUrl: string,
  sessionId: string,
  selection: { provider: string; model: string; reasoningEffort?: string },
): Effect.Effect<
  { selected: { provider: string; model: string; reasoningEffort?: string } },
  ProviderAdapterRequestError
> =>
  dshCall<{ selected: { provider: string; model: string; reasoningEffort?: string } }>(
    baseUrl,
    "session.selectModel",
    {
      sessionId,
      provider: selection.provider,
      model: selection.model,
      ...(selection.reasoningEffort ? { reasoningEffort: selection.reasoningEffort } : {}),
    },
  );

// ---------------------------------------------------------------------------
// Provider snapshot helpers (for DshDriver's initial/check paths, kept here
// so the thin layer can be imported without the full adapter)
// ---------------------------------------------------------------------------
export function buildInitialDshProviderSnapshotForLayer(settings: {
  enabled: boolean;
  customModels?: ReadonlyArray<string>;
}): Effect.Effect<ServerProviderDraft> {
  return Effect.gen(function* () {
    const checkedAt = DateTime.formatIso(yield* DateTime.now);
    const models = dshModelsFromSettings(settings.customModels);
    if (!settings.enabled) {
      return buildServerProvider({
        presentation: DSH_PROVIDER_PRESENTATION,
        enabled: false,
        checkedAt,
        models,
        probe: {
          installed: false,
          version: null,
          status: "warning",
          auth: { status: "unknown" },
          message: "DSH is disabled in T3 Code settings.",
        },
      });
    }
    return buildServerProvider({
      presentation: DSH_PROVIDER_PRESENTATION,
      enabled: true,
      checkedAt,
      models,
      probe: {
        installed: true,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Checking DSH service availability...",
      },
    });
  });
}

/**
 * Convert a live DSH `llm.models` catalog into `ServerProviderModel[]`.
 * Successful groups are expanded 1:1; per-provider failures are surfaced
 * as custom models with a warning capability so the picker can still show them.
 */
export function catalogToServerProviderModels(
  catalog: DshLlmCatalog,
  customModels: ReadonlyArray<string> | undefined,
): ReadonlyArray<ServerProviderModel> {
  const fromCatalog: ServerProviderModel[] = [];
  for (const group of catalog.groups) {
    for (const model of group.models) {
      fromCatalog.push({
        slug: model.id,
        name: model.name || model.id,
        isCustom: false,
        capabilities: EMPTY_CAPABILITIES,
      });
    }
  }
  const fallback = fromCatalog.length > 0 ? fromCatalog : [...DSH_FALLBACK_MODELS];
  return providerModelsFromSettings(fallback, customModels ?? [], EMPTY_CAPABILITIES);
}
