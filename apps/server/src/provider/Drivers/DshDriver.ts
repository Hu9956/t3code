// @effect-diagnostics globalFetchInEffect:off
// @effect-diagnostics globalFetch:off
// @effect-diagnostics cryptoRandomUUIDInEffect:off
// @effect-diagnostics preferSchemaOverJson:off
// @effect-diagnostics anyUnknownInErrorContext:off
// @effect-diagnostics unknownInEffectCatch:off
// @effect-diagnostics globalErrorInEffectCatch:off
// @effect-diagnostics globalErrorInEffectFailure:off
// @effect-diagnostics missingEffectError:off
// @effect-diagnostics missingEffectContext:off
/**
 * DshDriver — `ProviderDriver` for the DSH host service.
 *
 * Health check is a real HTTP probe:
 *   POST http://127.0.0.1:3080/api/host.describe
 *   Content-Type: application/json
 *   body: { type:"client-request", rpcId:crypto.randomUUID(), method:"host.describe", payload:{} }
 *   5s timeout via AbortSignal.timeout.
 *
 * ServerResponse mapping:
 *   200 && result.ok===true → ready
 *   ECONNREFUSED/ENOTFOUND/fetch failed → not-installed
 *   403 → auth
 *   other → error
 *
 * The adapter is a placeholder (Task 3 fills it) — a minimal
 * `ProviderAdapterShape` that typechecks.
 *
 * @module provider/Drivers/DshDriver
 */
import { DshSettings, ProviderDriverKind, type ServerProvider } from "@t3tools/contracts";
import i18next from "i18next";
import { TextGenerationError } from "@t3tools/contracts";
import { createModelCapabilities } from "@t3tools/shared/model";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import * as BackgroundPolicy from "../../background/BackgroundPolicy.ts";
import { ServerConfig } from "../../config.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import {
  ProviderAdapterRequestError,
  ProviderAdapterValidationError,
  type ProviderAdapterError,
  ProviderDriverError,
} from "../Errors.ts";
import { makeManagedServerProvider } from "../makeManagedServerProvider.ts";
import {
  defaultProviderContinuationIdentity,
  type ProviderDriver,
  type ProviderInstance,
} from "../ProviderDriver.ts";
import type { ServerProviderDraft } from "../providerSnapshot.ts";
import { buildServerProvider, providerModelsFromSettings } from "../providerSnapshot.ts";
import { mergeProviderInstanceEnvironment } from "../ProviderInstanceEnvironment.ts";
import {
  makeManualOnlyProviderMaintenanceCapabilities,
  makeStaticProviderMaintenanceResolver,
  resolveProviderMaintenanceCapabilitiesEffect,
} from "../providerMaintenance.ts";
import {
  haveProviderSnapshotSettingsChanged,
  makeProviderSnapshotSettingsSource,
  type ProviderSnapshotSettings,
} from "../providerUpdateSettings.ts";
import type { ProviderAdapterShape } from "../Services/ProviderAdapter.ts";
import type * as TextGeneration from "../../textGeneration/TextGeneration.ts";
import { makeDshAdapter } from "../Layers/DshAdapter.ts";

const decodeDshSettings = Schema.decodeSync(DshSettings);

export const DRIVER_KIND = ProviderDriverKind.make("dsh");

const UPDATE = makeStaticProviderMaintenanceResolver(
  makeManualOnlyProviderMaintenanceCapabilities({
    provider: DRIVER_KIND,
    packageName: null,
  }),
);

export type DshDriverEnv =
  | BackgroundPolicy.BackgroundPolicy
  | FileSystem.FileSystem
  | Path.Path
  | ServerConfig
  | ServerSettingsService;

const DSH_PRESENTATION = {
  displayName: "DSH",
  showInteractionModeToggle: false,
  requiresNewThreadForModelChange: false,
} as const;

const EMPTY_CAPABILITIES = createModelCapabilities({
  optionDescriptors: [],
});

const DSH_BUILT_IN_MODELS = [
  {
    slug: "deepseek-v4-flash",
    name: "DeepSeek V4 Flash",
    isCustom: false,
    capabilities: EMPTY_CAPABILITIES,
  },
] as const;

function dshModelsFromSettings(
  customModels: ReadonlyArray<string> | undefined,
  builtInModels: ReadonlyArray<(typeof DSH_BUILT_IN_MODELS)[number]> = [...DSH_BUILT_IN_MODELS],
): ReturnType<typeof providerModelsFromSettings> {
  return providerModelsFromSettings(
    [...builtInModels] as any,
    customModels ?? [],
    EMPTY_CAPABILITIES,
  );
}

function resolveBaseUrl(settings: DshSettings): string {
  const raw = settings.baseUrl?.trim() || "http://127.0.0.1:3080";
  return raw.replace(/\/+$/, "");
}

function isNotInstalledFetchError(error: unknown): boolean {
  const message =
    error instanceof Error
      ? error.message
      : typeof error === "string"
        ? error
        : String(error ?? "");
  const lower = message.toLowerCase();
  if (
    lower.includes("econnrefused") ||
    lower.includes("enotfound") ||
    lower.includes("econnreset") ||
    lower.includes("enetunreach") ||
    lower.includes("fetch failed") ||
    lower.includes("failed to fetch") ||
    lower.includes("networkerror") ||
    lower.includes("getaddrinfo")
  ) {
    return true;
  }
  if (error && typeof error === "object" && "cause" in (error as any)) {
    const cause = (error as any).cause;
    if (cause && typeof cause === "object") {
      const code = (cause as any).code;
      if (typeof code === "string") {
        const lc = code.toLowerCase();
        if (
          lc.includes("econnrefused") ||
          lc.includes("enotfound") ||
          lc.includes("econnreset") ||
          lc.includes("enetunreach")
        ) {
          return true;
        }
      }
      const causeMessage =
        typeof (cause as any).message === "string" ? (cause as any).message : String(cause);
      const cl = causeMessage.toLowerCase();
      if (cl.includes("econnrefused") || cl.includes("enotfound") || cl.includes("fetch failed")) {
        return true;
      }
    }
  }
  return false;
}

function isTimeoutError(error: unknown): boolean {
  if (error instanceof DOMException && error.name === "TimeoutError") return true;
  if (error instanceof Error && error.name === "TimeoutError") return true;
  if (error instanceof DOMException && error.name === "AbortError") return true;
  if (error instanceof Error && error.name === "AbortError") return true;
  const msg = error instanceof Error ? error.message.toLowerCase() : String(error).toLowerCase();
  if (msg.includes("timeout") || msg.includes("aborted") || msg.includes("signal timed out")) {
    return true;
  }
  if (error && typeof error === "object" && "cause" in (error as any)) {
    const cause = (error as any).cause;
    if (
      cause instanceof DOMException &&
      (cause.name === "TimeoutError" || cause.name === "AbortError")
    ) {
      return true;
    }
    if (cause instanceof Error && (cause.name === "TimeoutError" || cause.name === "AbortError")) {
      return true;
    }
  }
  return false;
}

export function buildInitialDshProviderSnapshot(
  settings: DshSettings,
): Effect.Effect<ServerProviderDraft> {
  return Effect.gen(function* () {
    const checkedAt = DateTime.formatIso(yield* DateTime.now);
    const models = dshModelsFromSettings(settings.customModels);

    if (!settings.enabled) {
      return buildServerProvider({
        presentation: DSH_PRESENTATION,
        enabled: false,
        checkedAt,
        models,
        probe: {
          installed: false,
          version: null,
          status: "warning",
          auth: { status: "unknown" },
          message: i18next.t("DSH is disabled in T3 Code settings."),
        },
      });
    }

    return buildServerProvider({
      presentation: DSH_PRESENTATION,
      enabled: true,
      checkedAt,
      models,
      probe: {
        installed: true,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: i18next.t("Checking DSH service availability..."),
      },
    });
  });
}

export const checkDshProviderStatus = (
  settings: DshSettings,
): Effect.Effect<ServerProviderDraft, never> =>
  Effect.gen(function* () {
    const checkedAt = DateTime.formatIso(yield* DateTime.now);
    const fallbackModels = dshModelsFromSettings(settings.customModels);

    if (!settings.enabled) {
      return buildServerProvider({
        presentation: DSH_PRESENTATION,
        enabled: false,
        checkedAt,
        models: fallbackModels,
        probe: {
          installed: false,
          version: null,
          status: "warning",
          auth: { status: "unknown" },
          message: i18next.t("DSH is disabled in T3 Code settings."),
        },
      });
    }

    const baseUrl = resolveBaseUrl(settings);
    const url = `${baseUrl}/api/host.describe`;
    const body = JSON.stringify({
      type: "client-request",
      rpcId: globalThis.crypto.randomUUID(),
      method: "host.describe",
      payload: {},
    });

    const fetchResult = yield* Effect.tryPromise({
      try: () =>
        fetch(url, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body,
          signal: AbortSignal.timeout(5000),
        }),
      catch: (cause) => (cause instanceof Error ? cause : new Error(String(cause))),
    }).pipe(
      Effect.map((res) => ({ _tag: "ok" as const, res })),
      Effect.catch((cause) => Effect.succeed({ _tag: "err" as const, cause })),
    );

    if (fetchResult._tag === "err") {
      const cause = fetchResult.cause;
      if (isTimeoutError(cause)) {
        return buildServerProvider({
          presentation: DSH_PRESENTATION,
          enabled: settings.enabled,
          checkedAt,
          models: fallbackModels,
          probe: {
            installed: true,
            version: null,
            status: "error",
            auth: { status: "unknown" },
            message: i18next.t("DSH service timed out after 5s."),
          },
        });
      }
      if (isNotInstalledFetchError(cause)) {
        return buildServerProvider({
          presentation: DSH_PRESENTATION,
          enabled: settings.enabled,
          checkedAt,
          models: fallbackModels,
          probe: {
            installed: false,
            version: null,
            status: "error",
            auth: { status: "unknown" },
            message: i18next.t("DSH service is not installed or not running on {{baseUrl}}.", {
              baseUrl,
            }),
          },
        });
      }
      return buildServerProvider({
        presentation: DSH_PRESENTATION,
        enabled: settings.enabled,
        checkedAt,
        models: fallbackModels,
        probe: {
          installed: true,
          version: null,
          status: "error",
          auth: { status: "unknown" },
          message: i18next.t("Failed to reach DSH service: {{message}}", {
            message: cause.message,
          }),
        },
      });
    }

    const response: Response = fetchResult.res;

    if (response.status === 403) {
      return buildServerProvider({
        presentation: DSH_PRESENTATION,
        enabled: settings.enabled,
        checkedAt,
        models: fallbackModels,
        probe: {
          installed: true,
          version: null,
          status: "warning",
          auth: { status: "unauthenticated" },
          message: i18next.t("DSH service requires authentication (403)."),
        },
      });
    }

    if (!response.ok) {
      const detail = `DSH service returned HTTP ${response.status}.`;
      return buildServerProvider({
        presentation: DSH_PRESENTATION,
        enabled: settings.enabled,
        checkedAt,
        models: fallbackModels,
        probe: {
          installed: true,
          version: null,
          status: "error",
          auth: { status: "unknown" },
          message: detail,
        },
      });
    }

    const jsonResult = yield* Effect.tryPromise({
      try: () => response.json() as Promise<unknown>,
      catch: (cause) => (cause instanceof Error ? cause : new Error(String(cause))),
    }).pipe(
      Effect.map((data) => ({ _tag: "ok" as const, data })),
      Effect.catch((cause) => Effect.succeed({ _tag: "err" as const, cause } as const)),
    );

    if (jsonResult._tag === "err") {
      return buildServerProvider({
        presentation: DSH_PRESENTATION,
        enabled: settings.enabled,
        checkedAt,
        models: fallbackModels,
        probe: {
          installed: true,
          version: null,
          status: "error",
          auth: { status: "unknown" },
          message: i18next.t("DSH service returned an invalid response."),
        },
      });
    }

    const data = jsonResult.data;
    if (data === null || typeof data !== "object") {
      return buildServerProvider({
        presentation: DSH_PRESENTATION,
        enabled: settings.enabled,
        checkedAt,
        models: fallbackModels,
        probe: {
          installed: true,
          version: null,
          status: "error",
          auth: { status: "unknown" },
          message: i18next.t("DSH service returned an invalid response."),
        },
      });
    }

    const record = data as Record<string, unknown>;
    const resultField = record["result"] as unknown;
    let ok = false;
    if (resultField && typeof resultField === "object") {
      const r = resultField as Record<string, unknown>;
      if (r["ok"] === true) ok = true;
    }
    if (!ok && record["ok"] === true) {
      if (resultField === undefined) ok = true;
    }

    if (ok) {
      return buildServerProvider({
        presentation: DSH_PRESENTATION,
        enabled: settings.enabled,
        checkedAt,
        models: fallbackModels,
        probe: {
          installed: true,
          version: null,
          status: "ready",
          auth: { status: "authenticated" },
        },
      });
    }

    const messageFromResult = (() => {
      if (resultField && typeof resultField === "object") {
        const r = resultField as Record<string, unknown>;
        if (typeof r["error"] === "string" && (r["error"] as string).trim()) {
          return (r["error"] as string).trim();
        }
        if (typeof r["message"] === "string" && (r["message"] as string).trim()) {
          return (r["message"] as string).trim();
        }
      }
      if (typeof record["error"] === "string" && (record["error"] as string).trim()) {
        return (record["error"] as string).trim();
      }
      if (typeof record["message"] === "string" && (record["message"] as string).trim()) {
        return (record["message"] as string).trim();
      }
      return "DSH service returned an error.";
    })();

    return buildServerProvider({
      presentation: DSH_PRESENTATION,
      enabled: settings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: true,
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: messageFromResult,
      },
    });
  });

// ── Real adapter wiring ────────────────────────────────────────────────

export interface DshAdapterShape extends ProviderAdapterShape<ProviderAdapterError> {}

const makeDshAdapterPlaceholder = (baseUrl: string): Effect.Effect<DshAdapterShape> =>
  makeDshAdapter(baseUrl) as unknown as Effect.Effect<DshAdapterShape>;

const makeDshTextGeneration = (): Effect.Effect<TextGeneration.TextGeneration["Service"]> =>
  Effect.succeed({
    generateCommitMessage: () =>
      Effect.fail(
        new TextGenerationError({
          operation: "generateCommitMessage",
          detail: "DSH text generation not yet implemented.",
        }),
      ),
    generatePrContent: () =>
      Effect.fail(
        new TextGenerationError({
          operation: "generatePrContent",
          detail: "DSH text generation not yet implemented.",
        }),
      ),
    generateBranchName: () =>
      Effect.fail(
        new TextGenerationError({
          operation: "generateBranchName",
          detail: "DSH text generation not yet implemented.",
        }),
      ),
    generateThreadTitle: () =>
      Effect.fail(
        new TextGenerationError({
          operation: "generateThreadTitle",
          detail: "DSH text generation not yet implemented.",
        }),
      ),
  } as TextGeneration.TextGeneration["Service"]);

const withInstanceIdentity =
  (input: {
    readonly instanceId: ProviderInstance["instanceId"];
    readonly displayName: string | undefined;
    readonly accentColor: string | undefined;
    readonly continuationGroupKey: string;
  }) =>
  (snapshot: ServerProviderDraft): ServerProvider => ({
    ...snapshot,
    instanceId: input.instanceId,
    driver: DRIVER_KIND,
    ...(input.displayName ? { displayName: input.displayName } : {}),
    ...(input.accentColor ? { accentColor: input.accentColor } : {}),
    continuation: { groupKey: input.continuationGroupKey },
  });

export const DshDriver: ProviderDriver<DshSettings, DshDriverEnv> = {
  driverKind: DRIVER_KIND,
  metadata: {
    displayName: "DSH",
    supportsMultipleInstances: true,
  },
  configSchema: DshSettings,
  defaultConfig: (): DshSettings => decodeDshSettings({}),
  create: ({ instanceId, displayName, accentColor, environment, enabled, config }) =>
    Effect.gen(function* () {
      const serverSettings = yield* ServerSettingsService;
      const processEnv = mergeProviderInstanceEnvironment(environment);
      const continuationIdentity = defaultProviderContinuationIdentity({
        driverKind: DRIVER_KIND,
        instanceId,
      });
      const stampIdentity = withInstanceIdentity({
        instanceId,
        displayName,
        accentColor,
        continuationGroupKey: continuationIdentity.continuationKey,
      });
      const effectiveConfig = { ...config, enabled } satisfies DshSettings;
      const baseUrl = resolveBaseUrl(effectiveConfig);

      const maintenanceCapabilities = yield* resolveProviderMaintenanceCapabilitiesEffect(UPDATE, {
        binaryPath: null,
        env: processEnv,
      });

      const adapter = yield* makeDshAdapter(baseUrl);
      const textGeneration = yield* makeDshTextGeneration();

      const checkProvider = checkDshProviderStatus(effectiveConfig).pipe(Effect.map(stampIdentity));

      const snapshotSettings = makeProviderSnapshotSettingsSource(effectiveConfig, serverSettings);
      const snapshot = yield* makeManagedServerProvider<ProviderSnapshotSettings<DshSettings>>({
        maintenanceCapabilities,
        getSettings: snapshotSettings.getSettings,
        streamSettings: snapshotSettings.streamSettings,
        haveSettingsChanged: haveProviderSnapshotSettingsChanged,
        initialSnapshot: (settings) =>
          buildInitialDshProviderSnapshot(settings.provider).pipe(Effect.map(stampIdentity)),
        checkProvider,
      }).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderDriverError({
              driver: DRIVER_KIND,
              instanceId,
              detail: `Failed to build DSH snapshot: ${cause.message ?? String(cause)}`,
              cause,
            }),
        ),
      );

      return {
        instanceId,
        driverKind: DRIVER_KIND,
        continuationIdentity,
        displayName,
        accentColor,
        enabled,
        snapshot,
        adapter,
        textGeneration,
      } satisfies ProviderInstance;
    }),
};
