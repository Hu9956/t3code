import type { ServerProvider, ServerProviderVersionAdvisory } from "@t3tools/contracts";
import i18next from "i18next";

/**
 * Visual treatment for each server-reported provider status. Centralized so
 * the default-driver card and per-instance cards share the same language.
 */
export const PROVIDER_STATUS_STYLES = {
  disabled: {
    dot: "bg-amber-400",
  },
  error: {
    dot: "bg-destructive",
  },
  ready: {
    dot: "bg-success",
  },
  warning: {
    dot: "bg-warning",
  },
} as const;

export type ProviderStatusKey = keyof typeof PROVIDER_STATUS_STYLES;

/**
 * Derive the headline + detail copy shown under a provider's name in the
 * settings page. Prefers `provider.message` for server-supplied detail and
 * falls back to generic phrasing when the server has not yet reported any
 * state — which happens before the first probe or when an instance names a
 * driver this build does not ship.
 */
export function getProviderSummary(provider: ServerProvider | undefined) {
  if (!provider) {
    return {
      headline: i18next.t("Checking provider status"),
      detail: i18next.t(
        "Waiting for the server to report installation and authentication details.",
      ),
    };
  }
  if (!provider.enabled) {
    return {
      headline: i18next.t("Disabled"),
      detail: provider.message
        ? i18next.t(provider.message)
        : i18next.t("This provider is installed but disabled for new sessions in T3 Code."),
    };
  }
  if (!provider.installed) {
    return {
      headline: i18next.t("Not found"),
      detail: provider.message
        ? i18next.t(provider.message)
        : i18next.t("CLI not detected on PATH."),
    };
  }
  if (provider.auth.status === "authenticated") {
    const authLabel = provider.auth.label ?? provider.auth.type;
    return {
      headline: authLabel
        ? i18next.t("Authenticated · {{label}}", { label: authLabel })
        : i18next.t("Authenticated"),
      detail: provider.message ? i18next.t(provider.message) : null,
    };
  }
  if (provider.auth.status === "unauthenticated") {
    return {
      headline: i18next.t("Not authenticated"),
      detail: provider.message ? i18next.t(provider.message) : null,
    };
  }
  if (provider.status === "warning") {
    return {
      headline: i18next.t("Needs attention"),
      detail: provider.message
        ? i18next.t(provider.message)
        : i18next.t("The provider is installed, but the server could not fully verify it."),
    };
  }
  if (provider.status === "error") {
    return {
      headline: i18next.t("Unavailable"),
      detail: provider.message
        ? i18next.t(provider.message)
        : i18next.t("The provider failed its startup checks."),
    };
  }
  return {
    headline: i18next.t("Available"),
    detail:
      provider.message ??
      i18next.t("Installed and ready, but authentication could not be verified."),
  };
}

/**
 * Normalize a version string for display. Adds the `v` prefix when the
 * driver reported a bare version (e.g. `1.2.3`) so cards render
 * consistently regardless of driver.
 */
export function getProviderVersionLabel(version: string | null | undefined) {
  if (!version) return null;
  return version.startsWith("v") ? version : `v${version}`;
}

export function getProviderVersionAdvisoryPresentation(
  advisory: ServerProviderVersionAdvisory | undefined,
): {
  readonly detail: string;
  readonly updateCommand: string | null;
  readonly emphasis: "normal" | "strong";
} | null {
  if (!advisory || advisory.status === "current" || advisory.status === "unknown") {
    return null;
  }

  const label = i18next.t("Update available");
  const version = advisory.latestVersion;
  const versionLabel = getProviderVersionLabel(version);

  return {
    detail:
      advisory.message ??
      (versionLabel
        ? `${label}: install ${versionLabel}.`
        : `${label}: install the latest provider version.`),
    updateCommand: advisory.updateCommand,
    emphasis: "normal" as const,
  };
}
