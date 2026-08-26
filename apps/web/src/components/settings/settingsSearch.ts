import { isElectron } from "~/env";
import i18next from "i18next";

export type SettingsPath =
  | "/settings/general"
  | "/settings/appearance"
  | "/settings/keybindings"
  | "/settings/providers"
  | "/settings/integrations"
  | "/settings/source-control"
  | "/settings/connections"
  | "/settings/archived";

export interface SettingsSearchItem {
  readonly id: string;
  readonly title: string;
  readonly to: SettingsPath;
  readonly targetId?: string;
  // Its row only renders in the desktop app, so a browser result would land on
  // an anchor that isn't there.
  readonly desktopOnly?: boolean;
}

/**
 * Section labels in sidebar order. The sidebar nav and the search-result
 * subtitles both render from this record, so each label exists once.
 */
export const SETTINGS_SECTION_LABELS: Readonly<Record<SettingsPath, string>> = {
  "/settings/general": "General",
  "/settings/appearance": "Appearance",
  "/settings/keybindings": i18next.t("Keybindings"),
  "/settings/providers": i18next.t("Providers"),
  "/settings/integrations": "Integrations",
  "/settings/source-control": "Source Control",
  "/settings/connections": "Connections",
  "/settings/archived": "Archive",
};

/**
 * Every searchable setting, in result order. This catalog is the single
 * source of truth for anchor ids and visible titles: panels render both via
 * `searchableSetting`, so a retitle (or, later, a translation pass) happens
 * here once instead of separately in the panel and the index.
 */
export const SETTINGS_SEARCH_ITEMS = [
  {
    id: "color-scheme",
    title: i18next.t("Color scheme"),
    to: "/settings/appearance",
    // The scheme tiles sit at the top of the Appearance section.
    targetId: "appearance",
  },
  {
    id: "theme",
    title: i18next.t("Themes"),
    to: "/settings/appearance",
    // Theme cards live directly under the scheme tiles; the section is the
    // stable scroll destination for both.
    targetId: "appearance",
  },
  {
    // Prefixed because the slider control already owns the `appearance-contrast` id.
    id: "setting-appearance-contrast",
    title: i18next.t("Contrast"),
    to: "/settings/appearance",
  },
  {
    // Prefixed because the slider control already owns the `glass-opacity` id.
    id: "setting-glass-opacity",
    title: i18next.t("Glass opacity"),
    to: "/settings/appearance",
  },
  {
    id: "environment-identification",
    title: i18next.t("Environment identification"),
    to: "/settings/appearance",
    // The setting is stage-dependent, so its parent section is the stable destination.
    targetId: "appearance",
  },
  {
    id: "interface-font",
    title: i18next.t("Interface font"),
    to: "/settings/appearance",
  },
  {
    id: "prompt-font",
    title: i18next.t("Prompt font"),
    to: "/settings/appearance",
  },
  {
    id: "code-font",
    title: i18next.t("Code font"),
    to: "/settings/appearance",
  },
  {
    id: "terminal-font",
    title: i18next.t("Terminal font"),
    to: "/settings/appearance",
  },
  {
    id: "font-smoothing",
    title: i18next.t("Font smoothing"),
    to: "/settings/appearance",
  },
  {
    id: "word-wrap",
    title: i18next.t("Word wrap"),
    to: "/settings/appearance",
  },
  {
    id: "project-grouping",
    title: i18next.t("Project grouping"),
    to: "/settings/general",
  },
  {
    id: "auto-settle-inactive-threads",
    title: i18next.t("Auto-settle inactive threads"),
    to: "/settings/general",
  },
  {
    id: "auto-settle-merged-threads",
    title: i18next.t("Auto-settle merged threads"),
    to: "/settings/general",
  },
  {
    id: "time-format",
    title: i18next.t("Time format"),
    to: "/settings/general",
  },
  {
    id: "hide-whitespace-changes",
    title: i18next.t("Hide whitespace changes"),
    to: "/settings/general",
  },
  {
    id: "skills-in-slash-menu",
    title: i18next.t("Show skills in slash menu"),
    to: "/settings/general",
  },
  {
    id: "provider-update-checks",
    title: i18next.t("Provider update checks"),
    to: "/settings/general",
  },
  {
    id: "new-threads",
    title: i18next.t("New threads"),
    to: "/settings/general",
  },
  {
    id: "start-from-origin",
    title: i18next.t("Start from origin"),
    to: "/settings/general",
    targetId: "new-threads",
  },
  {
    id: "add-project-starts-in",
    title: i18next.t("Add project starts in"),
    to: "/settings/general",
  },
  {
    id: "archive-confirmation",
    title: i18next.t("Archive confirmation"),
    to: "/settings/general",
  },
  {
    id: "delete-confirmation",
    title: i18next.t("Delete confirmation"),
    to: "/settings/general",
  },
  {
    id: "quit-confirmation",
    title: i18next.t("Hold to quit"),
    to: "/settings/general",
    desktopOnly: true,
  },
  {
    id: "text-generation-model",
    title: i18next.t("Text generation model"),
    to: "/settings/general",
  },
  {
    id: "diagnostics",
    title: i18next.t("Diagnostics"),
    to: "/settings/general",
  },
  {
    id: "legacy-plan-mode",
    title: i18next.t("Plan mode (legacy)"),
    to: "/settings/general",
  },
  {
    id: "legacy-token-streaming",
    title: i18next.t("Stream token by token (legacy)"),
    to: "/settings/general",
  },
  {
    id: "legacy-sidebar",
    title: i18next.t("Sidebar (legacy)"),
    to: "/settings/general",
  },
  {
    id: "keybindings",
    title: i18next.t("Keybindings"),
    to: "/settings/keybindings",
  },
  {
    id: "providers",
    title: i18next.t("Providers"),
    to: "/settings/providers",
  },
  {
    id: "agent-browser-access",
    title: i18next.t("Agent browser access"),
    to: "/settings/integrations",
    targetId: "browser",
  },
  {
    id: "browser-default-viewport",
    title: i18next.t("Default browser viewport"),
    to: "/settings/integrations",
    targetId: "browser",
  },
  {
    id: "browser-default-zoom",
    title: i18next.t("Default browser zoom"),
    to: "/settings/integrations",
    targetId: "browser",
  },
  {
    id: "browser-default-appearance",
    title: i18next.t("Default browser appearance"),
    to: "/settings/integrations",
    targetId: "browser",
  },
  {
    id: "browser-auto-show-floating-preview",
    title: i18next.t("Auto-show floating preview"),
    to: "/settings/integrations",
    targetId: "browser",
  },
  {
    id: "source-control",
    title: i18next.t("Source control"),
    to: "/settings/source-control",
  },
  {
    id: "remote-environments",
    title: i18next.t("Remote environments"),
    to: "/settings/connections",
  },
  {
    id: "archive",
    title: i18next.t("Archived threads"),
    to: "/settings/archived",
  },
] as const satisfies ReadonlyArray<SettingsSearchItem>;

export type SettingsSearchItemId = (typeof SETTINGS_SEARCH_ITEMS)[number]["id"];

const SEARCH_ITEMS_BY_ID = Object.fromEntries(
  SETTINGS_SEARCH_ITEMS.map((item) => [item.id, item]),
) as Readonly<Record<SettingsSearchItemId, SettingsSearchItem>>;

/**
 * `id` and `title` props for the element a search item anchors to. Panels
 * spread (or pick from) this instead of restating the strings, so the catalog
 * and the rendered settings cannot drift apart.
 */
export function searchableSetting(id: SettingsSearchItemId): {
  readonly id: string;
  readonly title: string;
} {
  const { id: anchorId, title } = SEARCH_ITEMS_BY_ID[id];
  return { id: anchorId, title };
}

function normalizeSearchText(value: string): string {
  return value
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLocaleLowerCase()
    .replace(/\s+/g, " ")
    .trim();
}

export function searchSettings(
  query: string,
  items: ReadonlyArray<SettingsSearchItem> = SETTINGS_SEARCH_ITEMS,
): ReadonlyArray<SettingsSearchItem> {
  const normalizedQuery = normalizeSearchText(query);
  if (normalizedQuery.length === 0) return [];

  return items.filter(
    (item) =>
      (isElectron || item.desktopOnly !== true) &&
      normalizeSearchText(item.title).includes(normalizedQuery),
  );
}
