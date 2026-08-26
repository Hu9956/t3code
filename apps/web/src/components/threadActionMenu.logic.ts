import type { ContextMenuItem } from "@t3tools/contracts";
import type { SnoozePreset } from "@t3tools/client-runtime/state/thread-settled";
import i18next from "i18next";

/**
 * Ids for the per-thread action menu. Snooze presets are dispatched as
 * `snooze:<presetId>` so the union stays closed while the preset list
 * remains data-driven.
 */
export type ThreadActionMenuId =
  | "new-thread-on-branch"
  | "pin"
  | "unpin"
  | "settle"
  | "unsettle"
  | "snooze"
  | `snooze:${string}`
  | "unsnooze"
  | "rename"
  | "regenerate-title"
  | "mark-unread"
  | "copy"
  | "copy-path"
  | "copy-branch"
  | "copy-thread-id"
  | "archive"
  | "delete";

export interface ThreadActionMenuState {
  readonly branch: string | null;
  readonly isPinned: boolean;
  readonly isSettled: boolean;
  readonly isSnoozed: boolean;
  readonly canSnoozeNow: boolean;
  readonly isRegeneratingTitle: boolean;
  /** Archive rejects a thread with an active turn, so disable it here rather than let the action fail. */
  readonly isRunning: boolean;
  readonly supports: {
    readonly settlement: boolean;
    readonly snooze: boolean;
    readonly pinning: boolean;
    readonly titleRegeneration: boolean;
  };
  readonly snoozePresets: ReadonlyArray<SnoozePreset>;
}

/**
 * Single source for the per-thread action menu: the sidebar row's right-click
 * menu and the chat header menu both render exactly this list, so labels,
 * ordering, and capability gating cannot drift between the two surfaces.
 */
export function buildThreadActionMenuItems(
  state: ThreadActionMenuState,
): ReadonlyArray<ContextMenuItem<ThreadActionMenuId>> {
  return [
    ...(state.branch
      ? [
          {
            id: "new-thread-on-branch" as const,
            label: `New thread on ${state.branch}`,
            icon: "message-square-plus",
          },
        ]
      : []),
    ...(state.supports.pinning
      ? [
          state.isPinned
            ? { id: "unpin" as const, label: i18next.t("Unpin thread"), icon: "pin-off" }
            : { id: "pin" as const, label: i18next.t("Pin thread"), icon: "pin" },
        ]
      : []),
    // Both lifecycle actions stay available on pinned threads: settling
    // clears the pin ("done" beats "keep on top"), and snoozing hides the
    // card until wake with the pin intact.
    ...(state.supports.settlement
      ? [
          state.isSettled
            ? {
                id: "unsettle" as const,
                label: i18next.t("Un-settle thread"),
                icon: "circle-check",
              }
            : { id: "settle" as const, label: i18next.t("Settle thread"), icon: "circle-check" },
        ]
      : []),
    ...(state.supports.snooze
      ? [
          state.isSnoozed
            ? { id: "unsnooze" as const, label: i18next.t("Wake thread"), icon: "clock" }
            : {
                id: "snooze" as const,
                label: i18next.t("Snooze"),
                icon: "clock",
                disabled: !state.canSnoozeNow,
                children: state.snoozePresets.map((preset) => ({
                  id: `snooze:${preset.id}` as const,
                  label: `${preset.label} (${preset.whenLabel})`,
                })),
              },
        ]
      : []),
    { id: "rename", label: i18next.t("Rename thread"), icon: "pencil", separatorBefore: true },
    ...(state.supports.titleRegeneration
      ? [
          {
            id: "regenerate-title" as const,
            label: state.isRegeneratingTitle ? "Regenerating…" : "Regenerate title",
            icon: "refresh-cw",
            disabled: state.isRegeneratingTitle,
          },
        ]
      : []),
    { id: "mark-unread", label: i18next.t("Mark unread"), icon: "mail-open" },
    {
      id: "copy",
      label: i18next.t("Copy"),
      icon: "copy",
      separatorBefore: true,
      children: [
        { id: "copy-path", label: i18next.t("Path"), icon: "folder" },
        ...(state.branch
          ? [{ id: "copy-branch" as const, label: i18next.t("Branch"), icon: "git-branch" }]
          : []),
        { id: "copy-thread-id", label: i18next.t("Thread ID"), icon: "hash" },
      ],
    },
    // Archive removes the thread from the sidebar while keeping its
    // conversation under Settings > Archived threads — distinct from Settle
    // (stays visible in the Settled shelf) and Delete (clears history for
    // good), so it sits beside Delete without borrowing its destructive
    // styling.
    {
      id: "archive",
      label: i18next.t("Archive thread"),
      icon: "archive",
      disabled: state.isRunning,
      separatorBefore: true,
    },
    {
      id: "delete",
      label: i18next.t("Delete"),
      destructive: true,
      icon: "trash",
    },
  ];
}
