import {
  WorkspaceBreadcrumb,
  WorkspaceBreadcrumbItem,
  WorkspaceBreadcrumbSeparator,
} from "../WorkspaceBreadcrumb";
import { SETTINGS_SECTION_LABELS } from "./settingsSearch";
import i18next from "i18next";

const SETTINGS_BREADCRUMB_LABELS: Readonly<Record<string, string>> = {
  ...SETTINGS_SECTION_LABELS,
  "/settings/diagnostics": i18next.t("Diagnostics"),
};

function settingsBreadcrumbLabel(pathname: string): string | null {
  const normalizedPathname = pathname.replace(/\/+$/, "") || "/";
  return SETTINGS_BREADCRUMB_LABELS[normalizedPathname] ?? null;
}

export function SettingsBreadcrumb({ pathname }: { pathname: string }) {
  const sectionLabel = settingsBreadcrumbLabel(pathname);

  return (
    <WorkspaceBreadcrumb ariaLabel={i18next.t("Settings breadcrumb")}>
      {sectionLabel ? (
        <>
          <WorkspaceBreadcrumbItem>{i18next.t("Settings")}</WorkspaceBreadcrumbItem>
          <WorkspaceBreadcrumbSeparator />
        </>
      ) : null}
      <WorkspaceBreadcrumbItem current className="truncate">
        {sectionLabel ? i18next.t(sectionLabel) : i18next.t("Settings")}
      </WorkspaceBreadcrumbItem>
    </WorkspaceBreadcrumb>
  );
}
