import type { ReactNode } from "react";

import { useTranslation } from "react-i18next";

import { SUPPORTED_LANGUAGES, setAppLanguage } from "../../i18n";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "../ui/select";

export function LanguageSettingsRow() {
  const { t, i18n } = useTranslation();

  return (
    <div id="setting-language" className="flex items-center justify-between gap-4 py-2">
      <span>{t("Language")}</span>
      <Select
        value={i18n.language ?? "en"}
        onValueChange={(next) => {
          if (typeof next === "string" && next.length > 0) {
            setAppLanguage(next);
          }
        }}
      >
        <SelectTrigger className="w-40" aria-label={t("Language")}>
          <SelectValue />
        </SelectTrigger>
        <SelectContent>
          {SUPPORTED_LANGUAGES.map((language) => (
            <SelectItem key={language.code} value={language.code}>
              {language.label}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
    </div>
  );
}

export type { ReactNode };
