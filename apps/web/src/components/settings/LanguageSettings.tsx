import { useTranslation } from "react-i18next";

import { SUPPORTED_LANGUAGES, setAppLanguage } from "../../i18n";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "../ui/select";
import { SettingsRow } from "./settingsLayout";

export function LanguageSettingsRow() {
  const { t, i18n } = useTranslation();
  const currentCode = i18n.language ?? "en";
  const currentLabel =
    SUPPORTED_LANGUAGES.find((language) => language.code === currentCode)?.label ??
    SUPPORTED_LANGUAGES[0]!.label;

  return (
    <SettingsRow
      id="setting-language"
      title={t("Language")}
      description={t("Interface display language.")}
      control={
        <Select
          value={currentCode}
          onValueChange={(next) => {
            if (typeof next === "string" && next.length > 0) {
              setAppLanguage(next);
            }
          }}
        >
          <SelectTrigger className="w-40" aria-label={t("Language")}>
            <SelectValue>{currentLabel}</SelectValue>
          </SelectTrigger>
          <SelectContent>
            {SUPPORTED_LANGUAGES.map((language) => (
              <SelectItem key={language.code} value={language.code}>
                {language.label}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      }
    />
  );
}
