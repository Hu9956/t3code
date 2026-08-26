import i18next from "i18next";
import { initReactI18next } from "react-i18next";

import zhCN from "./locales/zh-CN.json";

/**
 * Runtime-only i18n. Natural keys: the English source string is the lookup key,
 * so any missing translation falls back to readable English instead of a key name.
 */
export const SUPPORTED_LANGUAGES = [
  { code: "zh-CN", label: "简体中文" },
  { code: "en", label: i18next.t("English") },
] as const;

function detectInitialLanguage(): string {
  try {
    const stored = localStorage.getItem("t3.language");
    if (stored) return stored;
  } catch {
    // localStorage unavailable (non-browser context) — fall through
  }
  return navigator.language?.toLowerCase().startsWith("zh") ? "zh-CN" : "en";
}

void i18next.use(initReactI18next).init({
  resources: {
    "zh-CN": { translation: zhCN },
  },
  lng: detectInitialLanguage(),
  fallbackLng: "en",
  interpolation: {
    // React already escapes output
    escapeValue: false,
  },
  returnNull: false,
});

export function setAppLanguage(language: string): void {
  localStorage.setItem("t3.language", language);
  void i18next.changeLanguage(language);
}

export default i18next;
