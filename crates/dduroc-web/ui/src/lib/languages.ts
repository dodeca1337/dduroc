import type { Catalog, Entry, PresentationInput } from './types';

export interface MessageLanguages { primary: string; fallback: string }
export const LANGUAGE_STORAGE_KEY = 'dduroc.viewer.messageLanguages';
export const defaultMessageLanguages = (): MessageLanguages => ({ primary: '', fallback: '' });
const key = (value: string) => value.toLowerCase().replaceAll('_', '-');
const valid = (value: unknown): value is string => typeof value === 'string' && value.length <= 64 && (!value || /^[A-Za-z0-9_-]+$/.test(value));
export function loadMessageLanguages(): MessageLanguages {
  try {
    const value = JSON.parse(localStorage.getItem(LANGUAGE_STORAGE_KEY) ?? 'null');
    if (value && valid(value.primary) && valid(value.fallback)) return { primary: value.primary, fallback: value.primary && key(value.primary) !== key(value.fallback) ? value.fallback : '' };
  } catch { /* Unavailable or invalid browser storage does not block reading. */ }
  return defaultMessageLanguages();
}
export function saveMessageLanguages(value: MessageLanguages): void {
  try { localStorage.setItem(LANGUAGE_STORAGE_KEY, JSON.stringify(value)); } catch { /* Keep the selection for this page when persistence is unavailable. */ }
}
export function messagePresentation(value: MessageLanguages): PresentationInput | undefined {
  return value.primary ? { languages: value.fallback && key(value.fallback) !== key(value.primary) ? [value.primary, value.fallback] : [value.primary] } : undefined;
}
export function messageLanguageChoices(catalog: Catalog | null, selected: MessageLanguages): string[] {
  const values = [selected.primary, selected.fallback, ...(catalog?.defaultLanguages ?? []), ...(catalog?.schemas.flatMap(schema => schema.languages) ?? [])].filter(Boolean);
  return values.filter((value, index) => values.indexOf(value) === index).sort();
}
export function messageLanguageStatus(entry: Entry): string {
  const message = entry.message;
  if (!message) return '';
  if (message.renderStatus === 'UNKNOWN_SCHEMA') return 'Unknown schema · no rendered message';
  if (message.renderStatus === 'UNSUPPORTED_DECODER') return 'Message renderer unavailable';
  if (message.renderStatus !== 'RENDERED' || message.rendered == null) return 'Message rendering not requested';
  if (!message.language) return 'Rendered language unavailable';
  if (message.languagePreferenceIndex === undefined) return `${message.language} · preference unavailable`;
  if (message.languagePreferenceIndex === null) return `${message.language} · first schema language fallback`;
  if (message.languagePreferenceIndex === 0) return `${message.language} · primary preference`;
  return `${message.language} · fallback preference ${message.languagePreferenceIndex + 1}`;
}
export function messageFallbackBadge(entry: Entry): string {
  const message = entry.message;
  return message?.renderStatus === 'RENDERED' && message.rendered != null && message.language && (message.languagePreferenceIndex === null || (message.languagePreferenceIndex ?? 0) > 0) ? `${message.language} · fallback` : '';
}
