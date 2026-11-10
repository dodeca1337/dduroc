import type { Entry, FloatValue, ValueView } from './types';
export function decimal(value: unknown): string {
  if (typeof value !== 'string' || !/^(0|[1-9]\d*)$/.test(value)) throw new Error('Invalid decimal string from server');
  return value;
}
export function bootTime(entry: Pick<Entry, 'at'>): string {
  const micros = BigInt(decimal(entry.at.micros));
  return `Boot ${decimal(entry.at.boot)} · +${micros / 1_000_000n}.${(micros % 1_000_000n).toString().padStart(6, '0')}s`;
}
export type TimeDisplay = 'calendar' | 'boot';
// Entry.utc is the native UTC projection. String formatting preserves every
// fractional digit; Date would round the recorded value to milliseconds.
export function calendarTime(utc: string): string {
  const parts = /^([+-]?\d{4,}-\d{2}-\d{2})T(\d{2}:\d{2}:\d{2})(?:\.(\d+))?(?:Z|\+00:00)$/.exec(utc);
  return parts ? `${parts[1]} ${parts[2]}.${(parts[3] ?? '').padEnd(6, '0')} UTC` : utc;
}
export function timeText(entry: Entry, display: TimeDisplay = 'calendar'): string {
  if (display === 'boot') return bootTime(entry);
  return entry.utc ? calendarTime(entry.utc) : `${bootTime(entry)} · UTC unavailable`;
}
export function floatText(value: FloatValue): string {
  if (value.class === 'NAN') return 'NaN';
  if (value.class === 'POSITIVE_INFINITY') return '+Infinity';
  if (value.class === 'NEGATIVE_INFINITY') return '−Infinity';
  if (/^80000000(00000000)?$/.test(value.bits)) return '−0';
  return value.decimal;
}
export function scalarText(value?: ValueView | null): string {
  if (!value) return 'Not requested';
  switch (value.kind) {
    case 'NULL': return 'null';
    case 'BOOLEAN': return value.boolean ? 'true' : 'false';
    case 'FLOAT': return value.float ? floatText(value.float) : 'Unknown float';
    case 'BYTES': return value.bytes ? `${value.bytes.bytes} bytes` : 'Unknown bytes';
    case 'SEQUENCE': return value.items ? `[${value.items.length} values]` : 'More nested fields';
    case 'MAP': return value.pairs ? `{${value.pairs.length} fields}` : 'More nested fields';
    case 'VARIANT': return value.variant ?? 'Variant';
    default: return value.text ?? 'Unknown value';
  }
}
export function entrySummary(entry: Entry): string {
  if (entry.message) return entry.message.rendered ?? entry.message.name ?? `Unknown event ${entry.message.eventId}`;
  if (entry.text) return entry.text.text || entry.text.target || 'Free text';
  if (entry.sample) return `${entry.sample.name ?? `Metric ${entry.sample.metricId}`}: ${entry.sample.stateName ?? scalarText(entry.sample.value)}${entry.sample.unit ? ` ${entry.sample.unit}` : ''}`;
  if (entry.spanEvent) return `${entry.kind === 'SPAN_END' ? 'End' : 'Start'} ${entry.spanEvent.kindName ?? `span ${entry.spanEvent.id}`}`;
  return `Extension · ${entry.extension?.bytes ?? 'unknown'} bytes`;
}
export function entryLevel(entry: Entry): string { return entry.message?.level ?? entry.text?.level ?? entry.sample?.severity?.toUpperCase() ?? '—'; }
export function kindLabel(entry: Entry): string { return ({ MESSAGE: 'Message', TEXT: 'Text', SPAN_START: 'Span start', SPAN_END: 'Span end', SAMPLE: 'Sample', EXTENSION: 'Extension' })[entry.kind]; }
