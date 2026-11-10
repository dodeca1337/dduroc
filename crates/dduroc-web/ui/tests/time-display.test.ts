import { expect, it } from 'vitest';
import { bootTime, calendarTime, timeText } from '../src/lib/format';
import { defaultFilters, makeQuery } from '../src/lib/filters';
import type { Entry } from '../src/lib/types';
const record = (utc: string | null): Entry => ({ namespace: 'mixed', class: 'DEFAULT', at: { boot: '4294967295', micros: '9007199254740993' }, utc, span: null, kind: 'TEXT', text: { level: 'INFO', target: 'clock', text: 'Recorded time' } });

it('formats calendar UTC without losing microseconds or longer recorded fractions', () => {
  expect(calendarTime('2026-11-10T09:41:20.102461+00:00')).toBe('2026-11-10 09:41:20.102461 UTC');
  expect(calendarTime('2026-11-10T09:41:20.123456789Z')).toBe('2026-11-10 09:41:20.123456789 UTC');
  expect(calendarTime('2026-11-10T09:41:20Z')).toBe('2026-11-10 09:41:20.000000 UTC');
  expect(calendarTime('2026-11-10T09:41:20.001Z')).toBe('2026-11-10 09:41:20.001000 UTC');
  // An unexpected non-UTC value is retained verbatim, never relabelled as UTC.
  expect(calendarTime('2026-11-10T12:41:20.123456+03:00')).toBe('2026-11-10T12:41:20.123456+03:00');
});

it('defaults to recorded calendar time and marks exact boot-relative fallback explicitly', () => {
  expect(timeText(record('2026-11-10T09:41:20.102461Z'))).toBe('2026-11-10 09:41:20.102461 UTC');
  expect(timeText(record(null))).toBe('Boot 4294967295 · +9007199254.740993s · UTC unavailable');
  expect(timeText(record('2026-11-10T09:41:20.102461Z'), 'boot')).toBe(bootTime(record(null)));
});

it('requires RFC3339 seconds and forwards offsets and nanoseconds without conversion', () => {
  const filters = { ...defaultFilters(), clock: 'utc' as const, from: '2026-11-10T09:41:20.123456789+03:30', to: '2026-11-10T09:41:21.000000001-04:00' };
  expect(makeQuery(filters).from).toEqual({ utc: filters.from });
  expect(makeQuery(filters).to).toEqual({ utc: filters.to });
  expect(() => makeQuery({ ...filters, from: '2026-11-10T09:41Z' })).toThrow('RFC3339');
});
