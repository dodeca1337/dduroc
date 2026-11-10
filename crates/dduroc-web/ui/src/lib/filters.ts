import type { Class, Level, QueryInput } from './types';
export interface Filters { namespaceMode: 'all' | 'exact' | 'prefix'; namespace: string; class: '' | Class; level: '' | Level; kind: 'all' | 'messages' | 'text' | 'samples' | 'spans'; clock: 'all' | 'utc' | 'boot'; from: string; to: string; boot: string; eventNames: string; tags: string; order: 'NEWEST' | 'OLDEST'; seedStates: boolean; metadataOnly: boolean }
export const defaultFilters = (): Filters => ({ namespaceMode: 'all', namespace: '', class: '', level: '', kind: 'all', clock: 'all', from: '', to: '', boot: '', eventNames: '', tags: '', order: 'NEWEST', seedStates: false, metadataOnly: false });
function unsigned(text: string, name: string, max: bigint): string { if (!/^(0|[1-9]\d*)$/.test(text) || BigInt(text) > max) throw new Error(`${name} must be a decimal integer within its range.`); return text; }
function list(text: string): string[] | undefined { const values = text.split(',').map(value => value.trim()).filter(Boolean); if (values.length > 32 || values.some(value => value.length > 256)) throw new Error('Use at most 32 names or tags, each no longer than 256 characters.'); return values.length ? values : undefined; }
function utc(text: string): string { if (!/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(\.\d{1,9})?(Z|[+-]\d\d:\d\d)$/.test(text)) throw new Error('Use an RFC3339 UTC timestamp, for example 2026-11-10T12:00:00Z.'); return text; }
export function makeQuery(filters: Filters, live = false): QueryInput {
  if (live && filters.clock !== 'all' && filters.to.trim()) throw new Error('Live has no end time. Clear the end time before starting a stream.');
  const query: QueryInput = { order: live ? 'OLDEST' : filters.order };
  const namespace = filters.namespace.trim();
  if (namespace.length > 256) throw new Error('Use a namespace or prefix up to 256 characters.');
  if (namespace) { if (filters.namespaceMode === 'prefix') query.namespacePrefix = namespace; else query.namespaces = [namespace]; }
  if (filters.class) query.classes = [filters.class];
  const filter: NonNullable<QueryInput['filter']> = {};
  if (filters.level) filter.minLevel = filters.level;
  filter.eventNames = list(filters.eventNames); filter.anyTags = list(filters.tags);
  if (filters.kind !== 'all') filter.kinds = { messages: filters.kind === 'messages', text: filters.kind === 'text', samples: filters.kind === 'samples', spans: filters.kind === 'spans' };
  if (filters.clock === 'utc') { if (filters.from) query.from = { utc: utc(filters.from) }; if (!live && filters.to) query.to = { utc: utc(filters.to) }; }
  if (filters.clock === 'boot') {
    const boot = unsigned(filters.boot, 'Boot', 4_294_967_295n); query.boot = boot;
    if (filters.from) query.from = { boot, micros: unsigned(filters.from, 'Start microseconds', 18_446_744_073_709_551_615n) };
    if (!live && filters.to) query.to = { boot, micros: unsigned(filters.to, 'End microseconds', 18_446_744_073_709_551_615n) };
    if (filters.from && filters.to && !live && BigInt(filters.from) > BigInt(filters.to)) throw new Error('The start time must not be later than the end time.');
  }
  if (filters.seedStates && query.from) query.seedStates = true;
  if (Object.values(filter).some(value => value !== undefined)) query.filter = filter;
  return query;
}
export function activeFilterCount(filters: Filters): number { return [!!filters.namespace.trim(), !!filters.class, !!filters.level, filters.kind !== 'all', filters.clock !== 'all', !!filters.eventNames.trim(), !!filters.tags.trim()].filter(Boolean).length; }
