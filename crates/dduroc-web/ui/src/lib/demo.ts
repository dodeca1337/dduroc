// Explicit ?demo=1 preview only. Never used as a fallback for a real connection.
import { ApiError } from './api';
import type { Batch, Catalog, StorageStatistics, Entry, Page, QueryInput, PresentationInput, Session, SessionPage, SeekPage, CountedSession, Transport, ValueView } from './types';
const text = (value: string): ValueView => ({ kind: 'STRING', text: value });
const fields = (values: Record<string, ValueView>): ValueView => ({ kind: 'MAP', pairs: Object.entries(values).map(([key, value]) => ({ key: text(key), value })) });
function entry(index: number, value: Partial<Entry>): Entry {
  return { namespace: 'control.main', class: 'DEFAULT', at: { boot: '7', micros: (431_102_460n - BigInt(index) * 120_000n).toString() }, utc: `2026-11-10T09:41:${String(20 - Math.floor(index / 5)).padStart(2, '0')}.${String(102460 + (index % 5) * 120000).padStart(6, '0')}Z`, span: null, kind: 'MESSAGE', ...value };
}
function message(index: number, name: string, rendered: string, level: 'INFO' | 'WARN' | 'ERROR', values: Record<string, ValueView> = {}): Entry {
  return entry(index, { message: { eventId: String(index + 1), name, level, tags: ['lifecycle'], rendered, renderStatus: 'RENDERED', language: 'en', languagePreferenceIndex: 0, payload: { base64: 'AQIDBA==', bytes: '4' }, fieldsStatus: 'DECODED', fields: fields(values) } });
}
export const demoEntries: Entry[] = [
  message(0, 'ConfigurationApplied', 'Configuration applied successfully', 'INFO', { revision: { kind: 'UNSIGNED', text: '18446744073709551615' }, source: text('operator') }),
  entry(1, { namespace: 'network.link', class: 'TELEMETRY', kind: 'SAMPLE', sample: { metricId: '4', name: 'Link state', unit: '', tags: ['network'], kind: 'State', stateName: 'Connected', severity: 'normal', value: { kind: 'UNSIGNED', text: '2' } } }),
  message(2, 'WriteBacklog', 'Writer queue is approaching its capacity', 'WARN', { queued: { kind: 'UNSIGNED', text: '48' }, capacity: { kind: 'UNSIGNED', text: '64' } }),
  entry(3, { namespace: 'worker.jobs', kind: 'SPAN_END', span: '4294967295', spanEvent: { id: '4294967295', kindName: null, parent: null } }),
  entry(4, { namespace: 'worker.jobs', kind: 'SPAN_START', span: '4294967295', spanEvent: { id: '4294967295', kindName: 'WarmUp', parent: '2147483648' } }),
  message(5, 'DependencyUnavailable', 'The upstream service did not confirm the request', 'ERROR', { retry: { kind: 'BOOLEAN', boolean: false }, detail: text('No automatic retry was performed.') }),
  entry(6, { namespace: 'sensor.health', class: 'TELEMETRY', kind: 'SAMPLE', sample: { metricId: '6', name: 'Sensor reading', unit: 'V', tags: [], kind: 'Gauge', stateName: null, severity: null, value: { kind: 'FLOAT', float: { width: 64, class: 'NAN', decimal: 'NaN', bits: '7ff8000000000001' } } } }),
  entry(7, { namespace: 'external.module', message: { eventId: '65535', name: null, level: null, tags: [], rendered: null, renderStatus: 'UNKNOWN_SCHEMA', language: null, languagePreferenceIndex: null, payload: { base64: 'AAECAw==', bytes: '4' }, fieldsStatus: 'UNKNOWN_SCHEMA', fields: null } }),
  entry(8, { namespace: 'storage.writer', kind: 'TEXT', text: { level: 'INFO', target: 'storage::writer', text: 'Committed blocks are available to readers' } }),
  message(9, 'ServiceReady', 'Service entered the ready state', 'INFO', { mode: text('active'), limits: { kind: 'MAP' } }),
];
const russianMessages: Record<string, string> = {
  ConfigurationApplied: 'Конфигурация успешно применена', WriteBacklog: 'Очередь записи приближается к пределу',
  DependencyUnavailable: 'Внешняя служба не подтвердила запрос', ServiceReady: 'Служба перешла в состояние готовности',
};
function localizedEntry(source: Entry, presentation?: PresentationInput): Entry {
  const entry = structuredClone(source); const message = entry.message;
  if (message?.renderStatus === 'RENDERED') {
    const languages = presentation?.languages ?? ['en'];
    const index = languages.findIndex(code => ['en', 'ru'].includes(code.toLowerCase()));
    const language = index < 0 ? 'en' : languages[index].toLowerCase();
    message.language = language; message.languagePreferenceIndex = index < 0 ? null : index;
    if (language === 'ru' && message.name) message.rendered = russianMessages[message.name] ?? message.rendered;
  }
  return entry;
}
export type DemoScenario = 'history' | 'empty' | 'gap' | 'unknown' | 'limit' | 'disconnect';
export const scenarios: { value: DemoScenario; label: string }[] = [ { value: 'history', label: 'Mixed history' }, { value: 'empty', label: 'No results' }, { value: 'gap', label: 'Retention gap on next page' }, { value: 'unknown', label: 'Unknown schema and wide values' }, { value: 'limit', label: 'Viewer limit' }, { value: 'disconnect', label: 'Live connection interrupted' } ];
export class DemoTransport implements Transport {
  private count = 0; private sessions = new Map<string, { revision: string; offset: number; rows: Entry[]; presentation?: PresentationInput }>();
  constructor(public scenario: DemoScenario = 'history') {}
  async catalog(): Promise<Catalog> {
    const names = [...new Set(demoEntries.map(entry => entry.namespace))];
    return { apiVersion: '1', defaultLanguages: ['en'], namespaces: names.map(namespace => ({ namespace, class: 'DEFAULT', schemaName: 'example', protocolVersion: '3', retainedBytes: '248320' })), schemas: [{ name: 'example', version: '3', languages: ['en', 'ru'], events: demoEntries.flatMap(entry => entry.message?.name ? [{ id: entry.message.eventId, name: entry.message.name, tags: entry.message.tags, fields: [{ name: 'revision', typeName: 'u64' }] }] : []), metrics: [{ id: '4', name: 'Link state', valueType: 'u64', unit: '', kind: 'State' }] }] };
  }
  async statistics(): Promise<StorageStatistics> {
    const channels = BigInt(new Set(demoEntries.map(entry => entry.namespace)).size);
    return { countingState: 'READY', retainedRecords: String(demoEntries.length * 2), retainedBlocks: channels.toString(), logicalRetainedBytes: (channels * 248320n).toString(), channels: channels.toString(), roots: [{ index: '0', allocatedBytes: '1048576', logBytes: '786432', rewriteBytes: '0', metadataBytes: '262144', physicalLimitBytes: '67108864' }], runtime: { recordsWritten: String(demoEntries.length * 2), blocksWritten: channels.toString(), bytesWritten: (channels * 248320n).toString(), dropped: '0', rejected: '0', ioErrors: '0' } };
  }
  async open(query: QueryInput, _metadataOnly = false, presentation?: PresentationInput): Promise<Session> {
    let rows = this.scenario === 'empty' ? [] : this.scenario === 'unknown' ? [demoEntries[0], demoEntries[6], demoEntries[7], demoEntries[9]] : [...demoEntries, ...demoEntries];
    if (query.namespaces) rows = rows.filter(entry => query.namespaces!.includes(entry.namespace));
    if (query.namespacePrefix) rows = rows.filter(entry => entry.namespace.startsWith(query.namespacePrefix!));
    if (query.classes) rows = rows.filter(entry => query.classes!.includes(entry.class));
    if (query.filter?.eventNames) rows = rows.filter(entry => query.filter!.eventNames!.includes(entry.message?.name ?? ''));
    const kinds = query.filter?.kinds; if (kinds) rows = rows.filter(entry => entry.kind === 'MESSAGE' ? kinds.messages : entry.kind === 'TEXT' ? kinds.text : entry.kind === 'SAMPLE' ? kinds.samples : kinds.spans);
    if (query.order === 'OLDEST') rows.reverse();
    const session = { id: `demo-${++this.count}`, revision: '9007199254740992' };
    this.sessions.set(session.id, { revision: session.revision, offset: 0, rows, presentation }); return session;
  }
  async countedOpen(query: QueryInput, records: number, metadataOnly = false, presentation?: PresentationInput): Promise<CountedSession> {
    const session = await this.open(query, metadataOnly, presentation);
    const rows = this.sessions.get(session.id)!.rows;
    return { ...session, totalMatches: String(rows.length), totalSeeds: '0', totalPages: String(Math.max(1, Math.ceil(rows.length / Math.min(4, records)))) };
  }
  async page(session: Session, records: number, metadataOnly: boolean, signal: AbortSignal): Promise<SessionPage> {
    await new Promise<void>((resolve, reject) => { const timer = setTimeout(resolve, 140); signal.addEventListener('abort', () => { clearTimeout(timer); reject(new DOMException('Aborted', 'AbortError')); }, { once: true }); });
    const state = this.sessions.get(session.id); if (!state) throw new ApiError('SESSION_NOT_FOUND', 'Demo session closed');
    if (state.revision !== session.revision) throw new ApiError('REVISION_CONFLICT', 'Demo revision changed');
    if (this.scenario === 'gap' && state.offset) throw new ApiError('RETENTION_GAP', 'Demo retention gap');
    const entries = state.rows.slice(state.offset, state.offset + Math.min(4, records)).map(entry => localizedEntry(entry, state.presentation));
    if (metadataOnly) for (const entry of entries) { if (entry.message) { entry.message.fields = null; entry.message.payload = null; entry.message.rendered = null; entry.message.fieldsStatus = 'NOT_REQUESTED'; entry.message.renderStatus = 'NOT_REQUESTED'; } if (entry.sample) entry.sample.value = null; }
    state.offset += entries.length; state.revision = (BigInt(state.revision) + 1n).toString();
    const page: Page = { entries, seeds: [], damaged: [], unanchored: [], exhausted: state.offset >= state.rows.length, truncated: false };
    return { revision: state.revision, page };
  }
  async seek(session: Session, targetPage: number, records: number, metadataOnly: boolean, signal: AbortSignal, presentation?: PresentationInput): Promise<SeekPage> {
    const state = this.sessions.get(session.id); if (!state) throw new ApiError('SESSION_NOT_FOUND', 'Demo session closed');
    const size = Math.min(4, records); const lastPage = Math.max(1, Math.ceil(state.rows.length / size)); const reached = Math.min(targetPage, lastPage);
    // Reuse the preview's bounded conversion and delay while selecting from its fixed rows.
    state.offset = (reached - 1) * size; state.presentation = presentation;
    const result = await this.page(session, records, metadataOnly, signal);
    return { ...result, reachedPage: String(reached), targetReached: reached === targetPage, exhausted: reached === lastPage };
  }
  async cancel(id: string) { this.sessions.delete(id); }
  async close(session: Session) { this.sessions.delete(session.id); }
  follow(_query: QueryInput, _metadataOnly: boolean, callbacks: { next(batch: Batch): void; error(error: unknown): void; complete(): void }, presentation?: PresentationInput): () => void {
    let count = 0;
    const timer = setInterval(() => {
      if (this.scenario === 'disconnect' && count === 2) { clearInterval(timer); callbacks.error(new ApiError('NETWORK', 'Demo connection lost')); return; }
      callbacks.next({ entries: [localizedEntry(demoEntries[count % demoEntries.length], presentation)], seeds: [], damaged: [], unanchored: [], ended: false }); count++;
    }, 650);
    return () => clearInterval(timer);
  }
}
