import { ApiError } from './api';
import type { Damage, Entry, PresentationInput, QueryInput, Session, Transport, ValueView } from './types';

export type ExportFormat = 'csv' | 'jsonl';
export interface ExportOptions { query: QueryInput; presentation?: PresentationInput; metadataOnly: boolean; format: ExportFormat; maxRecords: number; maxBytes: number }
export interface ExportProgress { records: number; bytes: number }
export type ExportCleanup = 'closed' | 'cancelled' | 'unconfirmed';
export interface ExportResult { blob: Blob; filename: string; records: number; bytes: number; complete: boolean; cleanup: ExportCleanup; warnings: string[] }
export class ExportCleanupError extends Error {
  constructor() { super('The server session cleanup could not be confirmed.'); this.name = 'ExportCleanupError'; }
}
export const DEFAULT_EXPORT_RECORDS = 10_000;
export const DEFAULT_EXPORT_BYTES = 16 * 1024 * 1024;
export const MAX_EXPORT_RECORDS = 100_000;
export const MAX_EXPORT_BYTES = 32 * 1024 * 1024;
const SUMMARY_RESERVE = 4096;
const encoder = new TextEncoder();

function abortError(): DOMException { return new DOMException('Export cancelled', 'AbortError'); }
function checkAbort(signal: AbortSignal): void { if (signal.aborted) throw abortError(); }
function safeCsv(value: string): string {
  // Preserve the exact value in data_json. Leading controls/whitespace are
  // inspected because spreadsheets can ignore them before a formula marker.
  const safe = /^[\t\r\n]|^[\s\u0000-\u001f]*[=+@-]|^\d{16,}$/u.test(value) ? `'${value}` : value;
  return `"${safe.replaceAll('"', '""')}"`;
}
function csvRow(type: string, payload: unknown, entry?: Entry): string {
  return [type, entry?.namespace ?? '', entry?.class ?? '', entry?.utc ?? '', entry?.at.boot ?? '', entry?.at.micros ?? '', entry?.kind ?? '', JSON.stringify(payload)].map(safeCsv).join(',') + '\r\n';
}
function line(format: ExportFormat, type: string, payload: unknown, entry?: Entry): string {
  return format === 'csv' ? csvRow(type, payload, entry) : JSON.stringify({ type, data: payload }) + '\n';
}
function truncatedValue(value: ValueView | null | undefined): boolean {
  if (!value) return false;
  if (value.kind === 'SEQUENCE' && !value.items) return true;
  if (value.kind === 'MAP' && !value.pairs) return true;
  return (value.items?.some(truncatedValue) ?? false) || (value.pairs?.some(pair => truncatedValue(pair.key) || truncatedValue(pair.value)) ?? false);
}
function observe(entry: Entry, warnings: Set<string>, metadataOnly: boolean): void {
  if (entry.utc === null) warnings.add('Some records have no UTC anchor; boot time remains available.');
  if (metadataOnly) return;
  if (entry.message && entry.message.name === null) warnings.add('Some event schemas are unavailable; raw payload is included when provided.');
  if (truncatedValue(entry.message?.fields) || truncatedValue(entry.sample?.value)) warnings.add('Some nested decoded fields exceed the projection depth; raw payload is included when provided.');
  if (entry.message && entry.message.fieldsStatus !== 'DECODED') warnings.add('Some decoded event fields were unavailable; inspect fieldsStatus and raw payload.');
}

/** Export a fresh finite query session; callers must wait for cancellation and cleanup before starting another export. */
export async function exportHistory(api: Transport, options: ExportOptions, signal: AbortSignal, onProgress: (progress: ExportProgress) => void = () => {}): Promise<ExportResult> {
  const { format, metadataOnly, maxRecords, maxBytes } = options;
  const presentation = options.presentation ? { languages: [...options.presentation.languages] } : undefined;
  if (!['csv', 'jsonl'].includes(format) || !Number.isSafeInteger(maxRecords) || maxRecords < 1 || maxRecords > MAX_EXPORT_RECORDS || !Number.isSafeInteger(maxBytes) || maxBytes < SUMMARY_RESERVE * 2 || maxBytes > MAX_EXPORT_BYTES) throw new RangeError('Export limits are outside the supported range.');
  checkAbort(signal);
  const queryJson = JSON.stringify(options.query);
  if (typeof queryJson !== 'string' || queryJson.length > Math.min(maxBytes - SUMMARY_RESERVE, 1024 * 1024)) throw new RangeError('Export query is too large.');
  const query: QueryInput = JSON.parse(queryJson);
  const chunks: Uint8Array<ArrayBuffer>[] = [];
  let pending = new Uint8Array(64 * 1024);
  let pendingBytes = 0;
  const warnings = new Set<string>();
  let bytes = 0; let records = 0; let complete = false; let session: Session | undefined;
  let cancelled = false; let failed = false; let cleanup: ExportCleanup = 'unconfirmed';
  let sawDamage = false; let excludedUtcRuns = false; let sawTruncation = false;
  const append = (text: string, reserve = SUMMARY_RESERVE): boolean => {
    if (text.length > maxBytes - bytes - reserve) return false;
    const encoded = encoder.encode(text);
    if (bytes + encoded.byteLength + reserve > maxBytes) return false;
    let offset = 0;
    while (offset < encoded.byteLength) {
      const count = Math.min(pending.byteLength - pendingBytes, encoded.byteLength - offset);
      pending.set(encoded.subarray(offset, offset + count), pendingBytes);
      pendingBytes += count; offset += count;
      if (pendingBytes === pending.byteLength) { chunks.push(pending); pending = new Uint8Array(64 * 1024); pendingBytes = 0; }
    }
    bytes += encoded.byteLength; return true;
  };
  const note = (warning: string) => warnings.add(warning);
  const add = (type: string, payload: unknown, entry?: Entry) => append(line(format, type, payload, entry));
  if (format === 'csv' && !append('record_type,namespace,class,utc,boot,micros,kind,data_json\r\n')) throw new RangeError('Export byte limit is too small.');
  if (!add('metadata', { version: 1, query, presentation: presentation ?? null, metadataOnly, maxRecords, maxBytes, fieldProjectionDepth: 2, sampleValueProjection: 'scalar or container header only', schemaProvenance: 'not included; inspect event names and fieldsStatus', csvFormulaProtection: format === 'csv', encoding: 'UTF-8' })) throw new RangeError('Export byte limit is too small for query metadata.');
  try {
    // open intentionally is not aborted: a late successful acknowledgement
    // must be observed so the server-side session can be cancelled.
    session = await api.open(query, metadataOnly, presentation);
    checkAbort(signal);
    loop: while (true) {
      checkAbort(signal);
      const page = await api.page(session, Math.min(128, maxRecords - records), metadataOnly, signal);
      session = { id: session.id, revision: page.revision };
      checkAbort(signal);
      sawDamage ||= page.page.damaged.length > 0;
      excludedUtcRuns ||= page.page.unanchored.length > 0;
      sawTruncation ||= page.page.truncated;
      for (const damage of page.page.damaged as Damage[]) {
        note('The source reported damaged data.');
        if (!add('damage', damage)) { note('The export byte limit was reached.'); break loop; }
      }
      for (const boot of page.page.unanchored) {
        note('Some runs have no UTC anchor and were excluded from the UTC query.');
        if (!add('unanchored', { boot })) { note('The export byte limit was reached.'); break loop; }
      }
      for (const [type, items] of [['seed', page.page.seeds], ['entry', page.page.entries]] as const) {
        for (const entry of items) {
          if (records >= maxRecords) { note('The export record limit was reached.'); break loop; }
          if (!add(type, entry, entry)) { note('The export byte limit was reached.'); break loop; }
          records++; observe(entry, warnings, metadataOnly);
        }
      }
      onProgress({ records, bytes });
      if (page.page.truncated) note('The server reported a truncated result.');
      if (page.page.exhausted) { complete = !sawTruncation && !sawDamage && !excludedUtcRuns; break; }
      if (records >= maxRecords) { note('The export record limit was reached.'); break; }
    }
  } catch (error) {
    if (!session) {
      // No capability ID was delivered: a network/protocol failure can occur
      // after the server created it, so we cannot confirm cleanup locally.
      if (!(error instanceof ApiError) || ['NETWORK', 'PROTOCOL', 'VIEWER_LIMIT'].includes(error.code)) throw new ExportCleanupError();
      throw error;
    }
    if (signal.aborted || (error instanceof DOMException && error.name === 'AbortError')) cancelled = true;
    else { failed = true; note(`The export stopped because the server request failed: ${error instanceof Error ? error.message.slice(0, 256) : 'unknown error'}.`); }
  } finally {
    if (session) {
      const needsCancel = cancelled || failed || signal.aborted;
      try {
        if (needsCancel) { await api.cancel(session.id); cleanup = 'cancelled'; }
        else { await api.close(session); cleanup = 'closed'; }
      } catch {
        if (!needsCancel) {
          try { await api.cancel(session.id); cleanup = 'cancelled'; }
          catch { note('The server session could not be confirmed closed.'); }
        } else note('The server session could not be confirmed closed.');
      }
    }
  }
  if (signal.aborted || cancelled) {
    if (session && cleanup === 'unconfirmed') throw new ExportCleanupError();
    throw abortError();
  }
  const warningList = [...warnings];
  const summary = line(format, 'summary', { version: 1, records, complete, cleanup, warnings: warningList });
  if (!append(summary, 0)) throw new Error('Export summary exceeds its reserved space.');
  if (pendingBytes) chunks.push(pending.slice(0, pendingBytes));
  const blob = new Blob(chunks, { type: format === 'csv' ? 'text/csv;charset=utf-8' : 'application/x-ndjson;charset=utf-8' });
  return { blob, filename: `dduroc-export${complete ? "" : ".partial"}.${format}`, records, bytes, complete, cleanup, warnings: warningList };
}
