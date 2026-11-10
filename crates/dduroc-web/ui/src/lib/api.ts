import { createClient } from 'graphql-ws';
import type { Bootstrap, Catalog, StorageStatistics, Transport, QueryInput, Session, SessionPage, Batch, PresentationInput, SeekPage, CountedSession } from './types';
export class ApiError extends Error { constructor(public code: string, message: string) { super(message); this.name = 'ApiError'; } }
const VALUE_LEAF = 'kind text boolean variant float { width class decimal bits } bytes { base64 bytes }';
// This fits the server's default depth 16. Missing child keys are explicitly
// represented as "More nested fields" by the details UI; raw payload stays available.
const value = (depth: number): string => depth ? `${VALUE_LEAF} items { ${value(depth - 1)} } pairs { key { ${VALUE_LEAF} } value { ${value(depth - 1)} } }` : VALUE_LEAF;
const ENTRY = `namespace class at { boot micros } utc span kind
 message { eventId name level tags rendered @skip(if:$metadataOnly) renderStatus @skip(if:$metadataOnly) language languagePreferenceIndex payload @skip(if:$metadataOnly) { base64 bytes } fieldsStatus @skip(if:$metadataOnly) fields @skip(if:$metadataOnly) { ${value(2)} } }
 text { level target text @skip(if:$metadataOnly) }
 sample { metricId name unit tags kind stateName severity value @skip(if:$metadataOnly) { ${VALUE_LEAF} } }
 spanEvent { id kindName parent } extension @skip(if:$metadataOnly) { base64 bytes }`;
const PAGE = `entries { ${ENTRY} } seeds { ${ENTRY} } damaged { offset message } unanchored exhausted truncated`;
const CATALOG = `query { catalog { apiVersion defaultLanguages namespaces { namespace class schemaName protocolVersion retainedBytes } schemas { name version languages events { id name tags fields { name typeName } } metrics { id name valueType unit kind } } } }`;
const STATISTICS = `query { statistics { capturedAt countingState retainedRecords retainedBlocks logicalRetainedBytes channels roots { index allocatedBytes logBytes rewriteBytes metadataBytes physicalLimitBytes } runtime { recordsWritten blocksWritten bytesWritten dropped rejected ioErrors } } }`;
const BACKFILL_STATISTICS = `mutation { backfillStatistics { more } }`;
const OPEN = `mutation($query:QueryInput!,$presentation:PresentationInput){openReadSession(query:$query,presentation:$presentation){id revision}}`;
const COUNTED_OPEN = `mutation($query:QueryInput!,$presentation:PresentationInput,$records:UInt64!){openCountedReadSession(query:$query,presentation:$presentation,page:{maxRecords:$records,maxBytes:"1048576"}){id revision totalMatches totalSeeds totalPages}}`;
const PAGE_QUERY = `mutation($id:String!,$revision:UInt64!,$records:UInt64!,$metadataOnly:Boolean!){readSession(id:$id,expectedRevision:$revision,page:{maxRecords:$records,maxBytes:"1048576"}){revision page{${PAGE}}}}`;
const SEEK = `mutation($id:String!,$revision:UInt64!,$targetPage:UInt64!,$records:UInt64!,$metadataOnly:Boolean!,$presentation:PresentationInput){seekReadSession(id:$id,expectedRevision:$revision,targetPage:$targetPage,page:{maxRecords:$records,maxBytes:"1048576"},presentation:$presentation){revision reachedPage targetReached exhausted page{${PAGE}}}}`;
const CLOSE = `mutation($id:String!,$revision:UInt64!){closeReadSession(id:$id,expectedRevision:$revision)}`;
const CANCEL = `mutation($id:String!){cancelReadSession(id:$id)}`;
const FOLLOW = `subscription($query:QueryInput!,$presentation:PresentationInput,$metadataOnly:Boolean!){follow(query:$query,presentation:$presentation){entries{${ENTRY}}seeds{${ENTRY}}damaged{offset message}unanchored ended}}`;
const RESPONSE_LIMIT = 2 * 1024 * 1024;
function endpoint(value: string, base: string, socket: boolean): string {
  const url = new URL(value, base);
  if (socket && url.protocol === 'http:') url.protocol = 'ws:';
  if (socket && url.protocol === 'https:') url.protocol = 'wss:';
  if (url.username || url.password || !(socket ? ['ws:', 'wss:'] : ['http:', 'https:']).includes(url.protocol)) throw new ApiError('CONFIGURATION', 'The configured endpoint has an unsupported protocol or embedded credentials.');
  return url.href;
}
async function body(response: Response): Promise<unknown> {
  if (!response.ok) throw new ApiError('NETWORK', `The server returned HTTP ${response.status}.`);
  if (!response.body) throw new ApiError('NETWORK', 'The server returned no response body.');
  const reader = response.body.getReader(); const decoder = new TextDecoder(); let size = 0; let text = '';
  try { while (true) { const chunk = await reader.read(); if (chunk.done) break; size += chunk.value.byteLength; if (size > RESPONSE_LIMIT) throw new ApiError('VIEWER_LIMIT', 'The response exceeds the viewer byte limit. Narrow the query or use metadata only.'); text += decoder.decode(chunk.value, { stream: true }); } text += decoder.decode(); return JSON.parse(text); }
  finally { await reader.cancel().catch(() => {}); reader.releaseLock(); }
}
function data<T>(payload: unknown): T {
  const response = payload as { data?: T; errors?: { message?: string; extensions?: { code?: string } }[] };
  if (!response || typeof response !== 'object') throw new ApiError('PROTOCOL', 'The server returned an invalid GraphQL response.');
  if (response.errors?.length) throw new ApiError(response.errors[0].extensions?.code ?? 'GRAPHQL_ERROR', response.errors[0].message ?? 'The query failed.');
  if (!response.data) throw new ApiError('PROTOCOL', 'The response contains no data.'); return response.data;
}
export async function loadBootstrap(base = document.baseURI): Promise<Bootstrap> {
  const value = await body(await fetch(new URL('dduroc-config.json', base), { credentials: 'same-origin', signal: AbortSignal.timeout(10_000) })) as Partial<Bootstrap>;
  if (typeof value.graphqlEndpoint !== 'string') throw new ApiError('CONFIGURATION', 'The viewer configuration has no GraphQL endpoint.');
  if (value.maxRows !== undefined && (!Number.isSafeInteger(value.maxRows) || value.maxRows < 1 || value.maxRows > 10_000)) throw new ApiError('CONFIGURATION', 'The configured row limit must be between 1 and 10000.');
  return { graphqlEndpoint: endpoint(value.graphqlEndpoint, base, false), subscriptionEndpoint: typeof value.subscriptionEndpoint === 'string' ? endpoint(value.subscriptionEndpoint, base, true) : undefined, title: typeof value.title === 'string' ? value.title.slice(0, 120) : 'Journal', maxRows: value.maxRows ?? 1000 };
}
function frameWithinLimit(text: string, limit: number): boolean {
  if (text.length > limit) return false;
  let bytes = 0;
  for (let index = 0; index < text.length; index++) {
    const unit = text.charCodeAt(index);
    if (unit <= 0x7f) bytes++;
    else if (unit <= 0x7ff) bytes += 2;
    else if (unit >= 0xd800 && unit <= 0xdbff && index + 1 < text.length && text.charCodeAt(index + 1) >= 0xdc00 && text.charCodeAt(index + 1) <= 0xdfff) { bytes += 4; index++; }
    else bytes += 3;
    if (bytes > limit) return false;
  }
  return true;
}
export class HttpTransport implements Transport {
  constructor(private config: Bootstrap) {}
  private async request<T>(query: string, variables: object = {}, signal?: AbortSignal, cleanup = false): Promise<T> {
    const timeout = AbortSignal.timeout(15_000);
    return data<T>(await body(await fetch(this.config.graphqlEndpoint, { method: 'POST', headers: { 'Content-Type': 'application/json' }, credentials: 'same-origin', body: JSON.stringify({ query, variables }), signal: signal ? AbortSignal.any([signal, timeout]) : timeout, keepalive: cleanup })));
  }
  async catalog(signal?: AbortSignal): Promise<Catalog> { const result = (await this.request<{ catalog: Catalog }>(CATALOG, {}, signal)).catalog; if (result.apiVersion !== '1') throw new ApiError('PROTOCOL', 'This viewer requires GraphQL API version 1.'); return result; }
  async statistics(signal?: AbortSignal): Promise<StorageStatistics> { return (await this.request<{ statistics: StorageStatistics }>(STATISTICS, {}, signal)).statistics; }
  async backfillStatistics(signal?: AbortSignal): Promise<{ more: boolean }> { return (await this.request<{ backfillStatistics: { more: boolean } }>(BACKFILL_STATISTICS, {}, signal)).backfillStatistics; }
  async open(query: QueryInput, _metadataOnly = false, presentation?: PresentationInput): Promise<Session> { return (await this.request<{ openReadSession: Session }>(OPEN, { query, presentation })).openReadSession; }
  async countedOpen(query: QueryInput, records: number, _metadataOnly = false, presentation?: PresentationInput): Promise<CountedSession> { return (await this.request<{ openCountedReadSession: CountedSession }>(COUNTED_OPEN, { query, presentation, records: String(records) })).openCountedReadSession; }
  async page(session: Session, records: number, metadataOnly: boolean, signal: AbortSignal): Promise<SessionPage> { return (await this.request<{ readSession: SessionPage }>(PAGE_QUERY, { id: session.id, revision: session.revision, records: String(records), metadataOnly }, signal)).readSession; }
  async seek(session: Session, targetPage: number, records: number, metadataOnly: boolean, signal: AbortSignal, presentation?: PresentationInput): Promise<SeekPage> { return (await this.request<{ seekReadSession: SeekPage }>(SEEK, { id: session.id, revision: session.revision, targetPage: String(targetPage), records: String(records), metadataOnly, presentation }, signal)).seekReadSession; }
  async cancel(id: string): Promise<void> { try { await this.request(CANCEL, { id }, undefined, true); } catch (error) { if (!(error instanceof ApiError && error.code === 'SESSION_NOT_FOUND')) throw error; } }
  async close(session: Session): Promise<void> { await this.request(CLOSE, { id: session.id, revision: session.revision }, undefined, true); }
  follow(query: QueryInput, metadataOnly: boolean, callbacks: { next(batch: Batch): void; error(error: unknown): void; complete(): void }, presentation?: PresentationInput): () => void {
    if (!this.config.subscriptionEndpoint) throw new ApiError('UNSUPPORTED_OPERATION', 'This host has not configured live subscriptions.');
    // An interrupted stream cannot resume exactly. Never reconnect or replay implicitly.
    let finished = false;
    const fail = (error: unknown) => { if (finished) return; finished = true; callbacks.error(error); void client.dispose(); };
    const client = createClient({ url: this.config.subscriptionEndpoint, lazy: true, retryAttempts: 0, connectionAckWaitTimeout: 10_000,
      on: { opened(socket) {
        const ws = socket as WebSocket;
        const parse = ws.onmessage;
        // graphql-ws installs its parser before onopen. Guard the raw textual
        // frame before delegating, without allocating a second encoded buffer.
        ws.onmessage = (event) => {
          if (typeof event.data !== 'string' || !frameWithinLimit(event.data, RESPONSE_LIMIT)) {
            fail(new ApiError('VIEWER_LIMIT', 'The live frame exceeds the viewer byte limit.'));
            ws.close(3008, 'Viewer frame limit'); return;
          }
          parse?.call(ws, event);
        };
      } },
    });
    const dispose = client.subscribe<{ follow: Batch }>({ query: FOLLOW, variables: { query, presentation, metadataOnly } }, {
      next: result => { if (finished) return; try { callbacks.next(data<{ follow: Batch }>(result).follow); } catch (error) { fail(error); } },
      error: fail,
      complete: () => { if (!finished) { finished = true; callbacks.complete(); } },
    });
    return () => { finished = true; dispose(); void client.dispose(); };
  }
}
