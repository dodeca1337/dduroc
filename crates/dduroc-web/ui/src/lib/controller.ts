import { writable, type Readable } from 'svelte/store';
import { ApiError } from './api';
import type { Batch, Catalog, StorageStatistics, Damage, Page, PresentationInput, QueryInput, Session, StoredRow, Transport, ViewError } from './types';
export type Phase = 'idle' | 'counting' | 'loading' | 'ready' | 'loading-more' | 'live' | 'stopped' | 'complete' | 'error' | 'limit';
export interface ViewerState {
  totalMatches: string | null; totalSeeds: string | null;
  statistics: StorageStatistics | null; statisticsError: string | null;
  phase: Phase; rows: StoredRow[]; seeds: StoredRow[]; catalog: Catalog | null; catalogError: string | null;
  error: ViewError | null; damaged: Damage[]; unanchored: string[]; truncated: boolean; exhausted: boolean;
  mode: 'snapshot' | 'live'; bytes: number; generation: number; cleanupWarning: string | null;
  cachedPageNumbers: number[]; canSeek: boolean; knownLastPage: number | null; navigationTarget: number | null; navigationProgress: number; languageBusy: boolean; notice: string | null;
  page: number; pageSize: number; firstCachedPage: number; lastCachedPage: number; cachedPages: number; cachedRecords: number; visitedRecords: number; canPrevious: boolean; canNext: boolean; pageBusy: boolean; otherPageWarnings: boolean;
}
export const initialState = (): ViewerState => ({ totalMatches: null, totalSeeds: null, statistics: null, statisticsError: null, phase: 'idle', rows: [], seeds: [], catalog: null, catalogError: null, error: null, damaged: [], unanchored: [], truncated: false, exhausted: false, mode: 'snapshot', cachedPageNumbers: [], canSeek: false, knownLastPage: null, navigationTarget: null, navigationProgress: 0, languageBusy: false, notice: null, bytes: 0, generation: 0, cleanupWarning: null, page: 0, pageSize: 64, firstCachedPage: 0, lastCachedPage: 0, cachedPages: 0, cachedRecords: 0, visitedRecords: 0, canPrevious: false, canNext: false, pageBusy: false, otherPageWarnings: false });
export function explain(error: unknown): ViewError {
  const code = error instanceof ApiError ? error.code : error instanceof Error && error.name === 'AbortError' ? 'CANCELLED' : 'NETWORK';
  const messages: Record<string, [string, string]> = {
    RETENTION_GAP: ['History changed', 'Retention removed part of this history. The cursor was closed. Run a new query to resync.'],
    SNAPSHOT_EXPIRED: ['Snapshot changed', 'The underlying history was replaced. Run a new query to resync.'],
    CONVERSION_FAILED: ['Details could not be read', 'A field, rendered message or payload exceeds the server limits or cannot be decoded. Choose Metadata only, then run a new query.'],
    PAGE_TOO_SMALL: ['A record exceeds the page limit', 'Narrow the query or ask the host to increase its page byte limit. This cursor was closed.'],
    REVISION_CONFLICT: ['Cursor changed', 'The page revision no longer matches. Run a new query; the previous page will not be replayed.'],
    SESSION_EXPIRED: ['Cursor expired', 'The session deadline or idle timeout was reached. Run a new query.'],
    SESSION_NOT_FOUND: ['Cursor is no longer available', 'The session was closed or the server restarted. Run a new query.'],
    VIEWER_LIMIT: ['Viewer limit reached', 'Reading stopped before discarding records. Narrow your filters and run a new query to resync.'],
    READ_LIMIT_EXCEEDED: ['Read limit reached', 'The server stopped this query at a resource limit. Narrow its scope and run again.'],
    DEADLINE_EXCEEDED: ['Read timed out', 'The outcome of the last page is uncertain. Its cursor was closed. Run a new query; no page is retried automatically.'],
    CAPACITY_EXCEEDED: ['Reader is busy', 'The server has no free read workers or sessions. Wait, then run a new query.'],
    SLOW_CONSUMER: ['Live reader fell behind', 'The stream stopped rather than dropping records silently. Run a new query to resync.'],
    PROTOCOL: ['Incompatible response', 'The server response is not compatible with this viewer. Check the host API version.'],
    NETWORK: ['Connection interrupted', 'The last read may have completed on the server. The cursor was closed. Check the connection, then run a new query.'],
  };
  const [title, message] = messages[code] ?? ['Read stopped', error instanceof Error ? error.message.slice(0, 1024) : 'The server could not complete this read. Run a new query.'];
  return { code, title, message };
}
interface CachedPage { number: number; rows: StoredRow[]; seeds: StoredRow[]; damaged: Damage[]; unanchored: string[]; bytes: number }
export class ViewerController implements Readable<ViewerState> {
  private value = initialState(); private store = writable(this.value); subscribe = this.store.subscribe;
  private generation = 0; private sequence = 0; private session: Session | null = null; private pageAbort: AbortController | null = null;
  private pendingOpen: Promise<void> | null = null; private cleanup: Promise<void> = Promise.resolve(); private stopStream: (() => void) | null = null; private disposed = false; private metadataOnly = false; private catalogAbort = new AbortController();
  private presentation: PresentationInput | undefined; private query: QueryInput | null = null; private presentationVersion = 0; private activeSeek: Promise<void> | null = null; private highestPage = 0; private finalPage: number | null = null;
  private pages: CachedPage[] = []; private evictedWarnings = false;
  constructor(private api: Transport, private maxRows = 1000, private maxBytes = 8 * 1024 * 1024, private pageSize = 64, private maxPages = 5) {
    if (![maxRows, maxBytes, pageSize, maxPages].every(value => Number.isSafeInteger(value) && value > 0)) throw new RangeError('Viewer cache limits must be positive integers.');
    this.pageSize = Math.min(pageSize, maxRows); this.set({ pageSize: this.pageSize });
  }
  private set(change: Partial<ViewerState>) { this.value = { ...this.value, ...change }; this.store.set(this.value); }
  private current(generation: number) { return !this.disposed && generation === this.generation; }
  private async cancel(id: string) { try { await this.api.cancel(id); } catch { if (!this.disposed) this.set({ cleanupWarning: 'An earlier cursor could not be closed. The server will expire it; no page was retried.' }); } }
  private retire() {
    this.pageAbort?.abort(); this.pageAbort = null;
    this.set({ pageBusy: false, canSeek: false, navigationTarget: null, canNext: this.value.mode === 'snapshot' && this.pages.some(page => page.number === this.value.page + 1), canPrevious: this.value.mode === 'snapshot' && this.pages.some(page => page.number === this.value.page - 1) });
    const stop = this.stopStream; this.stopStream = null; stop?.();
    const session = this.session; this.session = null;
    if (session) this.cleanup = Promise.all([this.cleanup, this.cancel(session.id)]).then(() => {});
    return this.cleanup;
  }
  private readFailed(error: unknown) {
    this.retire();
    const explained = explain(error);
    if (['SESSION_EXPIRED', 'RETENTION_GAP', 'SNAPSHOT_EXPIRED'].includes(explained.code)) {
      this.pages = []; this.evictedWarnings = false;
      this.set({ totalMatches: null, totalSeeds: null, knownLastPage: null, cachedPageNumbers: [], cachedPages: 0, cachedRecords: 0, firstCachedPage: 0, lastCachedPage: 0, canPrevious: false, canNext: false, otherPageWarnings: false });
    }
    this.set({ phase: explained.code === 'VIEWER_LIMIT' ? 'limit' : 'error', error: explained });
  }
  async discover() {
    const catalog = this.api.catalog(this.catalogAbort.signal).then(value => { if (!this.disposed) this.set({ catalog: value, catalogError: null }); }).catch(error => { if (!this.disposed) this.set({ catalogError: error instanceof Error ? error.message.slice(0, 1024) : 'Catalog unavailable' }); });
    const statistics = this.api.statistics?.(this.catalogAbort.signal).then(value => { if (!this.disposed) this.set({ statistics: value, statisticsError: null }); }).catch(() => { if (!this.disposed) this.set({ statisticsError: 'Storage statistics are unavailable. Refresh the catalog to try again.' }); });
    await Promise.all([catalog, statistics]);
  }
  async run(query: QueryInput, metadataOnly = false, presentation?: PresentationInput) {
    const generation = ++this.generation; const retired = this.retire(); this.metadataOnly = metadataOnly; this.query = structuredClone(query); this.presentation = presentation ? structuredClone(presentation) : undefined; this.highestPage = 0; this.finalPage = null; this.pages = []; this.evictedWarnings = false;
    this.set({ ...initialState(), catalog: this.value.catalog, catalogError: this.value.catalogError, statistics: this.value.statistics, statisticsError: this.value.statisticsError, phase: this.api.countedOpen ? 'counting' : 'loading', generation, pageSize: this.pageSize });
    // Serialize pending opens. A stale open is allowed to return its capability
    // so it can be explicitly cancelled instead of becoming an unknown orphan.
    if (this.pendingOpen) await this.pendingOpen.catch(() => {});
    await retired;
    if (!this.current(generation)) return;
    const pending = (async () => {
      const opened = this.api.countedOpen ? await this.api.countedOpen(query, this.pageSize, metadataOnly, presentation) : await this.api.open(query, metadataOnly, presentation);
      if (!this.current(generation)) { await this.cancel(opened.id); return; }
      this.session = { id: opened.id, revision: opened.revision };
      if (this.api.countedOpen) {
        if (!('totalMatches' in opened) || !('totalSeeds' in opened) || !('totalPages' in opened) ||
          ![opened.totalMatches, opened.totalSeeds, opened.totalPages].every(value => typeof value === 'string' && /^(0|[1-9]\d*)$/.test(value)) ||
          !Number.isSafeInteger(Number(opened.totalPages)) || Number(opened.totalPages) < 1) throw new ApiError('PROTOCOL', 'The counted snapshot totals are malformed or exceed the viewer page range.');
        this.finalPage = Number(opened.totalPages);
        this.set({ totalMatches: String(opened.totalMatches), totalSeeds: String(opened.totalSeeds), knownLastPage: this.finalPage, phase: 'loading' });
      }
      this.set({ canSeek: !!this.api.seek });
    })();
    this.pendingOpen = pending;
    try {
      await pending;
      if (this.current(generation) && this.session) { if (this.api.seek) await this.goTo(1); else await this.more(generation); }
    } catch (error) { if (this.current(generation)) { this.readFailed(error); } }
    finally { if (this.pendingOpen === pending) this.pendingOpen = null; }
  }
  private pageData(page: Pick<Page, 'entries' | 'seeds' | 'damaged' | 'unanchored'>) {
    if (!Array.isArray(page.entries) || !Array.isArray(page.seeds) || !Array.isArray(page.damaged) || !Array.isArray(page.unanchored)) throw new ApiError('PROTOCOL', 'The record batch is malformed.');
    const bytes = new TextEncoder().encode(JSON.stringify(page)).byteLength;
    const wrap = (entry: StoredRow['entry']): StoredRow => ({ id: `${this.generation}:${++this.sequence}`, entry });
    return { rows: page.entries.map(wrap), seeds: page.seeds.map(wrap), damaged: page.damaged, unanchored: page.unanchored, bytes };
  }
  private showPage(page: CachedPage) {
    this.set({ rows: page.rows, seeds: page.seeds, damaged: page.damaged, unanchored: page.unanchored, page: page.number,
      firstCachedPage: this.pages[0].number, lastCachedPage: this.pages[this.pages.length - 1].number,
      cachedPageNumbers: this.pages.map(value => value.number), cachedPages: this.pages.length, cachedRecords: this.pages.reduce((sum, value) => sum + value.rows.length + value.seeds.length, 0),
      bytes: this.pages.reduce((sum, value) => sum + value.bytes, 0), canPrevious: this.pages.some(value => value.number === page.number - 1) || (!!this.api.seek && !!this.session && page.number > 1),
      canNext: this.pages.some(value => value.number === page.number + 1) || (!!this.session && (this.api.seek ? this.finalPage === null || page.number < this.finalPage : !this.value.exhausted)),
      otherPageWarnings: this.evictedWarnings || this.pages.some(value => value.number !== page.number && (value.damaged.length > 0 || value.unanchored.length > 0)) });
  }
  private acceptSnapshot(page: Page): boolean {
    const data = this.pageData(page);
    if (data.rows.length + data.seeds.length > this.maxRows || data.bytes > this.maxBytes) {
      this.retire(); this.set({ phase: 'limit', error: explain(new ApiError('VIEWER_LIMIT', 'A page exceeds the browser cache limit')) }); return false;
    }
    const cached = { ...data, number: this.value.lastCachedPage + 1 };
    this.pages.push(cached);
    const overLimit = () => this.pages.length > this.maxPages || this.pages.reduce((sum, value) => sum + value.rows.length + value.seeds.length, 0) > this.maxRows || this.pages.reduce((sum, value) => sum + value.bytes, 0) > this.maxBytes;
    while (this.pages.length > 1 && overLimit()) {
      const removed = this.pages.shift()!;
      this.evictedWarnings ||= removed.damaged.length > 0 || removed.unanchored.length > 0;
    }
    this.set({ exhausted: page.exhausted, truncated: this.value.truncated || page.truncated, visitedRecords: this.value.visitedRecords + page.entries.length });
    this.showPage(cached); return true;
  }
  private acceptLive(batch: Pick<Page, 'entries' | 'seeds' | 'damaged' | 'unanchored'>, generation: number): boolean {
    if (!this.current(generation)) return false;
    if (!Array.isArray(batch.entries) || !Array.isArray(batch.seeds) || !Array.isArray(batch.damaged) || !Array.isArray(batch.unanchored)) throw new ApiError('PROTOCOL', 'The record batch is malformed.');
    const bytes = new TextEncoder().encode(JSON.stringify(batch)).byteLength;
    if (this.value.rows.length + this.value.seeds.length + batch.entries.length + batch.seeds.length > this.maxRows || bytes > this.maxBytes - this.value.bytes) {
      this.retire(); this.set({ phase: 'limit', error: explain(new ApiError('VIEWER_LIMIT', 'Viewer buffer full')) }); return false;
    }
    // No identity deduplication: equal timestamps, event IDs and payloads are valid records.
    const wrap = (entry: StoredRow['entry']): StoredRow => ({ id: `${generation}:${++this.sequence}`, entry });
    this.set({ rows: [...this.value.rows, ...batch.entries.map(wrap)], seeds: [...this.value.seeds, ...batch.seeds.map(wrap)], bytes: this.value.bytes + bytes,
      damaged: [...this.value.damaged, ...batch.damaged], unanchored: [...new Set([...this.value.unanchored, ...batch.unanchored])] });
    return true;
  }
  previous() {
    if (this.api.seek) return this.goTo(this.value.page - 1);
    if (this.value.mode !== 'snapshot' || this.pageAbort) return;
    const cached = this.pages.find(page => page.number === this.value.page - 1);
    if (cached) this.showPage(cached);
  }
  next() { return this.api.seek ? this.goTo(this.value.page + 1) : this.more(); }
  async more(generation = this.generation) {
    if (this.api.seek) return this.goTo(this.value.page + 1);
    if (!this.current(generation) || this.value.mode !== 'snapshot' || this.pageAbort) return;
    const cached = this.pages.find(page => page.number === this.value.page + 1);
    if (cached) { this.showPage(cached); return; }
    if (!this.session || this.value.exhausted) return;
    const abort = new AbortController(); this.pageAbort = abort;
    this.set({ pageBusy: true, ...(this.value.page ? { phase: 'loading-more' as const } : {}) });
    try {
      const result = await this.api.page(this.session, this.pageSize, this.metadataOnly, abort.signal);
      if (!this.current(generation)) return;
      this.session = { id: this.session!.id, revision: result.revision };
      if (!this.acceptSnapshot(result.page)) return;
      this.set({ phase: result.page.exhausted ? 'complete' : 'ready' });
      if (result.page.exhausted) {
        const session = this.session;
        const closing = this.cleanup.then(async () => {
          try { await this.api.close(session); } catch { await this.cancel(session.id); }
        });
        this.cleanup = closing; this.session = null;
        await closing;
      }
    } catch (error) { if (this.current(generation)) { this.readFailed(error); } }
    finally { if (this.pageAbort === abort) { this.pageAbort = null; this.set({ pageBusy: false }); } }
  }
  goTo(target: number, force = false): Promise<void> {
    if (!Number.isSafeInteger(target) || target < 1 || (this.finalPage !== null && target > this.finalPage) || this.disposed || this.value.mode !== 'snapshot' || this.pageAbort || (!force && this.value.languageBusy)) return Promise.resolve();
    const cached = !force && this.pages.find(page => page.number === target);
    if (cached) { this.showPage(cached); return Promise.resolve(); }
    if (!this.session || !this.api.seek) return Promise.resolve();
    const generation = this.generation; const presentationVersion = this.presentationVersion; const abort = new AbortController(); this.pageAbort = abort;
    this.set({ pageBusy: true, navigationTarget: target, navigationProgress: this.value.page, phase: this.value.page ? 'loading-more' : 'loading', error: null, notice: null });
    const pending = (async () => {
      try {
        let priorProgress: number | null = null;
        while (this.current(generation) && this.session && !abort.signal.aborted) {
          const result = await this.api.seek!(this.session, target, this.pageSize, this.metadataOnly, abort.signal, this.presentation);
          if (!this.current(generation)) return;
          const reached = Number(result.reachedPage);
          if (!Number.isSafeInteger(reached) || reached < 0 || (!result.targetReached && !result.exhausted && priorProgress !== null && reached <= priorProgress) || (result.targetReached && reached !== target) || ((result.targetReached || result.exhausted) && !result.page)) throw new ApiError('PROTOCOL', 'The page navigation response is malformed or made no progress.');
          this.session = { id: this.session!.id, revision: result.revision };
          this.set({ navigationProgress: reached }); priorProgress = reached;
          if (!result.targetReached && !result.exhausted) { await new Promise(resolve => setTimeout(resolve, 0)); continue; }
          const page = result.page!;
          // Complete the CAS before applying a queued language preference, but do not publish its old rendering.
          if (presentationVersion !== this.presentationVersion) break;
          const data = this.pageData(page);
          if (data.rows.length + data.seeds.length > this.maxRows || data.bytes > this.maxBytes) throw new ApiError('VIEWER_LIMIT', 'A page exceeds the browser cache limit');
          const cached = { ...data, number: reached };
          this.pages = this.pages.filter(value => value.number !== reached); this.pages.push(cached);
          const overLimit = () => this.pages.length > this.maxPages || this.pages.reduce((sum, value) => sum + value.rows.length + value.seeds.length, 0) > this.maxRows || this.pages.reduce((sum, value) => sum + value.bytes, 0) > this.maxBytes;
          while (this.pages.length > 1 && overLimit()) { const removed = this.pages.shift()!; this.evictedWarnings ||= !!(removed.damaged.length || removed.unanchored.length); }
          this.pages.sort((a, b) => a.number - b.number);
          const newlyVisited = reached > this.highestPage ? page.entries.length : 0;
          this.highestPage = Math.max(this.highestPage, reached);
          if (this.value.totalMatches !== null && (reached > this.finalPage! || result.exhausted !== (reached === this.finalPage))) throw new ApiError('PROTOCOL', 'The page response disagrees with the counted snapshot.');
          if (result.exhausted && this.value.totalMatches === null) this.finalPage = reached;
          this.set({ knownLastPage: this.finalPage });
          this.set({ exhausted: result.exhausted, truncated: this.value.truncated || page.truncated, visitedRecords: this.value.visitedRecords + newlyVisited, phase: result.exhausted ? 'complete' : 'ready', notice: result.targetReached ? null : `Page ${target} is beyond the end of this snapshot. Showing page ${reached}.` });
          this.showPage(cached); break;
        }
      } catch (error) { if (this.current(generation)) { this.readFailed(error); } }
      finally { if (this.pageAbort === abort) { this.pageAbort = null; this.set({ pageBusy: false, navigationTarget: null }); } }
    })();
    this.activeSeek = pending;
    void pending.finally(() => { if (this.activeSeek === pending) this.activeSeek = null; });
    return pending;
  }
  async setPresentation(presentation?: PresentationInput) {
    const version = ++this.presentationVersion; const generation = this.generation;
    const target = this.value.page || 1;
    const next = presentation ? structuredClone(presentation) : undefined;
    if (this.value.mode === 'live' && this.query) {
      const query = this.query; this.live(query, this.metadataOnly, next);
      this.set({ notice: 'Message language changed. Live history was resynchronized automatically; this is a new live read.' }); return;
    }
    this.set({ languageBusy: true, rows: [], seeds: [], damaged: [], unanchored: [] });
    await this.pendingOpen?.catch(() => {}); await this.activeSeek;
    if (!this.current(generation) || version !== this.presentationVersion) return;
    this.presentation = next; this.evictedWarnings ||= this.pages.some(page => !!(page.damaged.length || page.unanchored.length)); this.pages = [];
    this.set({ rows: [], seeds: [], damaged: [], unanchored: [], firstCachedPage: 0, lastCachedPage: 0, cachedPageNumbers: [], cachedPages: 0, cachedRecords: 0, bytes: 0, otherPageWarnings: this.evictedWarnings });
    if (!this.session || !this.api.seek) { this.set({ languageBusy: false, canPrevious: false, canNext: false, notice: 'The snapshot cursor is no longer available. Run a new query to render this language.' }); return; }
    await this.goTo(target, true);
    if (this.current(generation) && version === this.presentationVersion) this.set({ languageBusy: false });
  }
  live(query: QueryInput, metadataOnly = false, presentation?: PresentationInput) {
    const generation = ++this.generation; this.retire(); this.pages = []; this.evictedWarnings = false; this.query = structuredClone(query); this.presentation = presentation ? structuredClone(presentation) : undefined; this.metadataOnly = metadataOnly;
    this.set({ ...initialState(), catalog: this.value.catalog, catalogError: this.value.catalogError, statistics: this.value.statistics, statisticsError: this.value.statisticsError, phase: 'live', mode: 'live', generation });
    const fail = (error: unknown) => { if (this.current(generation)) { this.readFailed(error); } };
    try {
      const dispose = this.api.follow(query, metadataOnly, {
        next: (batch: Batch) => { if (!this.current(generation) || this.value.phase !== 'live') return; try { if (!this.acceptLive(batch, generation)) return; if (batch.ended) { this.retire(); this.set({ phase: 'stopped', exhausted: true }); } } catch (error) { fail(error); } },
        error: fail,
        complete: () => { if (this.current(generation) && this.value.phase === 'live') { this.retire(); this.set({ phase: 'stopped' }); } },
      }, presentation);
      // A deterministic fixture may complete synchronously during subscribe.
      if (this.current(generation) && this.value.phase === 'live') this.stopStream = dispose; else dispose();
    } catch (error) { fail(error); }
  }
  reset() {
    const generation = ++this.generation; this.retire(); this.pages = []; this.evictedWarnings = false;
    this.set({ ...initialState(), catalog: this.value.catalog, catalogError: this.value.catalogError, statistics: this.value.statistics, statisticsError: this.value.statisticsError, cleanupWarning: this.value.cleanupWarning, generation, pageSize: this.pageSize });
  }
  stop() { ++this.generation; this.retire(); this.set({ phase: 'stopped', languageBusy: false, navigationTarget: null }); }
  dispose() { this.disposed = true; ++this.generation; this.retire(); this.catalogAbort.abort(); }
}
