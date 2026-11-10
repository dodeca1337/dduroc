// Independent acceptance of the public viewer controller and HTTP transport.
import { afterEach, describe, expect, it, vi } from 'vitest';
import { get } from 'svelte/store';
import { buildSchema, parse, validate, type SelectionSetNode } from 'graphql';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { ViewerController } from '../src/lib/controller';
import { ApiError, HttpTransport, loadBootstrap } from '../src/lib/api';
import { bootTime, floatText, scalarText } from '../src/lib/format';
import { defaultFilters, makeQuery } from '../src/lib/filters';
import type { Batch, Entry, Page, Session, SessionPage, Transport } from '../src/lib/types';

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}
const settle = async () => { for (let i = 0; i < 12; i++) await Promise.resolve(); };
const entry = (namespace = 'same'): Entry => ({ namespace, class: 'DEFAULT', at: { boot: '1', micros: '9007199254740993' }, utc: null, span: null, kind: 'TEXT', text: { level: 'INFO', target: 'test', text: 'identical' } });
const page = (entries: Entry[] = [], overrides: Partial<Page> = {}): Page => ({ entries, seeds: [], damaged: [], unanchored: [], exhausted: false, truncated: false, ...overrides });
const batch = (entries: Entry[] = [], overrides: Partial<Batch> = {}): Batch => ({ entries, seeds: [], damaged: [], unanchored: [], ended: false, ...overrides });
function transport() {
  return {
    catalog: vi.fn<Transport['catalog']>().mockResolvedValue({ apiVersion: '1', defaultLanguages: ['en'], namespaces: [], schemas: [] }),
    open: vi.fn<Transport['open']>().mockResolvedValue({ id: 'A', revision: '0' }),
    page: vi.fn<Transport['page']>().mockResolvedValue({ revision: '1', page: page() }),
    cancel: vi.fn<Transport['cancel']>().mockResolvedValue(undefined),
    close: vi.fn<Transport['close']>().mockResolvedValue(undefined),
    follow: vi.fn<Transport['follow']>().mockReturnValue(vi.fn()),
  };
}
const controllers: ViewerController[] = [];
function viewer(api: Transport, rows = 1000, bytes = 8 * 1024 * 1024) {
  const controller = new ViewerController(api, rows, bytes); controllers.push(controller); return controller;
}
afterEach(() => { for (const controller of controllers.splice(0)) controller.dispose(); vi.unstubAllGlobals(); });

describe('cursor lifecycle', () => {
  it('waits for stale pending-open cancellation acknowledgement before the replacement opens', async () => {
    const api = transport(); const opening = deferred<Session>(); const cancellation = deferred<void>();
    api.open.mockImplementationOnce(() => opening.promise).mockResolvedValueOnce({ id: 'B', revision: '0' });
    api.cancel.mockImplementationOnce(() => cancellation.promise);
    const controller = viewer(api);
    const first = controller.run({ namespaces: ['A'] }); await settle();
    const second = controller.run({ namespaces: ['B'] });
    opening.resolve({ id: 'A', revision: '0' }); await settle();
    expect(api.cancel).toHaveBeenCalledWith('A');
    expect(api.open).toHaveBeenCalledTimes(1);
    expect(api.page).not.toHaveBeenCalled();
    cancellation.resolve(); await Promise.all([first, second]);
    expect(api.open).toHaveBeenCalledTimes(2);
    expect(api.page.mock.calls[0][0].id).toBe('B');
  });
  it('waits for cancellation of an already acquired cursor and ignores its late page', async () => {
    const api = transport(); const oldPage = deferred<SessionPage>(); const cancellation = deferred<void>();
    api.page.mockImplementationOnce(() => oldPage.promise).mockResolvedValueOnce({ revision: '1', page: page([entry('new')]) });
    api.cancel.mockImplementationOnce(() => cancellation.promise);
    api.open.mockResolvedValueOnce({ id: 'A', revision: '0' }).mockResolvedValueOnce({ id: 'B', revision: '0' });
    const controller = viewer(api); const first = controller.run({}); await settle();
    const signal = api.page.mock.calls[0][3];
    const second = controller.run({ namespaces: ['new'] }); await settle();
    expect(signal.aborted).toBe(true); expect(api.open).toHaveBeenCalledTimes(1);
    cancellation.resolve(); await second;
    oldPage.resolve({ revision: '1', page: page([entry('old')]) }); await first;
    expect(get(controller).rows.map(row => row.entry.namespace)).toEqual(['new']);
  });
  it('waits for EOF close acknowledgement before opening a replacement cursor', async () => {
    const api = transport(); const closing = deferred<void>();
    api.open.mockResolvedValueOnce({ id: 'A', revision: '0' }).mockResolvedValueOnce({ id: 'B', revision: '0' });
    api.page.mockResolvedValueOnce({ revision: '1', page: page([], { exhausted: true }) });
    api.close.mockImplementationOnce(() => closing.promise);
    const controller = viewer(api); const first = controller.run({}); await settle();
    expect(api.close).toHaveBeenCalledWith({ id: 'A', revision: '1' });
    const second = controller.run({ namespaces: ['B'] }); await settle();
    expect(api.open).toHaveBeenCalledTimes(1);
    closing.resolve(); await Promise.all([first, second]);
    expect(api.open).toHaveBeenCalledTimes(2);
  });
  it('serializes page CAS and passes wide revisions without numeric conversion', async () => {
    const api = transport(); const pending = deferred<SessionPage>();
    api.page.mockResolvedValueOnce({ revision: '9007199254740993', page: page([entry()]) }).mockImplementationOnce(() => pending.promise);
    const controller = viewer(api); await controller.run({});
    const firstPageId = get(controller).rows[0].id;
    const next = controller.more(); const redundant = controller.more();
    expect(api.page).toHaveBeenCalledTimes(2);
    expect(api.page.mock.calls[1][0].revision).toBe('9007199254740993');
    pending.resolve({ revision: '9007199254740994', page: page([entry()], { exhausted: true }) });
    await Promise.all([next, redundant]);
    expect(api.close).toHaveBeenCalledWith({ id: 'A', revision: '9007199254740994' });
    expect(get(controller).rows).toHaveLength(1); expect(get(controller).cachedRecords).toBe(2);
    const secondPageId = get(controller).rows[0].id; expect(secondPageId).not.toBe(firstPageId);
    controller.previous(); expect(get(controller).rows[0].id).toBe(firstPageId);
    await controller.next(); expect(get(controller).rows[0].id).toBe(secondPageId);
    await controller.more(); expect(api.page).toHaveBeenCalledTimes(2);
  });
  it.each(['DEADLINE_EXCEEDED', 'REVISION_CONFLICT', 'SNAPSHOT_EXPIRED', 'RETENTION_GAP', 'PAGE_TOO_SMALL'])('does not retry ambiguous or invalid page %s', async code => {
    const api = transport(); api.page.mockRejectedValue(new ApiError(code, 'test failure'));
    const controller = viewer(api); await controller.run({}); await settle(); await controller.more();
    expect(api.page).toHaveBeenCalledTimes(1); expect(api.cancel).toHaveBeenCalledWith('A');
    expect(get(controller).phase).toBe('error'); expect(get(controller).error?.code).toBe(code);
    expect(get(controller).exhausted).toBe(false);
  });
  it('cancels a capability arriving after disposal instead of leaving a server orphan', async () => {
    const api = transport(); const opening = deferred<Session>(); api.open.mockReturnValue(opening.promise);
    const controller = viewer(api); const running = controller.run({}); await settle(); controller.dispose();
    opening.resolve({ id: 'late', revision: '0' }); await running;
    expect(api.cancel).toHaveBeenCalledWith('late'); expect(api.page).not.toHaveBeenCalled();
  });
});

describe('bounded live data and truthful status', () => {
  it('keeps seeds and identical records distinct and preserves damage and unanchored indicators', async () => {
    const api = transport(); api.page.mockResolvedValue({ revision: '1', page: page([entry(), entry()], { seeds: [entry('seed')], damaged: [{ offset: '18446744073709551615', message: 'damaged block' }], unanchored: ['4294967295'], exhausted: true, truncated: true }) });
    const controller = viewer(api); await controller.run({}); const state = get(controller);
    expect(state.rows).toHaveLength(2); expect(state.seeds).toHaveLength(1);
    expect(state.damaged[0].offset).toBe('18446744073709551615'); expect(state.unanchored).toEqual(['4294967295']);
    expect(state.exhausted).toBe(true); expect(state.truncated).toBe(true);
  });
  it('stops at the byte ceiling before replacing retained records or accepting more callbacks', () => {
    const api = transport(); const stop = vi.fn(); api.follow.mockReturnValue(stop);
    const small = batch([entry()]); const ceiling = new TextEncoder().encode(JSON.stringify(small)).byteLength + 4;
    const controller = viewer(api, 1000, ceiling); controller.live({});
    const callbacks = api.follow.mock.calls[0][2]; callbacks.next(small); callbacks.next(batch([entry('too-large')]));
    expect(get(controller).rows.map(row => row.entry.namespace)).toEqual(['same']);
    expect(get(controller).phase).toBe('limit'); expect(stop).toHaveBeenCalledTimes(1);
    callbacks.next(small); expect(get(controller).rows).toHaveLength(1);
  });
  it('counts seeds against the row ceiling and reports overflow without silently dropping history', () => {
    const api = transport(); const controller = viewer(api, 1); controller.live({});
    api.follow.mock.calls[0][2].next(batch([entry()], { seeds: [entry('seed')] }));
    expect(get(controller).phase).toBe('limit'); expect(get(controller).rows).toHaveLength(0);
    expect(get(controller).error?.message).toContain('resync');
  });
  it('requires resync after slow consumer and suppresses callbacks from an older live subscription', () => {
    const api = transport(); const stop = vi.fn(); api.follow.mockReturnValue(stop);
    const controller = viewer(api); controller.live({}); const old = api.follow.mock.calls[0][2];
    old.next(batch([entry('old')])); old.error(new ApiError('SLOW_CONSUMER', 'overflow'));
    expect(get(controller).phase).toBe('error'); expect(get(controller).error?.message).toContain('resync');
    expect(api.follow).toHaveBeenCalledTimes(1);
    controller.live({ namespaces: ['new'] }); old.next(batch([entry('stale')])); old.complete();
    api.follow.mock.calls[1][2].next(batch([entry('new')]));
    expect(get(controller).phase).toBe('live'); expect(get(controller).rows.map(row => row.entry.namespace)).toEqual(['new']);
  });
});

it('formats lossless integers, negative zero, nonfinite values and unexpanded nested data honestly', () => {
  expect(bootTime(entry())).toBe('Boot 1 · +9007199254.740993s');
  expect(scalarText({ kind: 'UNSIGNED', text: '340282366920938463463374607431768211455' })).toBe('340282366920938463463374607431768211455');
  expect(scalarText({ kind: 'SIGNED', text: '-170141183460469231731687303715884105728' })).toBe('-170141183460469231731687303715884105728');
  expect(floatText({ width: 64, class: 'FINITE', decimal: '-0', bits: '8000000000000000' })).toBe('−0');
  expect(floatText({ width: 32, class: 'NAN', decimal: '', bits: '7fc00042' })).toBe('NaN');
  expect(floatText({ width: 64, class: 'POSITIVE_INFINITY', decimal: '', bits: '7ff0000000000000' })).toBe('+Infinity');
  expect(scalarText({ kind: 'MAP' })).toBe('More nested fields');
  expect(scalarText({ kind: 'SEQUENCE', items: [] })).toBe('[0 values]');
});

it('validates exact boot-time filters without coercing u64 to Number', () => {
  const filters = { ...defaultFilters(), clock: 'boot' as const, boot: '4294967295', from: '9007199254740993', to: '18446744073709551615' };
  expect(makeQuery(filters).from).toEqual({ boot: '4294967295', micros: '9007199254740993' });
  expect(() => makeQuery({ ...filters, to: '18446744073709551616' })).toThrow();
  expect(() => makeQuery({ ...filters, boot: '01' })).toThrow();
  expect(() => makeQuery(filters, true)).toThrow(/end time/);
});

const bootstrap = { graphqlEndpoint: 'https://journal.test/logs/graphql', subscriptionEndpoint: 'wss://journal.test/logs/graphql/ws', title: 'Journal', maxRows: 1000 };
function jsonResponse(value: unknown) { return new Response(JSON.stringify(value), { headers: { 'content-type': 'application/json' } }); }
it('resolves host-configured endpoints from a subpath without adding external dependencies', async () => {
  const fetcher = vi.fn().mockResolvedValue(jsonResponse({ graphqlEndpoint: 'graphql', subscriptionEndpoint: 'graphql/ws' })); vi.stubGlobal('fetch', fetcher);
  expect(await loadBootstrap('https://journal.test/proxy/logs/')).toMatchObject({ graphqlEndpoint: 'https://journal.test/proxy/logs/graphql', subscriptionEndpoint: 'wss://journal.test/proxy/logs/graphql/ws' });
  expect(String(fetcher.mock.calls[0][0])).toBe('https://journal.test/proxy/logs/dduroc-config.json');
  fetcher.mockResolvedValueOnce(jsonResponse({ graphqlEndpoint: 'https://api.test/graphql', subscriptionEndpoint: 'wss://api.test/ws' }));
  expect(await loadBootstrap('https://journal.test/logs/')).toMatchObject({ graphqlEndpoint: 'https://api.test/graphql', subscriptionEndpoint: 'wss://api.test/ws' });
  for (const graphqlEndpoint of ['javascript:alert(1)', 'https://user:secret@api.test/graphql']) {
    fetcher.mockResolvedValueOnce(jsonResponse({ graphqlEndpoint })); await expect(loadBootstrap('https://journal.test/logs/')).rejects.toThrow();
  }
});

it('bounds HTTP response bytes while streaming and cancels the unread body', async () => {
  const cancel = vi.fn(); const body = new ReadableStream<Uint8Array>({ start(controller) { controller.enqueue(new Uint8Array(2 * 1024 * 1024 + 1)); }, cancel });
  vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(body)));
  await expect(new HttpTransport(bootstrap).open({})).rejects.toMatchObject({ code: 'VIEWER_LIMIT' });
  expect(cancel).toHaveBeenCalledTimes(1);
});

it('uses actual schema-valid HTTP operations within the default GraphQL selection depth', async () => {
  const documents: string[] = [];
  const fetcher = vi.fn().mockImplementation(async (_url, init) => {
    const request = JSON.parse(init.body as string); documents.push(request.query);
    if (request.query.includes('openReadSession')) return jsonResponse({ data: { openReadSession: { id: 'A', revision: '0' } } });
    if (request.query.includes('seekReadSession')) { expect(request.variables.targetPage).toBe('75'); expect(request.variables.presentation).toEqual({ languages: ['ru', 'en'] }); expect(request.variables.revision).toBe('9007199254740993'); return jsonResponse({ data: { seekReadSession: { revision: '9007199254740994', reachedPage: '75', targetReached: true, exhausted: false, page: page() } } }); }
    if (request.query.includes('readSession')) { expect(request.variables.revision).toBe('9007199254740993'); return jsonResponse({ data: { readSession: { revision: '9007199254740994', page: page() } } }); }
    if (request.query.includes('closeReadSession')) return jsonResponse({ data: { closeReadSession: true } });
    if (request.query.includes('cancelReadSession')) return jsonResponse({ data: { cancelReadSession: true } });
    return jsonResponse({ data: { catalog: { apiVersion: '1', defaultLanguages: ['en'], namespaces: [], schemas: [] } } });
  }); vi.stubGlobal('fetch', fetcher);
  const api = new HttpTransport(bootstrap);
  await api.catalog(); await api.open({}); await api.page({ id: 'A', revision: '9007199254740993' }, 64, false, new AbortController().signal); await api.seek({ id: 'A', revision: '9007199254740993' }, 75, 64, false, new AbortController().signal, { languages: ['ru', 'en'] }); await api.close({ id: 'A', revision: '1' }); await api.cancel('A');
  const schema = buildSchema(readFileSync(resolve(process.cwd(), '../../dduroc-graphql/schema.graphql'), 'utf8'));
  const depth = (selection: SelectionSetNode): number => Math.max(0, ...selection.selections.map(node => node.kind === 'Field' ? 1 + (node.selectionSet ? depth(node.selectionSet) : 0) : node.kind === 'InlineFragment' ? depth(node.selectionSet) : 0));
  for (const query of documents) {
    const document = parse(query); expect(validate(schema, document), query).toEqual([]);
    for (const definition of document.definitions) if (definition.kind === 'OperationDefinition') expect(depth(definition.selectionSet), query).toBeLessThanOrEqual(16);
  }
});

class ScriptedWebSocket {
  static readonly CONNECTING = 0; static readonly OPEN = 1; static readonly CLOSING = 2; static readonly CLOSED = 3;
  static instances: ScriptedWebSocket[] = [];
  readonly CONNECTING = 0; readonly OPEN = 1; readonly CLOSING = 2; readonly CLOSED = 3;
  readyState = 0; protocol = 'graphql-transport-ws'; binaryType = 'blob';
  onopen: ((event: Event) => unknown) | null = null;
  onmessage: ((event: MessageEvent) => unknown) | null = null;
  onclose: ((event: CloseEvent) => unknown) | null = null;
  onerror: ((event: Event) => unknown) | null = null;
  sent: { type: string; id?: string; payload?: { query: string } }[] = [];
  closes: number[] = [];
  constructor(readonly url: string, readonly protocols: string | string[]) {
    ScriptedWebSocket.instances.push(this);
    queueMicrotask(() => { this.readyState = 1; this.onopen?.(new Event('open')); });
  }
  send(value: string) {
    const message = JSON.parse(value); this.sent.push(message);
    if (message.type === 'connection_init') queueMicrotask(() => this.receive(JSON.stringify({ type: 'connection_ack' })));
  }
  receive(value: unknown) { this.onmessage?.(new MessageEvent('message', { data: value })); }
  close(code = 1000) {
    if (code !== 1000 && (code < 3000 || code > 4999)) throw new Error(`invalid browser close code ${code}`);
    this.closes.push(code);
    if (this.readyState === 3) return;
    this.readyState = 3; queueMicrotask(() => this.onclose?.(new CloseEvent('close', { code, wasClean: true })));
  }
}

it.each([
  ['ASCII', () => 'x'.repeat(2 * 1024 * 1024 + 1)],
  ['UTF-8', () => '界'.repeat(750_000)],
] as const)('rejects an oversized %s websocket frame before graphql-ws parses it and preserves the cause', async (_label, oversized) => {
  ScriptedWebSocket.instances = []; vi.stubGlobal('WebSocket', ScriptedWebSocket);
  const next = vi.fn(); const error = vi.fn(); const complete = vi.fn();
  const dispose = new HttpTransport(bootstrap).follow({}, false, { next, error, complete });
  try {
    await vi.waitFor(() => expect(ScriptedWebSocket.instances[0]?.sent.some(message => message.type === 'subscribe')).toBe(true));
    const socket = ScriptedWebSocket.instances[0];
    expect(socket.protocols).toBe('graphql-transport-ws');
    const subscribe = socket.sent.find(message => message.type === 'subscribe')!;
    const document = parse(subscribe.payload!.query);
    const schema = buildSchema(readFileSync(resolve(process.cwd(), '../../dduroc-graphql/schema.graphql'), 'utf8'));
    expect(validate(schema, document)).toEqual([]);
    const raw = oversized(); const parsing = vi.spyOn(JSON, 'parse');
    try {
      socket.receive(raw); await settle();
      expect(parsing.mock.calls.some(call => call[0] === raw)).toBe(false);
      expect(error).toHaveBeenCalledTimes(1); expect(error.mock.calls[0][0]).toMatchObject({ code: 'VIEWER_LIMIT' });
      expect(next).not.toHaveBeenCalled(); expect(complete).not.toHaveBeenCalled();
      expect(socket.readyState).toBe(ScriptedWebSocket.CLOSED); expect(ScriptedWebSocket.instances).toHaveLength(1);
      expect(socket.closes.every(code => code === 1000 || code >= 3000 && code <= 4999)).toBe(true);
    } finally { parsing.mockRestore(); }
  } finally { dispose(); }
});

it('forwards a bounded live batch losslessly and completes the modern websocket subscription on disposal', async () => {
  ScriptedWebSocket.instances = []; vi.stubGlobal('WebSocket', ScriptedWebSocket);
  const next = vi.fn(); const error = vi.fn(); const complete = vi.fn();
  const dispose = new HttpTransport(bootstrap).follow({}, false, { next, error, complete });
  try {
    await vi.waitFor(() => expect(ScriptedWebSocket.instances[0]?.sent.some(message => message.type === 'subscribe')).toBe(true));
    const socket = ScriptedWebSocket.instances[0]; const id = socket.sent.find(message => message.type === 'subscribe')!.id;
    socket.receive(JSON.stringify({ type: 'next', id, payload: { data: { follow: batch([entry()]) } } }));
    await settle(); expect(next.mock.calls[0][0].entries[0].at.micros).toBe('9007199254740993'); expect(error).not.toHaveBeenCalled();
    dispose(); await settle();
    expect(socket.sent.some(message => message.type === 'complete' && message.id === id)).toBe(true);
    expect(socket.readyState).toBe(ScriptedWebSocket.CLOSED);
  } finally { dispose(); }
});

it.runIf(!!process.env.DDUROC_WEB_TEST_URL)('loads the real embedded UI with cold-cache local fonts and real GraphQL HTTP/WS', async () => {
  const { chromium } = await import('playwright-core');
  const url = process.env.DDUROC_WEB_TEST_URL!;
  const browser = await chromium.launch({ headless: true, executablePath: process.env.DDUROC_WEB_TEST_BROWSER });
  try {
    const context = await browser.newContext({ viewport: { width: 1280, height: 900 }, serviceWorkers: 'block' });
    const page = await context.newPage();
    const cdp = await context.newCDPSession(page);
    await cdp.send('Network.enable'); await cdp.send('Network.setCacheDisabled', { cacheDisabled: true });
    const requests: { url: string; type: string }[] = [];
    const errors: string[] = []; const graphqlErrors: unknown[] = []; const responses: Promise<void>[] = [];
    const sent: string[] = []; const received: string[] = [];
    const wireEntries: Entry[] = []; const countedSnapshots: { totalMatches: string; totalPages: string }[] = []; let posts = 0;
    page.on('request', request => { if (request.method() === 'POST') posts++; });
    page.on('request', request => requests.push({ url: request.url(), type: request.resourceType() }));
    page.on('pageerror', error => errors.push(error.message));
    page.on('response', response => {
      if (response.request().method() === 'POST') responses.push((async () => {
        const payload = await response.json(); if (payload.errors?.length) graphqlErrors.push(payload.errors);
        if (payload.data?.openCountedReadSession) countedSnapshots.push(payload.data.openCountedReadSession);
        wireEntries.push(...((payload.data?.readSession ?? payload.data?.seekReadSession)?.page?.entries ?? []));
      })());
    });
    page.on('websocket', socket => {
      socket.on('framesent', event => { if (typeof event.payload === 'string') sent.push(event.payload); });
      socket.on('framereceived', event => { if (typeof event.payload === 'string') received.push(event.payload); });
    });
    const productionUrl = new URL(url); productionUrl.searchParams.set('demo', '1');
    await page.goto(productionUrl.href, { waitUntil: 'networkidle' });
    await page.getByRole('table').waitFor();
    expect(await page.getByText('Demo preview', { exact: true }).count()).toBe(0);
    expect(await page.getByRole('row').count()).toBeGreaterThan(1);
    expect(await page.getByRole('table').getAttribute('aria-label') || await page.getByRole('table').getAttribute('aria-labelledby')).toBeTruthy();
    await Promise.all(responses);
    expect(countedSnapshots).toHaveLength(1);
    expect(countedSnapshots[0]).toMatchObject({ totalMatches: '5030', totalPages: '79' });
    expect(await page.getByRole('spinbutton', { name: 'Page number, of 79 pages', exact: true }).inputValue()).toBe('1');
    expect(await page.getByRole('button', { name: 'Go', exact: true }).count()).toBe(0);
    expect(wireEntries.length).toBeGreaterThan(0);
    expect(wireEntries.every(entry => entry.utc !== null)).toBe(true);
    const display = page.getByLabel('Time display', { exact: true });
    expect(await display.inputValue()).toBe('calendar');
    const beforeToggle = await page.locator('tbody tr').allTextContents();
    const calendarLabel = (utc: string) => {
      const match = /^(.*)T([^.+Z]+)(?:\.(\d+))?(?:Z|\+00:00)$/.exec(utc)!;
      expect(match).not.toBeNull();
      return `${match[1]} ${match[2]}.${(match[3] ?? '').padEnd(6, '0')} UTC`;
    };
    for (const entry of wireEntries.slice(0, 5)) {
      expect(await page.getByRole('cell', { name: calendarLabel(entry.utc!), exact: true }).count()).toBeGreaterThan(0);
    }
    const postsBeforeToggle = posts; const framesBeforeToggle = sent.length;
    await display.selectOption('boot');
    for (const entry of wireEntries.slice(0, 5)) {
      const us = BigInt(entry.at.micros);
      const label = `Boot ${entry.at.boot} · +${us / 1_000_000n}.${(us % 1_000_000n).toString().padStart(6, '0')}s`;
      expect(await page.getByRole('cell', { name: label, exact: true }).count()).toBeGreaterThan(0);
    }
    await display.selectOption('calendar');
    expect(await page.locator('tbody tr').allTextContents()).toEqual(beforeToggle);
    expect(posts).toBe(postsBeforeToggle); expect(sent.length).toBe(framesBeforeToggle);
    const fontStacks = await page.evaluate(() => ({ sans: getComputedStyle(document.querySelector('h1')!).fontFamily, mono: getComputedStyle(document.querySelector('code')!).fontFamily }));
    await page.evaluate(async stacks => {
      const text = 'Journal 0123 Привет Ελληνικά ∞ −';
      const fixture = document.createElement('section'); fixture.id = 'acceptance-font-probe';
      fixture.setAttribute('aria-label', 'Acceptance font coverage');
      for (const [family, weight] of [[stacks.sans, '300'], [stacks.sans, '400'], [stacks.sans, '600'], [stacks.mono, '400']]) {
        const row = document.createElement('p'); row.textContent = text;
        row.style.fontFamily = family; row.style.fontWeight = weight; row.style.fontSize = '16px';
        fixture.append(row);
      }
      document.body.append(fixture); fixture.getBoundingClientRect(); await document.fonts.ready;
    }, fontStacks);
    const loadedFaces = await page.evaluate(() => Array.from(document.fonts).filter(face => face.status === 'loaded').map(face => ({ family: face.family.replaceAll('"', ''), weight: face.weight })));
    const sansFamily = fontStacks.sans.split(',')[0].replaceAll('"', '').trim();
    const monoFamily = fontStacks.mono.split(',')[0].replaceAll('"', '').trim();
    for (const [family, weight] of [[sansFamily, '300'], [sansFamily, '400'], [sansFamily, '600'], [monoFamily, '400']]) {
      expect(loadedFaces.some(face => face.family === family && face.weight === weight), `${family} ${weight}`).toBe(true);
    }
    await page.evaluate(() => document.getElementById('acceptance-font-probe')?.remove());
    const fontUrls = requests.filter(request => request.type === 'font').map(request => request.url);
    expect(fontUrls.length).toBeGreaterThanOrEqual(4);
    console.info(JSON.stringify({ fontRequests: fontUrls, fontStacks, loadedFaces }));
    const startLive = page.getByRole('button', { name: 'Start live', exact: true });
    if (!(await startLive.isVisible())) await page.getByRole('button', { name: /^Filters ·/ }).click();
    await startLive.click();
    await vi.waitFor(() => expect(received.some(frame => JSON.parse(frame).type === 'next')).toBe(true), { timeout: 10_000 });
    const batches = received.map(frame => JSON.parse(frame)).filter(message => message.type === 'next');
    expect(batches.some(message => message.payload?.data?.follow?.entries?.length > 0)).toBe(true);
    expect(batches.flatMap(message => message.payload?.errors ?? [])).toEqual([]);
    await page.getByRole('button', { name: 'Stop reading', exact: true }).click();
    await vi.waitFor(() => expect(sent.some(frame => JSON.parse(frame).type === 'complete')).toBe(true));
    await Promise.all(responses);
    expect(graphqlErrors).toEqual([]); expect(errors).toEqual([]);
    const unexpected = requests.filter(request => new URL(request.url).origin !== new URL(url).origin);
    console.info(JSON.stringify({ realServer: url, requests: requests.length, fonts: fontUrls, loadedFaces, graphqlErrors, javascriptErrors: errors, websocketNextFrames: batches.length, unexpectedRequests: unexpected }));
    expect(unexpected).toEqual([]);
    await context.close();
  } finally { await browser.close(); }
}, 30_000);
