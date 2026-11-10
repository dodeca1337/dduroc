// Independent finite-cursor pagination acceptance: current page, bounded cache,
// and stable row identities even when every timestamp and payload is identical.
import { afterEach, expect, it, vi } from 'vitest';
import { get } from 'svelte/store';
import { ViewerController } from '../src/lib/controller';
import { ApiError } from '../src/lib/api';
import type { Entry, Page, SessionPage, StorageStatistics, Transport } from '../src/lib/types';

const entry = (): Entry => ({ namespace: 'tied', class: 'DEFAULT', at: { boot: '1', micros: '9007199254740993' }, utc: null, span: null, kind: 'TEXT', text: { level: 'INFO', target: 'same', text: 'same' } });
const page = (entries: Entry[] = [], overrides: Partial<Page> = {}): Page => ({ entries, seeds: [], damaged: [], unanchored: [], exhausted: false, truncated: false, ...overrides });
function transport() {
  return { catalog: vi.fn<Transport['catalog']>(), open: vi.fn<Transport['open']>().mockResolvedValue({ id: 'snapshot', revision: '0' }),
    page: vi.fn<Transport['page']>(), cancel: vi.fn<Transport['cancel']>().mockResolvedValue(undefined), close: vi.fn<Transport['close']>().mockResolvedValue(undefined), follow: vi.fn<Transport['follow']>() };
}
function deferred<T>() { let resolve!: (value: T) => void; const promise = new Promise<T>(yes => { resolve = yes; }); return { promise, resolve }; }
const settle = async () => { for (let i = 0; i < 20; i++) await Promise.resolve(); };
const controllers: ViewerController[] = [];
function viewer(api: Transport, rows = 1000, bytes = 8 * 1024 * 1024, size = 64, pages = 5) { const value = new ViewerController(api, rows, bytes, size, pages); controllers.push(value); return value; }
afterEach(() => { for (const controller of controllers.splice(0)) controller.dispose(); });

it('traverses 5120 tied records using one cursor while retaining only five pages', async () => {
  const api = transport(); let offset = 0; let revision = 0n;
  api.page.mockImplementation(async (session, count) => {
    expect(session).toEqual({ id: 'snapshot', revision: revision.toString() });
    const length = Math.min(count, 5120 - offset); offset += length; revision++;
    return { revision: revision.toString(), page: page(Array.from({ length }, entry), { exhausted: offset === 5120 }) };
  });
  const controller = viewer(api); await controller.run({ order: 'OLDEST' });
  const seen = new Set<string>(); let pageNumber = 0;
  while (true) {
    const state = get(controller); pageNumber++;
    expect(state.page).toBe(pageNumber); expect(state.rows).toHaveLength(64);
    expect(state.cachedPages).toBeLessThanOrEqual(5); expect(state.cachedRecords).toBeLessThanOrEqual(320);
    expect(state.bytes).toBeLessThanOrEqual(8 * 1024 * 1024);
    for (const row of state.rows) { expect(seen.has(row.id)).toBe(false); seen.add(row.id); }
    if (state.exhausted) break;
    await controller.next();
  }
  expect(seen.size).toBe(5120); expect(get(controller).visitedRecords).toBe(5120);
  expect(get(controller)).toMatchObject({ page: 80, firstCachedPage: 76, lastCachedPage: 80, cachedPages: 5, canNext: false });
  expect(api.open).toHaveBeenCalledTimes(1); expect(api.page).toHaveBeenCalledTimes(80); expect(api.close).toHaveBeenCalledWith({ id: 'snapshot', revision: '80' });
  const finalIds = get(controller).rows.map(row => row.id);
  for (let i = 0; i < 5; i++) controller.previous();
  expect(get(controller).page).toBe(76); expect(get(controller).canPrevious).toBe(false);
  for (let i = 0; i < 4; i++) await controller.next();
  expect(get(controller).rows.map(row => row.id)).toEqual(finalIds);
  expect(api.open).toHaveBeenCalledTimes(1); expect(api.page).toHaveBeenCalledTimes(80);
});

it('uses cached Back/Next without RPC and serializes concurrent forward clicks', async () => {
  const api = transport(); const pending = deferred<SessionPage>();
  api.page.mockResolvedValueOnce({ revision: '9007199254740993', page: page([entry()]) })
    .mockResolvedValueOnce({ revision: '9007199254740994', page: page([entry()]) })
    .mockImplementationOnce(() => pending.promise);
  const controller = viewer(api); await controller.run({}); const firstId = get(controller).rows[0].id;
  await controller.next(); const secondId = get(controller).rows[0].id; expect(secondId).not.toBe(firstId);
  controller.previous(); expect(get(controller).rows[0].id).toBe(firstId);
  await controller.next(); expect(get(controller).rows[0].id).toBe(secondId); expect(api.page).toHaveBeenCalledTimes(2);
  const next = controller.next(); const duplicate = controller.next(); controller.previous();
  expect(get(controller)).toMatchObject({ page: 2, pageBusy: true }); expect(api.page).toHaveBeenCalledTimes(3);
  expect(api.page.mock.calls[2][0].revision).toBe('9007199254740994');
  pending.resolve({ revision: '9007199254740995', page: page([entry()], { exhausted: true }) }); await Promise.all([next, duplicate]);
  expect(get(controller)).toMatchObject({ page: 3, pageBusy: false, canNext: false });
});

it.each(['SESSION_EXPIRED', 'RETENTION_GAP', 'SNAPSHOT_EXPIRED'])('invalidates cached navigation after %s without reopening or retrying', async code => {
  const api = transport(); api.page.mockResolvedValueOnce({ revision: '1', page: page([entry()]) })
    .mockResolvedValueOnce({ revision: '2', page: page([entry()]) })
    .mockRejectedValueOnce(new ApiError(code, 'stopped'));
  const controller = viewer(api); await controller.run({}); await controller.next(); const lastId = get(controller).rows[0].id;
  await controller.next(); expect(get(controller).error?.code).toBe(code);
  expect(get(controller)).toMatchObject({ canNext: false, canPrevious: false, cachedPages: 0 });
  controller.previous(); await controller.next();
  expect(get(controller).rows[0].id).toBe(lastId); expect(get(controller).page).toBe(2);
  expect(api.page).toHaveBeenCalledTimes(3); expect(api.open).toHaveBeenCalledTimes(1);
});

it('accounts seed and diagnostic bytes and retains an explicit warning after their page is evicted', async () => {
  const api = transport(); api.page.mockResolvedValueOnce({ revision: '1', page: page([entry()], { seeds: [entry()], damaged: [{ offset: '0', message: 'damage' }], unanchored: ['1'] }) })
    .mockResolvedValueOnce({ revision: '2', page: page([entry()]) })
    .mockResolvedValueOnce({ revision: '3', page: page([entry()]) });
  const controller = viewer(api, 2); await controller.run({}); expect(get(controller).cachedRecords).toBe(2);
  await controller.next(); expect(get(controller)).toMatchObject({ cachedRecords: 1, firstCachedPage: 2, otherPageWarnings: true, visitedRecords: 2 });
  await controller.next(); expect(get(controller).cachedRecords).toBe(2); expect(get(controller).otherPageWarnings).toBe(true);
});

it('does not replace the current page with a response from a retired query', async () => {
  const api = transport(); const late = deferred<SessionPage>();
  api.open.mockResolvedValueOnce({ id: 'old', revision: '0' }).mockResolvedValueOnce({ id: 'new', revision: '0' });
  api.page.mockResolvedValueOnce({ revision: '1', page: page([entry()]) }).mockImplementationOnce(() => late.promise)
    .mockResolvedValueOnce({ revision: '1', page: page([{ ...entry(), namespace: 'new' }], { exhausted: true }) });
  const controller = viewer(api); await controller.run({}); const pending = controller.next(); await settle();
  await controller.run({ namespaces: ['new'] }); late.resolve({ revision: '2', page: page([entry()]) }); await pending;
  expect(get(controller).rows.map(row => row.entry.namespace)).toEqual(['new']); expect(get(controller).page).toBe(1);
  expect(api.cancel).toHaveBeenCalledWith('old');
});

it('evicts pages by encoded byte budget and refuses an oversized individual page without replacing the current one', async () => {
  const api = transport(); const one = page([entry()]);
  const ceiling = new TextEncoder().encode(JSON.stringify(one)).byteLength + 8;
  api.page.mockResolvedValueOnce({ revision: '1', page: one }).mockResolvedValueOnce({ revision: '2', page: one });
  const controller = viewer(api, 1000, ceiling); await controller.run({}); await controller.next();
  expect(get(controller)).toMatchObject({ page: 2, firstCachedPage: 2, lastCachedPage: 2, cachedPages: 1, cachedRecords: 1 });
  expect(get(controller).bytes).toBeLessThanOrEqual(ceiling);
  const retainedId = get(controller).rows[0].id; const large = entry(); large.text!.text = 'too large'.repeat(100);
  api.page.mockResolvedValueOnce({ revision: '3', page: page([large]) }); await controller.next();
  expect(get(controller).phase).toBe('limit'); expect(get(controller).rows[0].id).toBe(retainedId);
  expect(get(controller).page).toBe(2); expect(get(controller).canNext).toBe(false);
});

it.runIf(!!process.env.DDUROC_WEB_TEST_URL)('pages through more than 5000 real records with bounded DOM and cached backward navigation', async () => {
  const { chromium } = await import('playwright-core');
  const url = process.env.DDUROC_WEB_TEST_URL!;
  const browser = await chromium.launch({ headless: true, executablePath: process.env.DDUROC_WEB_TEST_BROWSER });
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 1000 }, serviceWorkers: 'block' });
    const page = await context.newPage();
    const operations: { query: string; variables: Record<string, unknown> }[] = [];
    const counts: { totalMatches: string; totalSeeds: string; totalPages: string }[] = [];
    const pages: SessionPage[] = []; const statisticSnapshots: StorageStatistics[] = []; const errors: unknown[] = []; const responses: Promise<void>[] = [];
    page.on('pageerror', error => errors.push(error.message));
    page.on('request', request => { if (request.method() === 'POST') operations.push(request.postDataJSON()); });
    page.on('response', response => {
      if (response.request().method() === 'POST') responses.push((async () => {
        const json = await response.json(); if (json.errors?.length) errors.push(json.errors);
        if (json.data?.seekReadSession?.page) pages.push(json.data.seekReadSession);
        if (json.data?.statistics) statisticSnapshots.push(json.data.statistics);
        if (json.data?.openCountedReadSession) counts.push(json.data.openCountedReadSession);
      })());
    });
    await page.goto(url, { waitUntil: 'networkidle' }); await page.getByRole('table').waitFor(); await Promise.all(responses);
    expect(pages).toHaveLength(1);
    expect(counts).toHaveLength(1);
    expect(counts[0]).toMatchObject({ totalMatches: '5030', totalSeeds: '0', totalPages: '79' });
    expect(await page.getByRole('spinbutton', { name: 'Page number, of 79 pages', exact: true }).inputValue()).toBe('1');
    expect(await page.getByRole('button', { name: 'Go', exact: true }).count()).toBe(0);
    const stats = page.getByRole('region', { name: 'Global storage statistics' });
    await stats.waitFor(); expect(await stats.innerText()).toContain('5,030');
    const rootStats = statisticSnapshots.at(-1)!.roots;
    expect(rootStats?.length).toBeGreaterThan(0);
    const allocated = rootStats!.reduce((total, root) => total + BigInt(root.allocatedBytes), 0n);
    expect(await stats.getByTitle(`${allocated} bytes allocated across storage roots`, { exact: true }).count()).toBe(1);
    expect(await stats.innerText()).toContain('Allocated disk');
    expect(await stats.innerText()).toContain('Storage budget');
    await page.getByRole('button', { name: /^Filters ·/ }).click();
    const combo = page.getByRole('combobox', { name: 'Namespace', exact: true });
    const beforeTyping = operations.length;
    await combo.fill('radio-'); await combo.press('ArrowDown'); await combo.press('Enter');
    expect(['radio-0', 'radio-1']).toContain(await combo.inputValue());
    expect(operations).toHaveLength(beforeTyping);
    await combo.press('Escape');
    const next = page.getByRole('button', { name: 'Next page', exact: true });
    const previous = page.getByRole('button', { name: 'Previous page', exact: true });
    let total = 0; const identities = new Set<string>(); let number = 0;
    while (true) {
      const wire = pages.at(-1)!; number++;
      total += wire.page.entries.length;
      for (const entry of wire.page.entries) {
        const key = JSON.stringify(entry);
        expect(identities.has(key), `duplicate real record ${key}`).toBe(false); identities.add(key);
      }
      expect(await page.locator('tbody tr:visible').count()).toBeLessThanOrEqual(64);
      expect(await page.locator('tbody tr').count()).toBeLessThanOrEqual(128); // Carbon keeps one hidden detail row per record.
      const cache = await page.locator('.cache-note').innerText();
      const match = /Cached pages ([\d, ]+) · (\d+) records/.exec(cache)!;
      expect(match).not.toBeNull(); expect(match[1].split(',').length).toBeLessThanOrEqual(5);
      expect(Number(match[2])).toBeLessThanOrEqual(320);
      if (wire.page.exhausted) break;
      const oldLength = pages.length; await next.click();
      await vi.waitFor(() => expect(pages).toHaveLength(oldLength + 1), { timeout: 10_000 });
      await vi.waitFor(async () => expect(await page.locator('.bx--data-table-header__description').innerText()).toContain(`Page ${number + 1} ·`));
    }
    expect(total).toBe(5030); expect(number).toBe(79);
    await vi.waitFor(async () => expect(await next.isDisabled()).toBe(true));
    const finalRows = await page.locator('tbody tr').allTextContents(); const rpcBeforeBack = operations.length;
    for (let i = 0; i < 4; i++) await previous.click();
    expect(await previous.isDisabled()).toBe(false); // Evicted history remains addressable in the same snapshot.
    for (let i = 0; i < 4; i++) await next.click();
    expect(await page.locator('tbody tr').allTextContents()).toEqual(finalRows);
    expect(operations).toHaveLength(rpcBeforeBack);
    expect(operations.filter(operation => operation.query.includes('openCountedReadSession'))).toHaveLength(1);
    const cas = operations.filter(operation => operation.query.includes('seekReadSession('));
    expect(cas).toHaveLength(number);
    expect(cas.map(operation => operation.variables.revision)).toEqual(Array.from({ length: number }, (_, i) => String(i)));
    await Promise.all(responses); expect(errors).toEqual([]);
    // Filter totals are query-specific, while the statistics bar remains global.
    await combo.fill('radio-1'); await combo.press('ArrowDown'); await combo.press('Enter');
    expect(await combo.inputValue()).toBe('radio-1');
    await page.getByRole('button', { name: 'Run query', exact: true }).click();
    await vi.waitFor(async () => expect(await page.locator('.results-footer').innerText()).toContain('3 matching records · 1 page'));
    expect(counts.at(-1)).toMatchObject({ totalMatches: '3', totalSeeds: '0', totalPages: '1' });
    expect(await page.getByRole('spinbutton', { name: 'Page number, of 1 page', exact: true }).inputValue()).toBe('1');
    expect(await next.isDisabled()).toBe(true);
    expect(await stats.innerText()).toContain('5,030');
    await page.getByRole('combobox', { name: 'Namespace match', exact: true }).selectOption('all');
    await vi.waitFor(async () => expect(await combo.inputValue()).toBe(''));
    await page.getByRole('button', { name: 'Run query', exact: true }).click();
    await vi.waitFor(async () => expect(await page.getByRole('spinbutton', { name: 'Page number, of 79 pages', exact: true }).inputValue()).toBe('1'));
    expect(counts.at(-1)).toMatchObject({ totalMatches: '5030', totalSeeds: '0', totalPages: '79' });
    expect(operations.filter(operation => operation.query.includes('openCountedReadSession')).at(-1)?.variables.query).not.toHaveProperty('namespaces');
    await Promise.all(responses); expect(errors).toEqual([]);
    console.info(JSON.stringify({ realServer: url, records: total, pages: number, maxCachedPages: 5, maxVisibleRecordRows: 64, maxDomRowsIncludingDetails: 128, cachedNavigationAdditionalRpc: 0, filteredRecords: 3, allNamespacesResetRecords: 5030 }));
    await context.close();
  } finally { await browser.close(); }
}, 90_000);
