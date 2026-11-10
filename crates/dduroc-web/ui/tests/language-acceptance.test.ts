// Independent language policy acceptance, including real packaged UI when opted in.
import { afterEach, expect, it, vi } from 'vitest';
import { get } from 'svelte/store';
import { ViewerController } from '../src/lib/controller';
import { exportHistory } from '../src/lib/export';
import { LANGUAGE_STORAGE_KEY, loadMessageLanguages, messageLanguageChoices, messageLanguageStatus, messagePresentation } from '../src/lib/languages';
import type { Catalog, Entry, Page, SessionPage, Transport } from '../src/lib/types';

const catalog: Catalog = { apiVersion: '1', defaultLanguages: ['en'], namespaces: [], schemas: [{ name: 'variants', version: '1', languages: ['en', 'RU_ru', 'ru-RU'], events: [], metrics: [] }] };
function entry(text = 'old'): Entry { return { namespace: 'bi', class: 'DEFAULT', kind: 'MESSAGE', at: { boot: '1', micros: '1' }, utc: null, span: null, message: { eventId: '1', name: 'Tick', level: 'INFO', tags: [], rendered: text, renderStatus: 'RENDERED', language: 'ru', languagePreferenceIndex: 1, fieldsStatus: 'NOT_REQUESTED', fields: null, payload: null } }; }
const page = (text: string): Page => ({ entries: [entry(text)], seeds: [], damaged: [], unanchored: [], exhausted: false, truncated: false });
function deferred<T>() { let resolve!: (value: T) => void; const promise = new Promise<T>(yes => { resolve = yes; }); return { promise, resolve }; }
function transport() { return {
  catalog: vi.fn<Transport['catalog']>().mockResolvedValue(catalog),
  open: vi.fn<Transport['open']>().mockResolvedValue({ id: 'a', revision: '0' }),
  page: vi.fn<Transport['page']>().mockResolvedValue({ revision: '1', page: page('new') }),
  cancel: vi.fn<Transport['cancel']>().mockResolvedValue(undefined),
  close: vi.fn<Transport['close']>().mockResolvedValue(undefined),
  follow: vi.fn<Transport['follow']>().mockReturnValue(vi.fn()),
}; }
const controllers: ViewerController[] = [];
const settle = async () => { for (let i = 0; i < 20; i++) await Promise.resolve(); };
afterEach(() => { for (const controller of controllers.splice(0)) controller.dispose(); vi.restoreAllMocks(); vi.unstubAllGlobals(); localStorage.clear(); });
it('preserves every exact schema code and a persisted case variant as visible choices', () => {
  expect(messageLanguageChoices(catalog, { primary: 'EN', fallback: '' })).toEqual(['EN', 'RU_ru', 'en', 'ru-RU']);
});
it('loads browser overrides safely and removes equivalent fallback duplicates', () => {
  localStorage.setItem(LANGUAGE_STORAGE_KEY, JSON.stringify({ primary: 'RU_ru', fallback: 'ru-RU' }));
  expect(loadMessageLanguages()).toEqual({ primary: 'RU_ru', fallback: '' });
  expect(messagePresentation({ primary: '', fallback: 'en' })).toBeUndefined();
  for (const value of ['{', JSON.stringify({ primary: 'x'.repeat(65), fallback: '' }), JSON.stringify({ primary: ['ru'], fallback: 'en' })]) {
    localStorage.setItem(LANGUAGE_STORAGE_KEY, value); expect(loadMessageLanguages()).toEqual({ primary: '', fallback: '' });
  }
  vi.spyOn(Storage.prototype, 'getItem').mockImplementation(() => { throw new Error('blocked'); });
  expect(loadMessageLanguages()).toEqual({ primary: '', fallback: '' });
});
it('distinguishes no decoder, unknown schema, primary and schema fallback without false labels', () => {
  const record = entry();
  record.message!.renderStatus = 'UNSUPPORTED_DECODER'; record.message!.rendered = null;
  expect(messageLanguageStatus(record)).toMatch(/unavailable/i); expect(messageLanguageStatus(record)).not.toMatch(/not requested/i);
  record.message!.renderStatus = 'UNKNOWN_SCHEMA'; expect(messageLanguageStatus(record)).toMatch(/Unknown schema/);
  record.message!.renderStatus = 'RENDERED'; record.message!.rendered = 'такт'; record.message!.languagePreferenceIndex = null;
  expect(messageLanguageStatus(record)).toContain('first schema language fallback');
});
it('changing languages retires a pending page, clears all cached pages and rejects stale results', async () => {
  const api = transport(); const late = deferred<SessionPage>(); api.page.mockResolvedValueOnce({ revision: '1', page: page('old first') }).mockReturnValueOnce(late.promise);
  const controller = new ViewerController(api); controllers.push(controller);
  await controller.run({}, false, { languages: ['en'] }); const pending = controller.next(); await settle();
  const signal = api.page.mock.calls[1][3]; controller.reset();
  expect(signal.aborted).toBe(true); expect(api.cancel).toHaveBeenCalledWith('a');
  expect(get(controller)).toMatchObject({ rows: [], cachedPages: 0, cachedRecords: 0, canPrevious: false, canNext: false });
  await controller.run({}, false, { languages: ['ru', 'en'] });
  late.resolve({ revision: '2', page: page('stale English') }); await pending;
  expect(get(controller).rows.map(row => row.entry.message?.rendered)).toEqual(['new']);
  expect(api.open.mock.calls.at(-1)?.[2]).toEqual({ languages: ['ru', 'en'] });
});
it('changing languages unsubscribes live and ignores every retired callback', () => {
  const api = transport(); const stop = vi.fn(); api.follow.mockReturnValue(stop);
  const controller = new ViewerController(api); controllers.push(controller);
  controller.live({}, false, { languages: ['en'] }); const old = api.follow.mock.calls[0][2];
  old.next({ ...page('old'), ended: false }); controller.reset();
  expect(stop).toHaveBeenCalledTimes(1); expect(get(controller).rows).toEqual([]);
  controller.live({}, false, { languages: ['ru', 'en'] }); old.next({ ...page('stale'), ended: false }); old.complete();
  api.follow.mock.calls[1][2].next({ ...page('new'), ended: false });
  expect(get(controller).rows.map(row => row.entry.message?.rendered)).toEqual(['new']);
  expect(api.follow.mock.calls[1][3]).toEqual({ languages: ['ru', 'en'] });
});
it('freezes export language order together with query and preserves actual language metadata', async () => {
  const api = transport(); const opening = deferred<{ id: string; revision: string }>(); api.open.mockReturnValue(opening.promise);
  api.page.mockResolvedValue({ revision: '1', page: { ...page('такт 7'), exhausted: true } });
  const presentation = { languages: ['zz', 'ru'] };
  const exporting = exportHistory(api, { query: {}, presentation, metadataOnly: false, format: 'jsonl', maxRecords: 100, maxBytes: 65536 }, new AbortController().signal);
  presentation.languages[0] = 'mutated'; presentation.languages.push('en'); opening.resolve({ id: 'export', revision: '0' });
  const result = await exporting;
  expect(api.open.mock.calls[0][2]).toEqual({ languages: ['zz', 'ru'] });
  const text = await new Promise<string>((resolve, reject) => { const reader = new FileReader(); reader.onload = () => resolve(String(reader.result)); reader.onerror = () => reject(reader.error); reader.readAsText(result.blob); });
  const lines = text.trim().split('\n').map(line => JSON.parse(line));
  expect(lines[0].data.presentation).toEqual({ languages: ['zz', 'ru'] });
  const record = lines.find(line => line.data.message)?.data;
  expect(record.message).toMatchObject({ rendered: 'такт 7', language: 'ru', languagePreferenceIndex: 1 });
});

it.runIf(!!process.env.DDUROC_WEB_TEST_URL)('uses real language controls for snapshot, reload, live, and JSONL export', async () => {
  const { chromium } = await import('playwright-core');
  const browser = await chromium.launch({ headless: true, executablePath: process.env.DDUROC_WEB_TEST_BROWSER });
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 1000 }, serviceWorkers: 'block' });
    const tab = await context.newPage(); const errors: string[] = []; const operations: { query: string; variables: Record<string, any> }[] = []; const frames: any[] = [];
    tab.on('pageerror', error => errors.push(error.message));
    tab.on('request', request => { if (request.method() === 'POST') operations.push(request.postDataJSON()); });
    tab.on('websocket', socket => socket.on('framesent', frame => { try { frames.push(JSON.parse(String(frame.payload))); } catch { /* Ignore non-JSON protocol frames. */ } }));
    await tab.goto(process.env.DDUROC_WEB_TEST_URL!, { waitUntil: 'networkidle' }); await tab.locator('.message-cell').first().waitFor();
    const primary = tab.getByRole('combobox', { name: 'Primary message language', exact: true }); const fallback = tab.getByRole('combobox', { name: 'Fallback language', exact: true });
    expect(await primary.inputValue()).toBe('');
    await primary.selectOption('ru'); await fallback.selectOption('en');
    await vi.waitFor(() => expect(operations.filter(op => op.query.includes('seekReadSession')).at(-1)?.variables.presentation).toEqual({ languages: ['ru', 'en'] }));
    await vi.waitFor(async () => expect(await tab.locator('.message-cell').allTextContents()).toEqual(expect.arrayContaining([expect.stringMatching(/такт|ошибка/)])));
    expect(operations.filter(op => op.query.includes('openCountedReadSession'))).toHaveLength(1);
    expect(await tab.locator('.message-cell').allTextContents()).toEqual(expect.arrayContaining([expect.stringMatching(/такт|ошибка/)]));
    await tab.reload({ waitUntil: 'networkidle' }); await tab.locator('.message-cell').first().waitFor();
    expect(await primary.inputValue()).toBe('ru'); expect(await fallback.inputValue()).toBe('en');
    // Keep live below the browser cap so the following unsubscribe proves the language change.
    const namespace = tab.getByRole('combobox', { name: 'Namespace', exact: true });
    await namespace.fill('radio-0'); await namespace.press('ArrowDown'); await namespace.press('Enter');
    expect(await namespace.inputValue()).toBe('radio-0');
    await tab.getByRole('button', { name: 'Start live', exact: true }).click();
    await vi.waitFor(() => expect(frames.find(frame => frame.type === 'subscribe')?.payload.variables.presentation).toEqual({ languages: ['ru', 'en'] }));
    expect(frames.some(frame => frame.type === 'complete')).toBe(false);
    await primary.selectOption('en');
    await vi.waitFor(() => expect(frames.some(frame => frame.type === 'complete')).toBe(true));
    await vi.waitFor(() => expect(frames.filter(frame => frame.type === 'subscribe').at(-1)?.payload.variables.presentation).toEqual({ languages: ['en'] }));
    await primary.selectOption('ru'); await fallback.selectOption('en');
    await tab.getByRole('combobox', { name: 'Namespace match', exact: true }).selectOption('all');
    await tab.getByRole('button', { name: 'Export history', exact: true }).click();
    await tab.getByRole('button', { name: 'Prepare export', exact: true }).click();
    await tab.getByRole('button', { name: 'Download file', exact: true }).waitFor();
    expect(operations.filter(op => op.query.includes('openReadSession')).at(-1)?.variables.presentation).toEqual({ languages: ['ru', 'en'] });
    const downloadPromise = tab.waitForEvent('download'); await tab.getByRole('button', { name: 'Download file', exact: true }).click();
    const download = await downloadPromise; const stream = await download.createReadStream(); const chunks: Buffer[] = []; for await (const chunk of stream) chunks.push(Buffer.from(chunk));
    const lines = Buffer.concat(chunks).toString('utf8').trim().split('\n').map(line => JSON.parse(line));
    expect(lines[0].data.presentation).toEqual({ languages: ['ru', 'en'] });
    expect(lines[0].data.query.namespaces).toBeUndefined();
    const messages = lines.filter(line => line.data?.message).map(line => line.data.message);
    expect(messages.length).toBeGreaterThan(20);
    for (const message of messages) { expect(message.language).toBe('ru'); expect(message.languagePreferenceIndex).toBe(0); expect(message.rendered).toMatch(/такт|ошибка/); }
    expect(lines.some(line => line.data?.text?.text === 'Example diagnostic')).toBe(true);
    expect(errors).toEqual([]);
    console.info(JSON.stringify({ browserLanguages: ['ru', 'en'], exportedMessages: messages.length, livePresentationVerified: true, rawTextUnchanged: true }));
    await context.close();
  } finally { await browser.close(); }
}, 60_000);
