// Independent acceptance of paged exports, exact formats and owned cursor cleanup.
import { expect, it, vi } from 'vitest';
import { ApiError } from '../src/lib/api';
import { exportHistory, type ExportOptions } from '../src/lib/export';
import type { Entry, Page, Session, SessionPage, Transport } from '../src/lib/types';

const record = (namespace = 'radio'): Entry => ({ namespace, class: 'CRITICAL', at: { boot: '4294967295', micros: '18446744073709551615' }, utc: '2026-11-10T09:41:20.123456+00:00', span: null, kind: 'TEXT', text: { level: 'WARN', target: 'fixture', text: 'exact' } });
const page = (entries: Entry[] = [], overrides: Partial<Page> = {}): Page => ({ entries, seeds: [], damaged: [], unanchored: [], exhausted: false, truncated: false, ...overrides });
const options = (overrides: Partial<ExportOptions> = {}): ExportOptions => ({ query: {}, format: 'jsonl', metadataOnly: false, maxRecords: 100, maxBytes: 1024 * 1024, ...overrides });
function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}
const settle = async () => { for (let i = 0; i < 20; i++) await Promise.resolve(); };
function transport() {
  return {
    catalog: vi.fn<Transport['catalog']>(),
    open: vi.fn<Transport['open']>().mockResolvedValue({ id: 'export-only', revision: '0' }),
    page: vi.fn<Transport['page']>().mockResolvedValue({ revision: '1', page: page([], { exhausted: true }) }),
    close: vi.fn<Transport['close']>().mockResolvedValue(undefined),
    cancel: vi.fn<Transport['cancel']>().mockResolvedValue(undefined),
    follow: vi.fn<Transport['follow']>(),
  };
}

// An independent RFC4180 reader: quoted commas/newlines are not record boundaries.
function csvRows(text: string): string[][] {
  const rows: string[][] = []; let row: string[] = []; let field = ''; let quoted = false;
  for (let i = 0; i < text.length; i++) {
    const char = text[i];
    if (quoted) {
      if (char === '"' && text[i + 1] === '"') { field += '"'; i++; }
      else if (char === '"') quoted = false;
      else field += char;
    } else if (char === '"') quoted = true;
    else if (char === ',') { row.push(field); field = ''; }
    else if (char === '\r' && text[i + 1] === '\n') { row.push(field); rows.push(row); row = []; field = ''; i++; }
    else if (char === '\n') throw new Error('CSV record separators must be CRLF');
    else field += char;
  }
  expect(quoted).toBe(false);
  if (field || row.length) { row.push(field); rows.push(row); }
  return rows;
}

it('opens an independent frozen filtered query and follows exact page revisions through EOF', async () => {
  const api = transport(); const opening = deferred<Session>(); api.open.mockReturnValue(opening.promise);
  api.page.mockResolvedValueOnce({ revision: '9007199254740993', page: page([record('A')]) })
    .mockResolvedValueOnce({ revision: '9007199254740994', page: page([record('B')], { exhausted: true }) });
  const query = { namespaces: ['A', 'B'], classes: ['CRITICAL' as const], order: 'OLDEST' as const, from: { utc: '2026-11-10T00:00:00Z' }, to: { utc: '2026-11-11T00:00:00Z' }, filter: { eventNames: ['Fault'], anyTags: ['rf'] } };
  const expected = structuredClone(query); const progress = vi.fn();
  const running = exportHistory(api, options({ query }), new AbortController().signal, progress);
  query.namespaces.push('mutated'); query.filter.anyTags[0] = 'mutated';
  opening.resolve({ id: 'export-only', revision: '0' });
  const result = await running;
  expect(api.open).toHaveBeenCalledTimes(1); expect(api.open.mock.calls[0][0]).toEqual(expected);
  expect(api.page.mock.calls.map(call => call[0])).toEqual([{ id: 'export-only', revision: '0' }, { id: 'export-only', revision: '9007199254740993' }]);
  expect(api.close).toHaveBeenCalledWith({ id: 'export-only', revision: '9007199254740994' });
  expect(api.follow).not.toHaveBeenCalled(); expect(api.catalog).not.toHaveBeenCalled();
  expect(result.complete).toBe(true); expect(result.records).toBe(2); expect(result.bytes).toBe(result.blob.size);
  expect(progress).toHaveBeenCalled();
  const counts = progress.mock.calls.map(([value]) => value.records);
  expect(counts).toEqual([...counts].sort((a, b) => a - b));
});

it('does not settle cancellation before a late-open capability is cancelled and acknowledged', async () => {
  const api = transport(); const opening = deferred<Session>(); const cancellation = deferred<void>();
  api.open.mockReturnValue(opening.promise); api.cancel.mockReturnValue(cancellation.promise);
  const abort = new AbortController(); let settled = false;
  const running = exportHistory(api, options(), abort.signal, vi.fn()).then(() => { settled = true; }, () => { settled = true; });
  abort.abort(); await settle(); expect(settled).toBe(false);
  opening.resolve({ id: 'late', revision: '0' }); await settle();
  expect(api.cancel).toHaveBeenCalledWith('late'); expect(api.page).not.toHaveBeenCalled(); expect(settled).toBe(false);
  cancellation.resolve(); await running; expect(settled).toBe(true);
});

it('ignores a late page after abort and retains cleanup ownership until acknowledgement', async () => {
  const api = transport(); const pending = deferred<SessionPage>(); const cancellation = deferred<void>();
  api.page.mockReturnValue(pending.promise); api.cancel.mockReturnValue(cancellation.promise);
  const abort = new AbortController(); const progress = vi.fn(); let settled = false;
  const running = exportHistory(api, options(), abort.signal, progress).then(() => { settled = true; }, () => { settled = true; });
  await settle(); expect(api.page).toHaveBeenCalledTimes(1); const pageSignal = api.page.mock.calls[0][3];
  abort.abort(); expect(pageSignal.aborted).toBe(true);
  pending.resolve({ revision: '1', page: page([record('late')], { exhausted: true }) }); await settle();
  expect(api.cancel).toHaveBeenCalledWith('export-only'); expect(settled).toBe(false);
  expect(progress.mock.calls.every(([value]) => value.records === 0)).toBe(true);
  cancellation.resolve(); await running; expect(api.close).not.toHaveBeenCalled();
});

it('never opens a cursor when cancellation already happened', async () => {
  const api = transport(); const abort = new AbortController(); abort.abort();
  await expect(exportHistory(api, options(), abort.signal, vi.fn())).rejects.toBeDefined();
  expect(api.open).not.toHaveBeenCalled();
});

function blobText(blob: Blob): Promise<string> {
  return new Promise((resolve, reject) => { const reader = new FileReader(); reader.onload = () => resolve(String(reader.result)); reader.onerror = () => reject(reader.error); reader.readAsText(blob); });
}
async function jsonLines(blob: Blob) { return (await blobText(blob)).trimEnd().split('\n').map(line => JSON.parse(line)); }
const wideMessage = (): Entry => ({ ...record('="cmd",\r\nПривет Ελληνικά'), kind: 'MESSAGE', text: null, message: {
  eventId: '18446744073709551615', name: 'Wide', level: 'INFO', tags: ['RF'], rendered: 'line one, "quoted"\r\nline two λ', renderStatus: 'RENDERED', language: 'en', payload: { base64: 'AP8=', bytes: '2' }, fieldsStatus: 'DECODED', fields: { kind: 'SEQUENCE', items: [
    { kind: 'UNSIGNED', text: '340282366920938463463374607431768211455' },
    { kind: 'SIGNED', text: '-170141183460469231731687303715884105728' },
    { kind: 'FLOAT', float: { width: 64, class: 'FINITE', decimal: '-0', bits: '8000000000000000' } },
    { kind: 'FLOAT', float: { width: 32, class: 'NAN', decimal: '', bits: '7fc00042' } },
    { kind: 'BYTES', bytes: { base64: 'AP8=', bytes: '2' } },
  ] },
} });

it('writes parseable lossless JSONL with declared query/projection and a truthful summary', async () => {
  const api = transport(); const exact = wideMessage();
  api.page.mockResolvedValue({ revision: '1', page: page([exact], { seeds: [record('seed')], exhausted: true }) });
  const query = { namespaces: [exact.namespace], order: 'NEWEST' as const, seedStates: true };
  const result = await exportHistory(api, options({ query }), new AbortController().signal);
  const lines = await jsonLines(result.blob);
  expect(lines.map(line => line.type)).toEqual(['metadata', 'seed', 'entry', 'summary']);
  expect(lines[0].data).toMatchObject({ version: 1, query, metadataOnly: false, fieldProjectionDepth: 2, encoding: 'UTF-8' });
  expect(lines[2].data).toEqual(exact); expect(lines[3].data).toMatchObject({ records: 2, complete: true, warnings: [] });
  expect(result.blob.type).toContain('application/x-ndjson'); expect(result.bytes).toBe(result.blob.size);
});

it('quotes CSV commas, CRLF and Unicode while preserving exact values in data_json', async () => {
  const api = transport(); const exact = wideMessage();
  const entries = [exact, ...['=1+1', '+SUM(A1)', '-1+2', '@command', ' \t=1+1', '\ttext'].map(name => record(name))];
  api.page.mockResolvedValue({ revision: '1', page: page(entries, { exhausted: true }) });
  const result = await exportHistory(api, options({ format: 'csv' }), new AbortController().signal);
  const text = await blobText(result.blob); const rows = csvRows(text);
  expect(rows[0]).toEqual(['record_type', 'namespace', 'class', 'utc', 'boot', 'micros', 'kind', 'data_json']);
  expect(rows.every(row => row.length === 8)).toBe(true);
  const exported = rows.filter(row => row[0] === 'entry');
  expect(exported.map(row => JSON.parse(row[7]))).toEqual(entries);
  for (const [index, source] of entries.entries()) {
    expect(exported[index][1]).toBe(`'${source.namespace}`);
    expect(exported[index][4]).toBe('4294967295'); expect(exported[index][5]).toBe("'18446744073709551615");
  }
  expect(text).toContain('""cmd""'); expect(result.blob.type).toContain('text/csv'); expect(result.complete).toBe(true);
});

it('retains damage, unanchored runs and field-unavailable diagnostics rather than claiming completeness', async () => {
  const api = transport(); const exact = wideMessage();
  exact.message!.name = null; exact.message!.fieldsStatus = 'UNKNOWN_SCHEMA'; exact.message!.fields = { kind: 'SEQUENCE' };
  api.page.mockResolvedValue({ revision: '1', page: page([exact], { damaged: [{ offset: '18446744073709551615', message: 'bad block' }], unanchored: ['4294967295'], exhausted: true, truncated: true }) });
  const result = await exportHistory(api, options(), new AbortController().signal); const lines = await jsonLines(result.blob);
  expect(lines.find(line => line.type === 'damage').data.offset).toBe('18446744073709551615');
  expect(lines.find(line => line.type === 'unanchored').data.boot).toBe('4294967295');
  expect(lines.find(line => line.type === 'entry').data).toEqual(exact);
  expect(result.complete).toBe(false); expect(result.filename).toContain('.partial.');
  for (const pattern of [/damaged/, /UTC anchor/, /projection depth/, /fields/]) expect(result.warnings.join(' ')).toMatch(pattern);
  expect(lines.at(-1).data.complete).toBe(false);
});

it('counts state seeds against the row ceiling and never pretends a capped export reached EOF', async () => {
  const api = transport(); api.page.mockResolvedValue({ revision: '1', page: page([record('entry')], { seeds: [record('seed')], exhausted: true }) });
  const result = await exportHistory(api, options({ maxRecords: 1 }), new AbortController().signal);
  expect(result.records).toBe(1); expect(result.complete).toBe(false); expect(api.page).toHaveBeenCalledTimes(1);
  const lines = await jsonLines(result.blob);
  expect(lines.filter(line => line.type === 'entry')).toHaveLength(0); expect(lines.filter(line => line.type === 'seed')).toHaveLength(1);
  expect(lines.at(-1).data.complete).toBe(false); expect(result.warnings.join(' ')).toMatch(/record limit/);
});

it('bounds actual UTF-8 output bytes including metadata and summary and never splits a JSONL record', async () => {
  const api = transport(); const huge = record(); huge.text!.text = '界'.repeat(4000);
  api.page.mockResolvedValue({ revision: '1', page: page([record('fits'), huge], { exhausted: true }) });
  const result = await exportHistory(api, options({ maxBytes: 8192 }), new AbortController().signal);
  expect(result.bytes).toBe(result.blob.size); expect(result.bytes).toBeLessThanOrEqual(8192);
  expect(result.complete).toBe(false); expect(result.records).toBe(1); expect(result.filename).toContain('.partial.');
  const lines = await jsonLines(result.blob); expect(lines.at(-1).type).toBe('summary'); expect(lines.filter(line => line.type === 'entry')).toHaveLength(1);
});

it('preserves an explicit metadata-only scope without inventing unavailable payload', async () => {
  const api = transport(); const source = record(); source.text = { level: 'WARN', target: 'fixture' } as Entry['text'];
  api.page.mockResolvedValue({ revision: '1', page: page([source], { exhausted: true }) });
  const result = await exportHistory(api, options({ metadataOnly: true }), new AbortController().signal); const lines = await jsonLines(result.blob);
  expect(lines[0].data.metadataOnly).toBe(true); expect(lines[1].data).toEqual(source);
  expect(api.open.mock.calls[0][1]).toBe(true); expect(api.page.mock.calls[0][2]).toBe(true);
});

it('awaits fallback cancellation after an ambiguous close before resolving a completed result', async () => {
  const api = transport(); const cancellation = deferred<void>(); api.close.mockRejectedValue(new Error('response lost')); api.cancel.mockReturnValue(cancellation.promise);
  let settled = false; const running = exportHistory(api, options(), new AbortController().signal).then(result => { settled = true; return result; });
  await settle(); expect(api.cancel).toHaveBeenCalledWith('export-only'); expect(settled).toBe(false);
  cancellation.resolve(); const result = await running; expect(result.complete).toBe(true);
});

it('marks a server page failure and unconfirmed cleanup as partial, with no silent retry', async () => {
  const api = transport(); api.page.mockResolvedValueOnce({ revision: '1', page: page([record()]) }).mockRejectedValueOnce(new Error('RETENTION_GAP'));
  api.cancel.mockRejectedValue(new Error('offline'));
  const result = await exportHistory(api, options(), new AbortController().signal);
  expect(result.complete).toBe(false); expect(result.records).toBe(1); expect(api.page).toHaveBeenCalledTimes(2);
  expect(result.warnings.join(' ')).toContain('RETENTION_GAP'); expect(result.warnings.join(' ')).toContain('confirmed closed');
});

it.each([{ maxRecords: 0 }, { maxRecords: Number.MAX_SAFE_INTEGER }, { maxBytes: 1 }, { maxBytes: Infinity }])('rejects invalid resource limits before opening a server cursor: %j', async invalid => {
  const api = transport(); await expect(exportHistory(api, options(invalid), new AbortController().signal)).rejects.toBeDefined(); expect(api.open).not.toHaveBeenCalled();
});

it('does not call unrestricted history incomplete merely because returned rows have no UTC anchor', async () => {
  const api = transport(); const unanchored = record(); unanchored.utc = null;
  api.page.mockResolvedValue({ revision: '1', page: page([unanchored], { exhausted: true }) });
  const result = await exportHistory(api, options(), new AbortController().signal);
  expect(result.complete).toBe(true); expect(result.records).toBe(1);
  const lines = await jsonLines(result.blob); expect(lines.find(line => line.type === 'entry').data.utc).toBeNull();
  expect(lines.at(-1).data.complete).toBe(true);
});

it('cannot erase a previous page truncation when the final page says exhausted without truncation', async () => {
  const api = transport(); api.page.mockResolvedValueOnce({ revision: '1', page: page([record('first')], { truncated: true }) })
    .mockResolvedValueOnce({ revision: '2', page: page([record('last')], { exhausted: true }) });
  const result = await exportHistory(api, options(), new AbortController().signal);
  expect(result.complete).toBe(false); expect(result.warnings.join(' ')).toContain('truncated');
  expect((await jsonLines(result.blob)).at(-1).data.complete).toBe(false);
});

it('distinguishes cancelled work whose server cleanup failed from an acknowledged cancellation', async () => {
  const api = transport(); const opening = deferred<Session>(); api.open.mockReturnValue(opening.promise); api.cancel.mockRejectedValue(new Error('offline'));
  const abort = new AbortController(); const running = exportHistory(api, options(), abort.signal);
  abort.abort(); opening.resolve({ id: 'late', revision: '0' });
  await expect(running).rejects.toMatchObject({ name: 'ExportCleanupError' });
  expect(api.cancel).toHaveBeenCalledTimes(1);
});

it.runIf(!!process.env.DDUROC_WEB_TEST_URL)('downloads real filtered CSV/JSONL and persists the header light/dark theme without unexpected network', async () => {
  const { chromium } = await import('playwright-core');
  const url = process.env.DDUROC_WEB_TEST_URL!;
  const browser = await chromium.launch({ headless: true, executablePath: process.env.DDUROC_WEB_TEST_BROWSER });
  try {
    const context = await browser.newContext({ acceptDownloads: true, viewport: { width: 1440, height: 1000 }, serviceWorkers: 'block', colorScheme: 'dark' });
    const page = await context.newPage(); const cdp = await context.newCDPSession(page);
    await cdp.send('Network.enable'); await cdp.send('Network.setCacheDisabled', { cacheDisabled: true });
    const requests: string[] = []; const operations: { query: string; variables: Record<string, unknown> }[] = [];
    const wireEntries: Entry[] = []; const responses: Promise<void>[] = []; const failures: unknown[] = [];
    let downloads = 0;
    page.on('download', () => downloads++); page.on('pageerror', error => failures.push(error.message));
    page.on('request', request => { requests.push(request.url()); if (request.method() === 'POST') operations.push(request.postDataJSON()); });
    page.on('response', response => {
      if (response.request().method() === 'POST') responses.push((async () => {
        const payload = await response.json(); if (payload.errors?.length) failures.push(payload.errors);
        wireEntries.push(...((payload.data?.readSession ?? payload.data?.seekReadSession)?.page?.entries ?? []));
      })());
    });
    await page.goto(url, { waitUntil: 'networkidle' }); await page.getByRole('table').waitFor(); await Promise.all(responses);
    expect(operations.filter(operation => operation.query.includes('openCountedReadSession'))).toHaveLength(1);
    expect(await page.getByRole('spinbutton', { name: 'Page number, of 79 pages', exact: true }).inputValue()).toBe('1');
    expect(await page.getByRole('button', { name: 'Go', exact: true }).count()).toBe(0);
    const fault = wireEntries.find(entry => entry.namespace === 'radio-1' && entry.message?.name === 'Fault');
    expect(fault?.utc).toBeTruthy();
    const theme = page.getByRole('button', { name: 'Dark theme', exact: true });
    expect(await theme.getAttribute('aria-pressed')).toBe('false');
    const operationsBeforeTheme = operations.length; const backgrounds: string[] = [];
    for (const value of ['g100', 'white', 'g100']) {
      await theme.click();
      await vi.waitFor(async () => expect(await page.evaluate(() => document.documentElement.getAttribute('theme'))).toBe(value));
      expect(await theme.getAttribute('aria-pressed')).toBe(value === 'g100' ? 'true' : 'false');
      backgrounds.push(await page.evaluate(() => getComputedStyle(document.documentElement).getPropertyValue('--cds-background').trim()));
    }
    expect(new Set(backgrounds).size).toBe(2); expect(backgrounds.every(Boolean)).toBe(true);
    expect(operations.length).toBe(operationsBeforeTheme);
    expect(await page.evaluate(() => localStorage.getItem('dduroc.viewer.theme'))).toContain('g100');
    await page.emulateMedia({ colorScheme: 'light' });
    await page.reload({ waitUntil: 'networkidle' }); await page.getByRole('table').waitFor();
    expect(await page.getByRole('button', { name: 'Dark theme', exact: true }).getAttribute('aria-pressed')).toBe('true');
    async function checkResponsiveWidths(modal = false) {
      for (const width of [320, 477, 698, 1280]) {
        await page.setViewportSize({ width, height: 1000 });
        await page.evaluate(() => new Promise<void>(resolve => requestAnimationFrame(() => requestAnimationFrame(() => resolve()))));
        const geometry = await page.evaluate(() => {
          const content = document.querySelector<HTMLElement>('.bx--modal.is-visible .bx--modal-content');
          return { viewport: document.documentElement.clientWidth, document: document.documentElement.scrollWidth, body: document.body.scrollWidth, modal: content ? { client: content.clientWidth, scroll: content.scrollWidth } : null };
        });
        expect(geometry.document, `document overflow at ${width}px (modal=${modal})`).toBeLessThanOrEqual(geometry.viewport);
        expect(geometry.body, `body overflow at ${width}px (modal=${modal})`).toBeLessThanOrEqual(geometry.viewport);
        if (modal) {
          expect(geometry.modal, 'visible export content must be measured').not.toBeNull();
          expect(geometry.modal!.scroll, `modal content overflow at ${width}px`).toBeLessThanOrEqual(geometry.modal!.client);
        }
      }
      await page.setViewportSize({ width: 1440, height: 1000 });
    }
    await checkResponsiveWidths();
    await page.getByRole('button', { name: 'Export history', exact: true }).click();
    const dialog = page.getByRole('dialog', { name: 'Export history', exact: true }); await dialog.waitFor();
    await dialog.getByLabel('Namespace match', { exact: true }).selectOption('exact');
    await dialog.getByLabel('Namespace', { exact: true }).fill('radio-1');
    await dialog.getByLabel('Record kind', { exact: true }).selectOption('messages');
    await dialog.getByLabel('Event names', { exact: true }).fill('Fault');
    await dialog.getByLabel('Time range', { exact: true }).selectOption('utc');
    await dialog.getByLabel('From · inclusive', { exact: true }).fill(fault!.utc!);
    await dialog.getByLabel('To · inclusive', { exact: true }).fill(fault!.utc!);
    await checkResponsiveWidths(true);
    const files: { format: string; bytes: number; filename: string }[] = [];
    for (const format of ['jsonl', 'csv']) {
      await dialog.getByLabel('File format', { exact: true }).selectOption(format);
      const beforePrepare = downloads; const openBefore = operations.filter(operation => operation.query.includes('openReadSession')).length;
      await dialog.getByRole('button', { name: 'Prepare export', exact: true }).click();
      const save = dialog.getByRole('button', { name: 'Download file', exact: true }); await save.waitFor();
      expect(downloads).toBe(beforePrepare);
      expect(operations.filter(operation => operation.query.includes('openReadSession')).length).toBe(openBefore + 1);
      const pending = page.waitForEvent('download'); await save.click(); const download = await pending;
      expect(download.suggestedFilename()).toBe(`dduroc-export.${format}`);
      const stream = await download.createReadStream(); expect(stream).not.toBeNull();
      const chunks: Buffer[] = []; for await (const chunk of stream!) chunks.push(Buffer.from(chunk));
      const bytes = Buffer.concat(chunks); const text = bytes.toString('utf8');
      const lines = format === 'jsonl' ? text.trimEnd().split('\n').map(line => JSON.parse(line)) : csvRows(text).slice(1).map(row => ({ type: row[0], data: JSON.parse(row[7]) }));
      const exported = lines.filter(line => line.type === 'entry'); expect(exported).toHaveLength(1);
      expect(exported[0].data).toEqual(fault);
      expect(lines[0].data.query).toMatchObject({ namespaces: ['radio-1'], from: { utc: fault!.utc }, to: { utc: fault!.utc }, filter: { eventNames: ['Fault'] } });
      expect(lines.at(-1).data).toMatchObject({ records: 1, complete: true });
      files.push({ format, bytes: bytes.byteLength, filename: download.suggestedFilename() });
      await dialog.getByRole('button', { name: 'Edit scope and prepare again', exact: true }).click();
    }
    await dialog.getByLabel('Namespace match', { exact: true }).selectOption('all');
    await dialog.getByLabel('Record kind', { exact: true }).selectOption('all');
    await dialog.getByLabel('Event names', { exact: true }).fill('');
    await dialog.getByLabel('Time range', { exact: true }).selectOption('all');
    await dialog.getByLabel('Record limit', { exact: true }).fill('1');
    const beforePartial = downloads;
    await dialog.getByRole('button', { name: 'Prepare export', exact: true }).click();
    const partial = dialog.getByRole('button', { name: 'Download partial file', exact: true }); await partial.waitFor();
    expect(downloads).toBe(beforePartial);
    const pendingPartial = page.waitForEvent('download'); await partial.click(); const partialDownload = await pendingPartial;
    expect(partialDownload.suggestedFilename()).toBe('dduroc-export.partial.csv');
    const partialStream = await partialDownload.createReadStream(); const partialChunks: Buffer[] = [];
    for await (const chunk of partialStream!) partialChunks.push(Buffer.from(chunk));
    const partialRows = csvRows(Buffer.concat(partialChunks).toString('utf8'));
    expect(JSON.parse(partialRows.at(-1)![7])).toMatchObject({ complete: false, records: 1 });
    await Promise.all(responses);
    expect(failures).toEqual([]);
    const unexpected = requests.filter(request => !request.startsWith('blob:') && new URL(request).origin !== new URL(url).origin);
    expect(unexpected).toEqual([]);
    console.info(JSON.stringify({ files, partialFilename: partialDownload.suggestedFilename(), themeBackgrounds: backgrounds, unexpectedRequests: unexpected }));
    await context.close();
    const blockedContext = await browser.newContext({ serviceWorkers: 'block' });
    await blockedContext.addInitScript(() => Object.defineProperty(window, 'localStorage', { get() { throw new DOMException('Storage disabled for acceptance', 'SecurityError'); } }));
    const blockedPage = await blockedContext.newPage(); const blockedErrors: string[] = [];
    blockedPage.on('pageerror', error => blockedErrors.push(error.message));
    await blockedPage.goto(url, { waitUntil: 'networkidle' }); await blockedPage.getByRole('table').waitFor();
    const blockedTheme = blockedPage.getByRole('button', { name: 'Dark theme', exact: true });
    expect(await blockedTheme.getAttribute('aria-pressed')).toBe('false');
    await blockedTheme.click();
    await vi.waitFor(async () => expect(await blockedPage.evaluate(() => document.documentElement.getAttribute('theme'))).toBe('g100'));
    expect(await blockedTheme.getAttribute('aria-pressed')).toBe('true');
    expect(blockedErrors).toEqual([]); await blockedContext.close();
  } finally { await browser.close(); }
}, 30_000);

it('keeps data completeness separate from unconfirmed resource cleanup', async () => {
  const api = transport(); api.page.mockResolvedValue({ revision: '1', page: page([record()], { exhausted: true }) });
  api.close.mockRejectedValue(new Error('offline')); api.cancel.mockRejectedValue(new Error('offline'));
  const result = await exportHistory(api, options(), new AbortController().signal);
  expect(result.complete).toBe(true); expect(result.cleanup).toBe('unconfirmed');
  expect(result.warnings.join(' ')).toContain('confirmed closed');
  expect((await jsonLines(result.blob)).at(-1).data).toMatchObject({ complete: true, cleanup: 'unconfirmed' });
});


it('distinguishes uncertain open acknowledgements from confirmed server admission rejection', async () => {
  for (const failure of [new ApiError('NETWORK', 'response lost'), new ApiError('PROTOCOL', 'invalid response'), new ApiError('VIEWER_LIMIT', 'response too large'), new DOMException('timeout', 'TimeoutError')]) {
    const api = transport(); api.open.mockRejectedValue(failure);
    await expect(exportHistory(api, options(), new AbortController().signal)).rejects.toMatchObject({ name: 'ExportCleanupError' });
    expect(api.cancel).not.toHaveBeenCalled(); expect(api.page).not.toHaveBeenCalled();
  }
  const api = transport(); const refusal = new ApiError('CAPACITY_EXCEEDED', 'no session admitted'); api.open.mockRejectedValue(refusal);
  await expect(exportHistory(api, options(), new AbortController().signal)).rejects.toBe(refusal);
  expect(api.cancel).not.toHaveBeenCalled();
});
