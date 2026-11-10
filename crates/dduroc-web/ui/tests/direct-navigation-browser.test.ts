// Independent real packaged UI acceptance; the server must contain the 5030-record fixture.
import { expect, it, vi } from 'vitest';

it.runIf(!!process.env.DDUROC_WEB_TEST_URL)('jumps to evicted pages and automatically re-renders languages without opening another snapshot', async () => {
  const { chromium } = await import('playwright-core');
  const browser = await chromium.launch({ headless: true, executablePath: process.env.DDUROC_WEB_TEST_BROWSER });
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 1000 }, serviceWorkers: 'block' });
    const tab = await context.newPage();
    const operations: { query: string; variables: Record<string, any> }[] = [];
    const replies: any[] = []; const errors: unknown[] = []; const responses: Promise<void>[] = [];
    let documentNavigations = 0;
    tab.on('framenavigated', frame => { if (frame === tab.mainFrame()) documentNavigations++; });
    tab.on('pageerror', error => errors.push(error.message));
    tab.on('request', request => { if (request.method() === 'POST') operations.push(request.postDataJSON()); });
    tab.on('response', response => {
      if (response.request().method() === 'POST') responses.push((async () => {
        const payload = await response.json(); if (payload.errors) errors.push(payload.errors);
        if (payload.data?.seekReadSession) replies.push(payload.data.seekReadSession);
      })());
    });
    await tab.goto(process.env.DDUROC_WEB_TEST_URL!, { waitUntil: 'networkidle' });
    await tab.getByRole('table').waitFor().catch(async error => { await Promise.all(responses); console.error(JSON.stringify({ errors, body: await tab.locator('main').innerText() })); throw error; }); await Promise.all(responses);
    const heading = tab.locator('.bx--data-table-header__description');
    const settled = async (number: number) => {
      await vi.waitFor(async () => {
        expect(await tab.getByRole('region', { name: 'Journal records', exact: true }).getAttribute('aria-busy')).toBe('false');
        expect(await heading.innerText()).toContain(`Page ${number} ·`);
      }, { timeout: 20_000 });
    };
    const jump = async (number: number) => {
      const native = tab.getByRole('spinbutton', { name: /^Page number, of / });
      expect(await native.getAttribute('max')).toBe('79');
      await native.fill(String(number)); await native.press('Tab');
      await settled(number);
    };
    expect(await tab.getByRole('spinbutton', { name: 'Page number, of 79 pages', exact: true }).inputValue()).toBe('1');
    expect(await tab.getByRole('button', { name: 'Go', exact: true }).count()).toBe(0);
    const firstRows = await tab.locator('tbody tr').allTextContents();
    expect(firstRows.join(' ')).toContain('Example diagnostic');
    await jump(12);
    const original12 = await tab.locator('tbody tr').allTextContents();
    expect(replies.some(reply => !reply.targetReached && !reply.exhausted && reply.page === null)).toBe(true);
    for (const number of [3, 4, 5, 6, 7]) await jump(number);
    expect(await tab.locator('.cache-note').first().innerText()).not.toMatch(/Cached pages (?:1,|1 ·)/);
    const beforeReplay = replies.length; await jump(1);
    expect(replies.length).toBeGreaterThan(beforeReplay);
    expect(await tab.locator('tbody tr').allTextContents()).toEqual(firstRows);
    await jump(12);
    expect(await tab.locator('tbody tr').allTextContents()).toEqual(original12);

    // Draft filters must not replace the query captured by the existing session.
    await tab.getByRole('button', { name: /^Filters ·/ }).click();
    await tab.getByRole('combobox', { name: 'Namespace match', exact: true }).selectOption('exact');
    const namespace = tab.getByRole('combobox', { name: 'Namespace', exact: true });
    await namespace.fill('radio-0'); await namespace.press('ArrowDown'); await namespace.press('Enter');
    expect(await namespace.inputValue()).toBe('radio-0');
    const primary = tab.getByRole('combobox', { name: 'Primary message language', exact: true });
    let release!: () => void; let markHeld!: () => void;
    const barrier = new Promise<void>(resolve => { release = resolve; });
    const held = new Promise<void>(resolve => { markHeld = resolve; });
    let delayed = false;
    await tab.route('**/graphql', async route => {
      const request = route.request(); const body = request.postDataJSON();
      if (!delayed && body.query.includes('seekReadSession') && body.variables.presentation?.languages?.[0] === 'ru') {
        delayed = true; const response = await route.fetch(); markHeld(); await barrier; await route.fulfill({ response });
      } else await route.continue();
    });
    await primary.selectOption('ru'); await held;
    await primary.selectOption('en'); await primary.selectOption('ru'); release();
    await settled(12);
    const translated = await tab.locator('.message-cell').allTextContents();
    expect(translated.length).toBe(64); expect(translated.every(text => /такт/.test(text))).toBe(true);
    expect(await namespace.inputValue()).toBe('radio-0');
    expect(operations.filter(operation => operation.query.includes('openCountedReadSession'))).toHaveLength(1);
    await jump(1);
    expect((await tab.locator('tbody tr').allTextContents()).join(' ')).toContain('Example diagnostic');
    expect(await tab.locator('.message-cell').allTextContents()).toEqual(expect.arrayContaining([expect.stringMatching(/такт|ошибка/)]));
    await primary.selectOption('en'); await settled(1);
    expect(await tab.locator('tbody tr').allTextContents()).toEqual(firstRows);
    await jump(79); expect(await tab.getByRole('button', { name: 'Next page', exact: true }).isDisabled()).toBe(true);
    await jump(1); expect(await tab.locator('tbody tr').allTextContents()).toEqual(firstRows);
    expect(operations.filter(operation => operation.query.includes('openCountedReadSession'))).toHaveLength(1);
    expect(documentNavigations).toBe(1);
    expect(await tab.locator('tbody tr:visible').count()).toBeLessThanOrEqual(64);
    await Promise.all(responses); expect(errors).toEqual([]);
    console.info(JSON.stringify({ directJump: 12, evictedBackwardReplay: true, eofBackwardReplay: true, rapidLanguageChanges: true, preservedDraftFilters: true, snapshotsOpened: 1, documentNavigations }));
    await context.close();
  } finally { await browser.close(); }
}, 120_000);
