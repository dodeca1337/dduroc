// Independent packaged-browser regression for Carbon row-collapse scrollbar flicker.
import { expect, it, vi } from 'vitest';
import type { Locator } from 'playwright-core';

async function sampleToggle(button: Locator) {
  return button.evaluate(button => new Promise<{ frames: number; verticalScrollbars: string[]; horizontalPageOverflow: number; tableWidths: number[] }>(resolve => {
    const started = performance.now();
    let frames = 0; let horizontalPageOverflow = 0;
    const verticalScrollbars = new Set<string>(); const tableWidths = new Set<number>();
    function sample() {
      frames++;
      horizontalPageOverflow = Math.max(horizontalPageOverflow, document.documentElement.scrollWidth - document.documentElement.clientWidth);
      for (const element of document.querySelectorAll<HTMLElement>('.results, .bx--data-table-container, .bx--data-table-content, .bx--pagination, .results-footer')) {
        const style = getComputedStyle(element);
        // A single fractional-pixel rounding difference reproduced the original flicker.
        if (/^(auto|scroll)$/.test(style.overflowY) && element.scrollHeight > element.clientHeight) verticalScrollbars.add(element.className);
        if (element.classList.contains('bx--data-table-content')) tableWidths.add(element.clientWidth);
      }
      if (performance.now() - started < 650) requestAnimationFrame(sample);
      else resolve({ frames, verticalScrollbars: [...verticalScrollbars], horizontalPageOverflow, tableWidths: [...tableWidths] });
    }
    sample(); (button as HTMLButtonElement).click();
  }));
}

it.runIf(!!process.env.DDUROC_WEB_TEST_URL)('keeps nested table and pagination scrollbars stable throughout row collapse while preserving scroll access', async () => {
  const { chromium } = await import('playwright-core');
  const browser = await chromium.launch({ headless: true, executablePath: process.env.DDUROC_WEB_TEST_BROWSER });
  try {
    for (const width of [713, 1440]) {
      const context = await browser.newContext({ viewport: { width, height: 932 }, serviceWorkers: 'block' });
      const tab = await context.newPage(); const errors: string[] = [];
      tab.on('pageerror', error => errors.push(error.message));
      await tab.goto(process.env.DDUROC_WEB_TEST_URL!, { waitUntil: 'networkidle' });
      const button = tab.locator('.bx--table-expand__button').first(); await button.waitFor();
      const firstMessages = await tab.locator('.message-cell').allTextContents();
      const samples = [];
      for (let cycle = 0; cycle < 2; cycle++) {
        samples.push(await sampleToggle(button));
        expect(await button.getAttribute('aria-expanded')).toBe('true');
        samples.push(await sampleToggle(button));
        expect(await button.getAttribute('aria-expanded')).toBe('false');
      }
      for (const sample of samples) {
        expect(sample.frames).toBeGreaterThan(10);
        expect(sample.verticalScrollbars).toEqual([]);
        expect(sample.horizontalPageOverflow).toBe(0);
        expect(sample.tableWidths).toHaveLength(1);
      }

      // Horizontal columns must remain reachable; clipping all overflow would hide the symptom.
      const table = tab.locator('.bx--data-table-content');
      const horizontal = await table.evaluate(element => {
        const style = getComputedStyle(element); element.scrollLeft = element.scrollWidth;
        return { overflowX: style.overflowX, scrollLeft: element.scrollLeft, range: element.scrollWidth - element.clientWidth };
      });
      expect(horizontal.overflowX).toMatch(/^(auto|scroll)$/);
      if (width === 713) expect(horizontal.range).toBeGreaterThan(0);
      expect(horizontal.scrollLeft).toBe(horizontal.range);
      await table.evaluate(element => { element.scrollLeft = 0; });

      const messageRow = tab.locator('tbody tr').filter({ has: tab.locator('td:nth-child(5)', { hasText: 'Message' }) }).first();
      const detailSample = await sampleToggle(messageRow.getByRole('button'));
      expect(detailSample.verticalScrollbars).toEqual([]);
      const clippedTableBottom = await table.evaluate(element => element.querySelector('table')!.getBoundingClientRect().bottom - element.getBoundingClientRect().bottom);
      expect(clippedTableBottom).toBeLessThanOrEqual(1);
      const payload = tab.getByRole('textbox', { name: 'Raw payload in base64', exact: true }); await payload.waitFor();
      // Stress only this browser's detail fixture, leaving the server and stored records untouched.
      const payloadScroll = await payload.evaluate(element => {
        const textarea = element as HTMLTextAreaElement;
        textarea.value = Array.from({ length: 200 }, () => 'QUJD'.repeat(80)).join('\n');
        textarea.scrollTop = textarea.scrollHeight;
        return { scrollTop: textarea.scrollTop, scrollHeight: textarea.scrollHeight, clientHeight: textarea.clientHeight, overflowY: getComputedStyle(textarea).overflowY };
      });
      expect(payloadScroll.scrollHeight).toBeGreaterThan(payloadScroll.clientHeight);
      expect(payloadScroll.scrollTop).toBeGreaterThan(0);
      expect(payloadScroll.overflowY).toMatch(/^(auto|scroll)$/);
      const pageScroll = await tab.evaluate(() => {
        window.scrollTo(0, document.documentElement.scrollHeight);
        return { y: window.scrollY, rootOverflow: getComputedStyle(document.documentElement).overflowY, bodyOverflow: getComputedStyle(document.body).overflowY };
      });
      expect(pageScroll.y).toBeGreaterThan(0);
      expect(pageScroll.rootOverflow).not.toMatch(/^(hidden|clip)$/);
      expect(pageScroll.bodyOverflow).not.toMatch(/^(hidden|clip)$/);
      // The full range is available before visiting later pages.
      const nativePage = tab.getByRole('spinbutton', { name: 'Page number, of 79 pages', exact: true });
      expect(await nativePage.inputValue()).toBe('1');
      expect(await nativePage.getAttribute('max')).toBe('79');
      await nativePage.fill('79'); await nativePage.press('Tab');
      await vi.waitFor(async () => {
        expect(await tab.getByRole('region', { name: 'Journal records', exact: true }).getAttribute('aria-busy')).toBe('false');
        expect(await tab.locator('.bx--data-table-header__description').innerText()).toContain('Page 79 ·');
      }, { timeout: 20_000 });
      expect(await nativePage.isVisible()).toBe(true);
      expect(await nativePage.inputValue()).toBe('79');
      expect(await tab.locator('.bx--pagination option').count()).toBe(0);
      expect(await tab.getByRole('button', { name: 'Go', exact: true }).count()).toBe(0);
      expect(await tab.getByRole('button', { name: 'Next page', exact: true }).isDisabled()).toBe(true);
      await nativePage.fill('1'); await nativePage.press('Tab');
      await vi.waitFor(async () => {
        expect(await tab.getByRole('region', { name: 'Journal records', exact: true }).getAttribute('aria-busy')).toBe('false');
        expect(await tab.locator('.bx--data-table-header__description').innerText()).toContain('Page 1 ·');
      }, { timeout: 10_000 });
      expect(await tab.locator('.message-cell').allTextContents()).toEqual(firstMessages);
      expect(errors).toEqual([]);
      console.info(JSON.stringify({ viewport: `${width}x932`, sampledFrames: samples.reduce((sum, sample) => sum + sample.frames, detailSample.frames), nestedVerticalScrollbarFrames: 0, horizontalTableRange: horizontal.range, payloadScrollTop: payloadScroll.scrollTop, documentScrollTop: pageScroll.y }));
      await context.close();
    }
  } finally { await browser.close(); }
}, 60_000);
