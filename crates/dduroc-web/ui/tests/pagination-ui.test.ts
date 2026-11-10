import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/svelte';
import App from '../src/App.svelte';
import { DemoTransport } from '../src/lib/demo';

beforeEach(() => {
  history.replaceState(null, '', '/?demo=1'); localStorage.clear();
  vi.stubGlobal('matchMedia', () => ({ matches: false, addEventListener() {}, removeEventListener() {}, addListener() {}, removeListener() {} }));
});
afterEach(() => { cleanup(); localStorage.clear(); vi.unstubAllGlobals(); vi.restoreAllMocks(); });

it('uses native Carbon numeric pagination with exact snapshot totals from the first page without allocating options', async () => {
  const original = DemoTransport.prototype.seek;
  const lastPage = 1_000_000_000;
  const seek = vi.spyOn(DemoTransport.prototype, 'seek').mockImplementation(async function (this: DemoTransport, ...args) {
    const result = await original.apply(this, [args[0], 1, args[2], args[3], args[4], args[5]]);
    return { ...result, reachedPage: String(args[1]), targetReached: true, exhausted: args[1] === lastPage };
  });
  const originalOpen = DemoTransport.prototype.countedOpen;
  const open = vi.spyOn(DemoTransport.prototype, 'countedOpen').mockImplementation(async function (this: DemoTransport, ...args) {
    return { ...await originalOpen.apply(this, args), totalMatches: '9007199254740993', totalSeeds: '2', totalPages: String(lastPage) };
  });
  render(App); await screen.findByRole('heading', { name: 'Records' });
  expect(screen.queryByLabelText('Go to page')).toBeNull();
  expect(screen.queryByText('Total pages unknown')).toBeNull();
  const input = await screen.findByRole('spinbutton', { name: `Page number, of ${lastPage} pages` });
  expect(screen.queryByLabelText('Go to page')).toBeNull();
  expect(screen.queryByRole('button', { name: 'Go' })).toBeNull();
  expect(input.getAttribute('max')).toBe(String(lastPage));
  expect(document.querySelectorAll('.bx--pagination option')).toHaveLength(0);
  expect(screen.getByText('9007199254740993 matching records · 1000000000 pages in this snapshot · 2 state seeds separately')).toBeTruthy();
  await fireEvent.change(input, { target: { value: String(lastPage) } });
  await waitFor(() => expect(screen.getByText(/Page 1000000000 · 4 loaded/)).toBeTruthy());
  expect((screen.getByRole('button', { name: 'Next page' }) as HTMLButtonElement).disabled).toBe(true);

  const before = seek.mock.calls.length;
  await fireEvent.change(input, { target: { value: '12' } });
  await waitFor(() => expect(screen.getByText(/Page 12 · 4 loaded/)).toBeTruthy());
  expect(seek).toHaveBeenCalledTimes(before + 1);
  expect(seek.mock.calls.at(-1)?.[1]).toBe(12);
  await fireEvent.click(screen.getByRole('button', { name: 'Next page' }));
  await waitFor(() => expect(screen.getByText(/Page 13 · 4 loaded/)).toBeTruthy());
  expect(seek).toHaveBeenCalledTimes(before + 2);
  expect(seek.mock.calls.at(-1)?.[1]).toBe(13);
  await fireEvent.click(screen.getByRole('button', { name: 'Previous page' }));
  await waitFor(() => expect(screen.getByText(/Page 12 · 4 loaded/)).toBeTruthy());
  expect(seek).toHaveBeenCalledTimes(before + 2);
  expect(seek.mock.calls.every(call => call[2] === 64)).toBe(true);
  expect(open).toHaveBeenCalledTimes(1);
});


it('keeps native pagination and an exact zero count for an empty filtered snapshot', async () => {
  vi.spyOn(DemoTransport.prototype, 'countedOpen').mockImplementation(async function (this: DemoTransport, query, _records, metadataOnly, presentation) {
    const session = await this.open({ ...query, namespaces: ['no-match'] }, metadataOnly, presentation);
    return { ...session, totalMatches: '0', totalSeeds: '0', totalPages: '1' };
  });
  render(App);
  await screen.findByText('No records match these filters.');
  expect(screen.getByText('0 matching records · 1 page in this snapshot')).toBeTruthy();
  expect(screen.getByRole('spinbutton', { name: 'Page number, of 1 page' })).toBeTruthy();
  expect((screen.getByRole('button', { name: 'Next page' }) as HTMLButtonElement).disabled).toBe(true);
  expect(screen.queryByRole('button', { name: 'Go' })).toBeNull();
});
