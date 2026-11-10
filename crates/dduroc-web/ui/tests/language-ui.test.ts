import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/svelte';
import App from '../src/App.svelte';
import { DemoTransport } from '../src/lib/demo';
import { LANGUAGE_STORAGE_KEY } from '../src/lib/languages';
import * as exports from '../src/lib/export';

beforeEach(() => {
  history.replaceState(null, '', '/?demo=1'); localStorage.clear();
  vi.stubGlobal('matchMedia', () => ({ matches: false, addEventListener() {}, removeEventListener() {}, addListener() {}, removeListener() {} }));
});
afterEach(() => { cleanup(); localStorage.clear(); vi.unstubAllGlobals(); vi.restoreAllMocks(); });
const selectPrimary = (value: string) => fireEvent.change(screen.getByLabelText('Primary message language'), { target: { value } });
const selectFallback = (value: string) => fireEvent.change(screen.getByLabelText('Fallback language'), { target: { value } });

it('automatically rerenders the current snapshot page and preserves its query despite unsaved filters', async () => {
  const open = vi.spyOn(DemoTransport.prototype, 'open'); const seek = vi.spyOn(DemoTransport.prototype, 'seek');
  const cancel = vi.spyOn(DemoTransport.prototype, 'cancel');
  render(App); await screen.findByRole('heading', { name: 'Records' });
  expect(open.mock.calls[0][2]).toBeUndefined();
  expect((screen.getByLabelText('Fallback language') as HTMLSelectElement).disabled).toBe(true);
  await fireEvent.change(screen.getByRole('spinbutton', { name: 'Page number, of 5 pages' }), { target: { value: '3' } });
  await waitFor(() => expect(screen.getByText(/Page 3 · 4 loaded/)).toBeTruthy());
  const namespace = screen.getByRole('combobox', { name: 'Namespace' });
  await fireEvent.input(namespace, { target: { value: 'network.' } }); await fireEvent.blur(namespace);
  await selectPrimary('ru'); await selectFallback('en');
  expect(screen.queryByText('Configuration applied successfully')).toBeNull();
  await screen.findByText('Конфигурация успешно применена');
  expect(screen.getByText(/Page 3 · 4 loaded/)).toBeTruthy();
  expect(open).toHaveBeenCalledTimes(1); expect(cancel).not.toHaveBeenCalled();
  expect(seek.mock.calls.at(-1)?.[1]).toBe(3); expect(seek.mock.calls.at(-1)?.[5]).toEqual({ languages: ['ru', 'en'] });
  expect(localStorage.getItem(LANGUAGE_STORAGE_KEY)).toBe(JSON.stringify({ primary: 'ru', fallback: 'en' }));
  await fireEvent.click(screen.getAllByRole('button', { name: /expand current row/i })[2]);
  expect(screen.getByText('ru · primary preference')).toBeTruthy();
  await selectPrimary(''); await screen.findByText('Configuration applied successfully');
  expect(seek.mock.calls.at(-1)?.[5]).toBeUndefined(); expect(open).toHaveBeenCalledTimes(1);
});

it('restores explicit preferences before the first read and preserves unavailable stored choices', async () => {
  localStorage.setItem(LANGUAGE_STORAGE_KEY, JSON.stringify({ primary: 'zz', fallback: 'ru' }));
  const open = vi.spyOn(DemoTransport.prototype, 'open');
  const first = render(App); await screen.findByRole('heading', { name: 'Records' });
  expect((screen.getByLabelText('Primary message language') as HTMLSelectElement).value).toBe('zz');
  expect(open.mock.calls[0][2]).toEqual({ languages: ['zz', 'ru'] });
  expect(screen.getAllByText('ru · fallback').length).toBeGreaterThan(0);
  await fireEvent.click(screen.getAllByRole('button', { name: /expand current row/i })[0]);
  expect(screen.getByText('ru · fallback preference 2')).toBeTruthy();
  first.unmount(); render(App); await screen.findByRole('heading', { name: 'Records' });
  expect((screen.getByLabelText('Fallback language') as HTMLSelectElement).value).toBe('ru');
  expect(open.mock.calls[1][2]).toEqual({ languages: ['zz', 'ru'] });
});

it('automatically restarts live with the new language and explicitly announces history resynchronization', async () => {
  const stop = vi.fn(); const follow = vi.spyOn(DemoTransport.prototype, 'follow').mockReturnValue(stop);
  render(App); await screen.findByRole('heading', { name: 'Records' });
  await selectPrimary('ru'); await selectFallback('en');
  await fireEvent.click(await screen.findByRole('button', { name: 'Start live' }));
  expect(follow.mock.calls[0][3]).toEqual({ languages: ['ru', 'en'] });
  await selectPrimary('');
  expect(stop).toHaveBeenCalledTimes(1); expect(follow).toHaveBeenCalledTimes(2);
  expect(follow.mock.calls[1][3]).toBeUndefined(); expect(screen.queryByRole('table')).toBeNull();
  expect((screen.getByLabelText('Fallback language') as HTMLSelectElement).value).toBe('');
  expect(screen.getByText(/Service order: en; then the first language/)).toBeTruthy();
  expect(screen.getByText(/Live history was resynchronized automatically/)).toBeTruthy();
});

it('exports the shared preference and prevents changes while export preparation is in flight', async () => {
  let resolve!: (result: exports.ExportResult) => void;
  const pending = new Promise<exports.ExportResult>(yes => { resolve = yes; });
  const exportHistory = vi.spyOn(exports, 'exportHistory').mockReturnValue(pending);
  render(App); await screen.findByRole('heading', { name: 'Records' });
  await selectPrimary('ru'); await selectFallback('en');
  await fireEvent.click(screen.getByRole('button', { name: 'Export history' }));
  const modal = within(await screen.findByRole('dialog', { name: 'Export history' }));
  expect(modal.getByText(/Message language: ru → en/)).toBeTruthy();
  await fireEvent.click(modal.getByRole('button', { name: 'Prepare export' }));
  expect(exportHistory.mock.calls[0][1].presentation).toEqual({ languages: ['ru', 'en'] });
  expect((screen.getByLabelText('Primary message language') as HTMLSelectElement).disabled).toBe(true);
  expect((screen.getByLabelText('Fallback language') as HTMLSelectElement).disabled).toBe(true);
  resolve({ blob: new Blob(['ready']), filename: 'dduroc-export.jsonl', records: 1, bytes: 5, complete: true, cleanup: 'closed', warnings: [] });
  await modal.findByText('File ready');
  expect((screen.getByLabelText('Primary message language') as HTMLSelectElement).disabled).toBe(false);
});

it('remains usable when language preference persistence is denied', async () => {
  vi.spyOn(Storage.prototype, 'getItem').mockImplementation(() => { throw new Error('denied'); });
  vi.spyOn(Storage.prototype, 'setItem').mockImplementation(() => { throw new Error('denied'); });
  render(App); await screen.findByRole('heading', { name: 'Records' });
  await selectPrimary('ru'); await fireEvent.click(screen.getByRole('button', { name: 'Run query' }));
  await screen.findByText('Конфигурация успешно применена');
});
