import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/svelte';
import App from '../src/App.svelte';
import * as exports from '../src/lib/export';

beforeEach(() => {
  history.replaceState(null, '', '/?demo=1'); localStorage.clear();
  vi.stubGlobal('matchMedia', () => ({ matches: false, addEventListener() {}, removeEventListener() {}, addListener() {}, removeListener() {} }));
});
afterEach(() => { cleanup(); vi.unstubAllGlobals(); vi.restoreAllMocks(); });
async function dialog() { await fireEvent.click(screen.getByRole('button', { name: 'Export history' })); return within(await screen.findByRole('dialog', { name: 'Export history' })); }

it('copies an editable independent scope and requires an explicit partial download', async () => {
  const exportHistory = vi.spyOn(exports, 'exportHistory').mockResolvedValue({ blob: new Blob(['partial']), filename: 'journal-partial.csv', records: 12, bytes: 7, complete: false, cleanup: 'cancelled', warnings: ['The record limit was reached.'] });
  const create = vi.fn(() => 'blob:export-test'); const revoke = vi.fn(); const NativeURL = URL;
  vi.stubGlobal('URL', class extends NativeURL { static createObjectURL = create; static revokeObjectURL = revoke; });
  const download = vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(() => {});
  render(App); await screen.findByRole('heading', { name: 'Records' });
  await fireEvent.change(screen.getByLabelText('Namespace match'), { target: { value: 'exact' } });
  const mainNamespace = screen.getByLabelText('Namespace') as HTMLInputElement;
  await fireEvent.input(mainNamespace, { target: { value: 'control.main' } });
  const modal = await dialog();
  expect((modal.getByLabelText('Namespace') as HTMLInputElement).value).toBe('control.main');
  await fireEvent.input(modal.getByLabelText('Namespace'), { target: { value: 'external.module' } });
  await fireEvent.change(modal.getByLabelText('File format'), { target: { value: 'csv' } });
  await fireEvent.click(modal.getByLabelText('Metadata only · skip payloads and decoded values'));
  await fireEvent.click(modal.getByRole('button', { name: 'Prepare export' }));
  await modal.findByText('Partial export — review before saving');
  expect(exportHistory.mock.calls[0][1]).toMatchObject({ query: { namespaces: ['external.module'] }, metadataOnly: true, format: 'csv', maxRecords: 10000, maxBytes: 16 * 1024 * 1024 });
  expect(mainNamespace.value).toBe('control.main'); expect(create).not.toHaveBeenCalled();
  await fireEvent.click(modal.getByRole('button', { name: 'Download partial file' }));
  expect(create).toHaveBeenCalledTimes(1); expect(download).toHaveBeenCalledTimes(1);
  expect((download.mock.instances[0] as HTMLAnchorElement).download).toBe('journal-partial.csv');
});

it('waits for cancellation settlement even if the dialog is closed and reopened', async () => {
  let reject!: (cause: Error) => void;
  const pending = new Promise<exports.ExportResult>((_, no) => { reject = no; });
  const exportHistory = vi.spyOn(exports, 'exportHistory').mockReturnValue(pending);
  render(App); await screen.findByRole('heading', { name: 'Records' });
  let modal = await dialog(); await fireEvent.click(modal.getByRole('button', { name: 'Prepare export' }));
  await fireEvent.click(modal.getByRole('button', { name: 'Cancel export' }));
  expect(exportHistory.mock.calls[0][2].aborted).toBe(true);
  await fireEvent.click(modal.getByRole('button', { name: 'Close the modal' }));
  modal = await dialog();
  expect(modal.queryByRole('button', { name: 'Prepare export' })).toBeNull();
  expect(exportHistory).toHaveBeenCalledTimes(1);
  reject(new DOMException('Cancelled', 'AbortError'));
  await modal.findByText('Export cancelled');
  expect((modal.getByRole('button', { name: 'Prepare export' }) as HTMLButtonElement).disabled).toBe(false);
});

it('blocks new exports after unconfirmed cleanup, including closing and reopening', async () => {
  vi.spyOn(exports, 'exportHistory').mockRejectedValue(new exports.ExportCleanupError());
  render(App); await screen.findByRole('heading', { name: 'Records' });
  let modal = await dialog(); await fireEvent.click(modal.getByRole('button', { name: 'Prepare export' }));
  await modal.findByText('Export cursor cleanup is unconfirmed');
  expect((modal.getByRole('button', { name: 'Prepare export' }) as HTMLButtonElement).disabled).toBe(true);
  await fireEvent.click(modal.getByRole('button', { name: 'Close' }));
  modal = await dialog();
  expect((modal.getByRole('button', { name: 'Prepare export' }) as HTMLButtonElement).disabled).toBe(true);
});

it('uses real Carbon themes and remembers a manual selection across remount', async () => {
  const first = render(App); await screen.findByRole('heading', { name: 'Records' });
  expect(document.documentElement.getAttribute('theme')).toBe('white');
  await fireEvent.click(screen.getByRole('button', { name: 'Dark theme' }));
  await waitFor(() => expect(document.documentElement.getAttribute('theme')).toBe('g100'));
  expect(localStorage.getItem('dduroc.viewer.theme')).toBe('g100'); first.unmount();
  render(App); await screen.findByRole('heading', { name: 'Records' });
  expect(document.documentElement.getAttribute('theme')).toBe('g100');
  expect(screen.getByRole('button', { name: 'Dark theme' }).getAttribute('aria-pressed')).toBe('true');
});

it('remains usable when localStorage is denied', async () => {
  vi.spyOn(Storage.prototype, 'getItem').mockImplementation(() => { throw new Error('Storage denied'); });
  vi.spyOn(Storage.prototype, 'setItem').mockImplementation(() => { throw new Error('Storage denied'); });
  render(App); await screen.findByRole('heading', { name: 'Records' });
  await fireEvent.click(screen.getByRole('button', { name: 'Dark theme' }));
  expect(document.documentElement.getAttribute('theme')).toBe('g100');
});

it.each([['g10', 'white'], ['g90', 'g100'], ['g80', 'g100']])('maps the previous %s preference to %s without an OS override', async (stored, expected) => {
  localStorage.setItem('dduroc.viewer.theme', stored);
  render(App); await screen.findByRole('heading', { name: 'Records' });
  expect(document.documentElement.getAttribute('theme')).toBe(expected);
  expect(localStorage.getItem('dduroc.viewer.theme')).toBe(expected);
  expect(screen.getByRole('button', { name: 'Dark theme' }).closest('header')).toBeTruthy();
});
