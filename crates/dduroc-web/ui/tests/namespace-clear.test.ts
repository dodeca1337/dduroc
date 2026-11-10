import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/svelte';
import App from '../src/App.svelte';
import { DemoTransport } from '../src/lib/demo';
import * as exports from '../src/lib/export';

beforeEach(() => {
  history.replaceState(null, '', '/?demo=1'); localStorage.clear();
  vi.stubGlobal('matchMedia', () => ({ matches: false, addEventListener() {}, removeEventListener() {}, addListener() {}, removeListener() {} }));
});
afterEach(() => { cleanup(); localStorage.clear(); vi.unstubAllGlobals(); vi.restoreAllMocks(); });

it('clears an existing combobox selection when All namespaces is chosen for both viewer and export', async () => {
  const open = vi.spyOn(DemoTransport.prototype, 'open');
  const exportHistory = vi.spyOn(exports, 'exportHistory').mockResolvedValue({ blob: new Blob(['ready']), filename: 'all.jsonl', records: 1, bytes: 5, complete: true, cleanup: 'closed', warnings: [] });
  render(App); await screen.findByRole('heading', { name: 'Records' });
  const namespace = screen.getByRole('combobox', { name: 'Namespace' }) as HTMLInputElement;
  const mode = screen.getByLabelText('Namespace match') as HTMLSelectElement;
  async function choose() {
    namespace.focus();
    await fireEvent.input(namespace, { target: { value: 'network.' } });
    await fireEvent.keyDown(namespace, { key: 'ArrowDown' }); await fireEvent.keyDown(namespace, { key: 'Enter' });
    expect(namespace.value).toBe('network.link'); expect(mode.value).toBe('exact');
    mode.focus(); await fireEvent.blur(namespace, { relatedTarget: mode });
    await fireEvent.change(mode, { target: { value: 'all' } });
    await waitFor(() => { expect(namespace.value).toBe(''); expect(mode.value).toBe('all'); });
  }
  await choose();
  await fireEvent.click(screen.getByRole('button', { name: 'Run query' }));
  await waitFor(() => expect(open).toHaveBeenCalledTimes(2));
  expect(open.mock.calls[1][0].namespaces).toBeUndefined(); expect(open.mock.calls[1][0].namespacePrefix).toBeUndefined();
  await screen.findByRole('heading', { name: 'Records' });
  await choose();
  await fireEvent.click(screen.getByRole('button', { name: 'Export history' }));
  const modal = within(await screen.findByRole('dialog', { name: 'Export history' }));
  expect((modal.getByLabelText('Namespace match') as HTMLSelectElement).value).toBe('all');
  expect((modal.getByRole('combobox', { name: 'Namespace' }) as HTMLInputElement).value).toBe('');
  const exportNamespace = modal.getByRole('combobox', { name: 'Namespace' }) as HTMLInputElement;
  const exportMode = modal.getByLabelText('Namespace match') as HTMLSelectElement;
  exportNamespace.focus(); await fireEvent.input(exportNamespace, { target: { value: 'network.' } });
  await fireEvent.keyDown(exportNamespace, { key: 'ArrowDown' }); await fireEvent.keyDown(exportNamespace, { key: 'Enter' });
  expect(exportNamespace.value).toBe('network.link'); expect(exportMode.value).toBe('exact');
  exportMode.focus(); await fireEvent.blur(exportNamespace, { relatedTarget: exportMode });
  await fireEvent.change(exportMode, { target: { value: 'all' } });
  await waitFor(() => { expect(exportNamespace.value).toBe(''); expect(exportMode.value).toBe('all'); });
  await fireEvent.click(modal.getByRole('button', { name: 'Prepare export' }));
  await modal.findByText('File ready');
  expect(exportHistory.mock.calls[0][1].query.namespaces).toBeUndefined();
  expect(exportHistory.mock.calls[0][1].query.namespacePrefix).toBeUndefined();
});
