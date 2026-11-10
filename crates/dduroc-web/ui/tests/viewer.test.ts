import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/svelte';
import App from '../src/App.svelte';

beforeEach(() => {
  history.replaceState(null, '', '/?demo=1');
  vi.stubGlobal('matchMedia', () => ({ matches: false, addEventListener() {}, removeEventListener() {}, addListener() {}, removeListener() {} }));
});
afterEach(() => { cleanup(); vi.unstubAllGlobals(); });

it('loads an explicit preview, expands exact values and advances its owned cursor', async () => {
  const { DemoTransport } = await import('../src/lib/demo');
  const page = vi.spyOn(DemoTransport.prototype, 'page');
  render(App);
  expect(await screen.findByText('Demo preview')).toBeTruthy();
  await screen.findByRole('heading', { name: 'Records' });
  expect(screen.getByText('Configuration applied successfully')).toBeTruthy();
  const expand = screen.getAllByRole('button', { name: /expand current row/i });
  await fireEvent.click(expand[0]);
  expect(await screen.findByText('18446744073709551615')).toBeTruthy();
  await fireEvent.click(screen.getByRole('button', { name: 'Next page' }));
  await waitFor(() => expect(screen.getByText(/Page 2 · 4 loaded/)).toBeTruthy());
  expect(page).toHaveBeenCalledTimes(2);
  await fireEvent.click(screen.getByRole('button', { name: 'Previous page' }));
  expect(screen.getByText('Configuration applied successfully')).toBeTruthy();
  await fireEvent.click(screen.getByRole('button', { name: 'Next page' }));
  expect(screen.queryByText('Configuration applied successfully')).toBeNull();
  expect(page).toHaveBeenCalledTimes(2);
});

it('replaces the entire table with a meaningful no-results view', async () => {
  render(App); await screen.findByRole('heading', { name: 'Records' });
  await fireEvent.change(screen.getByLabelText('Preview scenario'), { target: { value: 'empty' } });
  await screen.findByText('No records match these filters.');
  expect(screen.queryByRole('table')).toBeNull();
  expect(screen.getByRole('button', { name: 'Clear filters and query' })).toBeTruthy();
});

it('shows a retention gap as a persistent resync action without discarding the first page', async () => {
  render(App); await screen.findByRole('heading', { name: 'Records' });
  await fireEvent.change(screen.getByLabelText('Preview scenario'), { target: { value: 'gap' } });
  await waitFor(() => expect(screen.getByRole('heading', { name: 'Records' })).toBeTruthy());
  await fireEvent.click(screen.getByRole('button', { name: 'Next page' }));
  await screen.findByText('History changed');
  expect(screen.getByRole('table')).toBeTruthy();
  expect(screen.queryByRole('button', { name: 'Next page' })).toBeNull();
  expect(screen.queryByRole('spinbutton', { name: /^Page number/ })).toBeNull();
  expect(screen.queryByText('20 matching records · 5 pages in this snapshot')).toBeNull();
  expect(screen.getByText(/Run a new query to resync/)).toBeTruthy();
});

it('best-effort cancels its known capability when the browser leaves the page', async () => {
  const { DemoTransport } = await import('../src/lib/demo');
  const cancel = vi.spyOn(DemoTransport.prototype, 'cancel');
  render(App); await screen.findByRole('heading', { name: 'Records' });
  window.dispatchEvent(new Event('pagehide'));
  await waitFor(() => expect(cancel).toHaveBeenCalledTimes(1));
});

it('does not open a cursor if bootstrap finishes after unmount', async () => {
  const api = await import('../src/lib/api');
  let resolve!: (value: import('../src/lib/types').Bootstrap) => void;
  const config = new Promise<import('../src/lib/types').Bootstrap>(yes => { resolve = yes; });
  vi.spyOn(api, 'loadBootstrap').mockReturnValue(config);
  const open = vi.spyOn(api.HttpTransport.prototype, 'open');
  history.replaceState(null, '', '/');
  const app = render(App); app.unmount();
  resolve({ graphqlEndpoint: 'http://localhost/graphql', title: 'Test', maxRows: 100 });
  await config; await Promise.resolve();
  expect(open).not.toHaveBeenCalled();
});

it('switches mixed anchored and unanchored rows without changing order or making cursor RPCs', async () => {
  const { DemoTransport, demoEntries } = await import('../src/lib/demo');
  const anchored = structuredClone(demoEntries[0]);
  anchored.utc = '2026-11-10T09:41:20.123456789+00:00';
  const unanchored = structuredClone(demoEntries[1]); unanchored.utc = null;
  const originalPage = DemoTransport.prototype.page;
  const open = vi.spyOn(DemoTransport.prototype, 'open');
  const page = vi.spyOn(DemoTransport.prototype, 'page').mockImplementation(async function (this: InstanceType<typeof DemoTransport>, ...args) {
    const result = await originalPage.apply(this, args);
    result.page.entries = [anchored, unanchored]; result.page.exhausted = false; return result;
  });
  const cancel = vi.spyOn(DemoTransport.prototype, 'cancel');
  const close = vi.spyOn(DemoTransport.prototype, 'close');
  const follow = vi.spyOn(DemoTransport.prototype, 'follow');
  render(App); await screen.findByRole('heading', { name: 'Records' });
  expect((screen.getByLabelText('Time display') as HTMLSelectElement).value).toBe('calendar');
  expect(screen.getByText('2026-11-10 09:41:20.123456789 UTC')).toBeTruthy();
  expect(screen.getByText('Boot 7 · +430.982460s · UTC unavailable')).toBeTruthy();
  const records = () => screen.getAllByRole('row').slice(1).map(row => row.textContent).filter(text => !!text?.trim());
  expect(records()[0]).toContain('Configuration applied successfully');
  expect(records()[1]).toContain('Link state');
  await fireEvent.change(screen.getByLabelText('Time display'), { target: { value: 'boot' } });
  expect(screen.getByText('Boot 7 · +431.102460s')).toBeTruthy();
  expect(screen.getByText('Boot 7 · +430.982460s')).toBeTruthy();
  expect(screen.queryByText(/UTC unavailable/)).toBeNull();
  await fireEvent.change(screen.getByLabelText('Time display'), { target: { value: 'calendar' } });
  expect(screen.getByText('2026-11-10 09:41:20.123456789 UTC')).toBeTruthy();
  expect(records()[0]).toContain('Configuration applied successfully');
  expect(records()[1]).toContain('Link state');
  expect(open).toHaveBeenCalledTimes(1); expect(page).toHaveBeenCalledTimes(1);
  expect(close).not.toHaveBeenCalled(); expect(cancel).not.toHaveBeenCalled(); expect(follow).not.toHaveBeenCalled();
  await fireEvent.click(screen.getAllByRole('button', { name: /expand current row/i })[0]);
  expect(screen.getByText('Boot 7 · +431.102460s')).toBeTruthy();
  expect(screen.getAllByText('2026-11-10 09:41:20.123456789 UTC')).toHaveLength(2);
});

it('allows namespace autocomplete immediately and submits only after Run query', async () => {
  const { DemoTransport } = await import('../src/lib/demo');
  const open = vi.spyOn(DemoTransport.prototype, 'open');
  render(App); await screen.findByRole('heading', { name: 'Records' });
  expect(screen.getByRole('button', { name: /Filters · All history/ }).getAttribute('aria-expanded')).toBe('false');
  const namespace = screen.getByRole('combobox', { name: 'Namespace' }) as HTMLInputElement;
  expect(namespace.closest('.bx--accordion__content')).toBeNull();
  expect(namespace.disabled).toBe(false);
  await fireEvent.input(namespace, { target: { value: 'network.' } });
  await fireEvent.keyDown(namespace, { key: 'ArrowDown' });
  await fireEvent.keyDown(namespace, { key: 'Enter' });
  expect(namespace.value).toBe('network.link'); expect(open).toHaveBeenCalledTimes(1);
  await fireEvent.click(screen.getByRole('button', { name: 'Run query' }));
  await waitFor(() => expect(open).toHaveBeenCalledTimes(2));
  expect(open.mock.calls[1][0].namespaces).toEqual(['network.link']);
});

it('keeps a custom namespace prefix when the combobox loses focus', async () => {
  render(App); await screen.findByRole('heading', { name: 'Records' });
  await fireEvent.change(screen.getByLabelText('Namespace match'), { target: { value: 'prefix' } });
  const namespace = screen.getByRole('combobox', { name: 'Namespace' }) as HTMLInputElement;
  await fireEvent.input(namespace, { target: { value: 'custom.namespace.' } }); await fireEvent.blur(namespace);
  expect(namespace.value).toBe('custom.namespace.');
});
