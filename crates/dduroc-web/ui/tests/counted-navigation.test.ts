import { afterEach, expect, it, vi } from 'vitest';
import { get } from 'svelte/store';
import { ViewerController } from '../src/lib/controller';
import type { CountedSession, Entry, Transport } from '../src/lib/types';

const entry: Entry = { namespace: 'selected', class: 'DEFAULT', at: { boot: '1', micros: '0' }, utc: null, span: null, kind: 'TEXT', text: { level: 'INFO', target: 'test', text: 'match' } };
const totals: CountedSession = { id: 'fixed', revision: '0', totalMatches: '9007199254740993', totalSeeds: '3', totalPages: '7' };
function deferred<T>() { let resolve!: (value: T) => void; const promise = new Promise<T>(yes => { resolve = yes; }); return { promise, resolve }; }
function transport() {
  return { catalog: vi.fn<Transport['catalog']>(), open: vi.fn<Transport['open']>(), countedOpen: vi.fn<NonNullable<Transport['countedOpen']>>().mockResolvedValue(totals), page: vi.fn<Transport['page']>(),
    seek: vi.fn<NonNullable<Transport['seek']>>().mockImplementation(async (_session, target) => ({ revision: String(target), reachedPage: String(target), targetReached: true, exhausted: target === 7, page: { entries: [entry], seeds: [entry], damaged: [], unanchored: [], exhausted: target === 7, truncated: false } })),
    cancel: vi.fn<Transport['cancel']>().mockResolvedValue(undefined), close: vi.fn<Transport['close']>().mockResolvedValue(undefined), follow: vi.fn<Transport['follow']>() };
}
const controllers: ViewerController[] = [];
function viewer(api: Transport) { const controller = new ViewerController(api); controllers.push(controller); return controller; }
afterEach(() => controllers.splice(0).forEach(controller => controller.dispose()));

it('waits for exact filtered counts before the first page without querying storage statistics or scanning in the browser', async () => {
  const api = transport(); const pending = deferred<CountedSession>(); api.countedOpen.mockReturnValueOnce(pending.promise);
  const controller = viewer(api); const query = { namespaces: ['selected'] }; const running = controller.run(query, true, { languages: ['en'] });
  await vi.waitFor(() => expect(api.countedOpen).toHaveBeenCalledTimes(1));
  expect(get(controller)).toMatchObject({ phase: 'counting', rows: [], totalMatches: null, knownLastPage: null });
  expect(api.seek).not.toHaveBeenCalled();
  pending.resolve(totals); await running;
  expect(api.countedOpen).toHaveBeenCalledWith(query, 64, true, { languages: ['en'] });
  expect(api.open).not.toHaveBeenCalled(); expect(api.page).not.toHaveBeenCalled();
  expect(get(controller)).toMatchObject({ page: 1, totalMatches: totals.totalMatches, totalSeeds: '3', knownLastPage: 7, canNext: true });
  expect(api.seek).toHaveBeenCalledTimes(1);
});

it('keeps exact totals and page boundaries across language rendering, direct seeks and cached navigation', async () => {
  const api = transport(); const controller = viewer(api); await controller.run({});
  await controller.goTo(7); expect(get(controller)).toMatchObject({ knownLastPage: 7, canNext: false, totalMatches: totals.totalMatches, totalSeeds: '3' });
  await controller.setPresentation({ languages: ['ru'] });
  expect(get(controller)).toMatchObject({ page: 7, knownLastPage: 7, totalMatches: totals.totalMatches, totalSeeds: '3', canNext: false });
  expect(api.countedOpen).toHaveBeenCalledTimes(1);
  await controller.goTo(8); expect(api.seek).toHaveBeenCalledTimes(3);
  await controller.goTo(1); expect(get(controller)).toMatchObject({ knownLastPage: 7, canNext: true });
  controller.live({}); expect(get(controller)).toMatchObject({ mode: 'live', totalMatches: null, totalSeeds: null, knownLastPage: null });
});

it('rejects totals that cannot drive native pagination and cancels the returned capability', async () => {
  const api = transport(); api.countedOpen.mockResolvedValueOnce({ ...totals, totalPages: '9007199254740992' });
  const controller = viewer(api); await controller.run({});
  expect(get(controller)).toMatchObject({ phase: 'error', error: { code: 'PROTOCOL' }, totalMatches: null });
  expect(api.cancel).toHaveBeenCalledWith('fixed'); expect(api.seek).not.toHaveBeenCalled();
});

it('does not advertise exact totals after snapshot invalidation', async () => {
  const { ApiError } = await import('../src/lib/api');
  const api = transport(); const controller = viewer(api); await controller.run({});
  api.seek.mockRejectedValueOnce(new ApiError('SNAPSHOT_EXPIRED', 'replaced')); await controller.goTo(2);
  expect(get(controller)).toMatchObject({ totalMatches: null, totalSeeds: null, knownLastPage: null, canSeek: false });
});
