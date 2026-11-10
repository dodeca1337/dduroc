import { afterEach, expect, it, vi } from 'vitest';
import { get } from 'svelte/store';
import { ViewerController } from '../src/lib/controller';
import { ApiError } from '../src/lib/api';
import type { Entry, Page, SeekPage, Transport } from '../src/lib/types';

const record = (text: string): Entry => ({ namespace: 'fixed', class: 'DEFAULT', at: { boot: '1', micros: '42' }, utc: null, span: null, kind: 'TEXT', text: { level: 'INFO', target: 'test', text } });
const page = (text: string, exhausted = false): Page => ({ entries: [record(text)], seeds: [], damaged: [], unanchored: [], exhausted, truncated: false });
const result = (number: number, text = String(number), exhausted = false): SeekPage => ({ revision: String(number), reachedPage: String(number), targetReached: true, exhausted, page: page(text, exhausted) });
function deferred<T>() { let resolve!: (value: T) => void; const promise = new Promise<T>(yes => { resolve = yes; }); return { resolve, promise }; }
function transport() {
  return { catalog: vi.fn<Transport['catalog']>(), open: vi.fn<Transport['open']>().mockResolvedValue({ id: 'fixed-snapshot', revision: '0' }),
    page: vi.fn<Transport['page']>(), seek: vi.fn<NonNullable<Transport['seek']>>().mockImplementation(async (_session, target) => result(target)),
    cancel: vi.fn<Transport['cancel']>().mockResolvedValue(undefined), close: vi.fn<Transport['close']>().mockResolvedValue(undefined), follow: vi.fn<Transport['follow']>() };
}
const controllers: ViewerController[] = [];
const viewer = (api: Transport) => { const value = new ViewerController(api, 1000, 8 * 1024 * 1024, 64, 2); controllers.push(value); return value; };
afterEach(() => { controllers.splice(0).forEach(value => value.dispose()); });

it('opens once, selects nonadjacent pages, replays an evicted page and does not skip cache holes', async () => {
  const api = transport(); const controller = viewer(api); await controller.run({ namespaces: ['fixed'] });
  expect(api.seek.mock.calls[0][1]).toBe(1); expect(api.page).not.toHaveBeenCalled();
  await controller.goTo(75); expect(get(controller)).toMatchObject({ page: 75, cachedPageNumbers: [1, 75], canPrevious: true });
  await controller.previous(); expect(get(controller).page).toBe(74);
  expect(get(controller).cachedPageNumbers).toEqual([74, 75]);
  await controller.goTo(1); expect(get(controller).page).toBe(1);
  await controller.next(); expect(get(controller).page).toBe(2);
  expect(api.seek.mock.calls.map(call => call[1])).toEqual([1, 75, 74, 1, 2]);
  expect(api.open).toHaveBeenCalledTimes(1); expect(api.close).not.toHaveBeenCalled();
  expect(api.seek.mock.calls.every(call => call[0].id === 'fixed-snapshot')).toBe(true);
  expect(get(controller).cachedPages).toBe(2);
});

it('reports bounded seek progress, preserves CAS revisions and stops at the actual last page', async () => {
  const api = transport(); const controller = viewer(api); await controller.run({});
  api.seek.mockResolvedValueOnce({ revision: '9007199254740993', reachedPage: '33', targetReached: false, exhausted: false, page: null });
  const last = deferred<SeekPage>(); api.seek.mockReturnValueOnce(last.promise);
  const moving = controller.goTo(100);
  await vi.waitFor(() => expect(api.seek).toHaveBeenCalledTimes(3));
  expect(get(controller)).toMatchObject({ page: 1, navigationTarget: 100, navigationProgress: 33, pageBusy: true });
  expect(api.seek.mock.calls[2][0].revision).toBe('9007199254740993');
  last.resolve({ ...result(79, 'last', true), targetReached: false }); await moving;
  expect(get(controller)).toMatchObject({ page: 79, knownLastPage: 79, canNext: false, pageBusy: false });
  expect(get(controller).notice).toContain('beyond the end'); expect(api.close).not.toHaveBeenCalled();
  await controller.goTo(1); expect(get(controller).page).toBe(1); expect(get(controller).canNext).toBe(true);
  expect(get(controller).knownLastPage).toBe(79);
});

it('retires a cancelled seek capability and ignores an uncertain late reply', async () => {
  const api = transport(); const controller = viewer(api); await controller.run({});
  const late = deferred<SeekPage>(); api.seek.mockReturnValueOnce(late.promise);
  const moving = controller.goTo(100); const signal = api.seek.mock.calls[1][4]; controller.stop();
  expect(signal.aborted).toBe(true); expect(api.cancel).toHaveBeenCalledWith('fixed-snapshot');
  late.resolve(result(100)); await moving;
  expect(get(controller)).toMatchObject({ page: 1, phase: 'stopped', canSeek: false, pageBusy: false });
  await controller.goTo(100); expect(api.seek).toHaveBeenCalledTimes(2);
});

it('serializes rapid language changes after a pending page CAS, hides stale rows and applies only the newest preference', async () => {
  const api = transport(); const controller = viewer(api); await controller.run({ namespaces: ['fixed'] });
  const late = deferred<SeekPage>(); api.seek.mockReturnValueOnce(late.promise);
  const moving = controller.goTo(4);
  const ru = controller.setPresentation({ languages: ['ru'] }); const en = controller.setPresentation({ languages: ['en'] });
  expect(get(controller)).toMatchObject({ rows: [], languageBusy: true });
  const seen: string[] = []; const stop = controller.subscribe(value => seen.push(...value.rows.map(row => row.entry.text!.text)));
  late.resolve({ ...result(4, 'stale-language'), revision: '9007199254740993' });
  api.seek.mockResolvedValueOnce({ ...result(1, 'new-language'), revision: '9007199254740994' });
  await Promise.all([moving, ru, en]); stop();
  expect(seen).not.toContain('stale-language');
  expect(api.seek).toHaveBeenCalledTimes(3); expect(api.seek.mock.calls[2][0].revision).toBe('9007199254740993');
  expect(api.seek.mock.calls[2][1]).toBe(1); expect(api.seek.mock.calls[2][5]).toEqual({ languages: ['en'] });
  expect(api.open).toHaveBeenCalledTimes(1); expect(api.cancel).not.toHaveBeenCalled();
  expect(get(controller)).toMatchObject({ page: 1, languageBusy: false, cachedPageNumbers: [1] });
});

it('refreshes the current page after EOF with service-default presentation and drops translated caches', async () => {
  const api = transport(); const controller = viewer(api); await controller.run({}, false, { languages: ['ru'] });
  api.seek.mockResolvedValueOnce(result(3, 'Russian', true)); await controller.goTo(3);
  api.seek.mockResolvedValueOnce(result(3, 'service default', true)); await controller.setPresentation();
  expect(api.seek.mock.calls[2][1]).toBe(3); expect(api.seek.mock.calls[2][5]).toBeUndefined();
  expect(get(controller).cachedPageNumbers).toEqual([3]); expect(get(controller).rows[0].entry.text!.text).toBe('service default');
  expect(api.open).toHaveBeenCalledTimes(1); expect(api.close).not.toHaveBeenCalled();
});

it.each(['RETENTION_GAP', 'SNAPSHOT_EXPIRED', 'SESSION_EXPIRED'])('invalidates cached navigation when %s is known', async code => {
  const api = transport(); const controller = viewer(api); await controller.run({}); await controller.goTo(2);
  api.seek.mockRejectedValueOnce(new ApiError(code, 'changed')); await controller.goTo(3);
  expect(get(controller)).toMatchObject({ canPrevious: false, canNext: false, canSeek: false, cachedPages: 0 });
  await controller.goTo(1); await controller.previous(); await controller.next();
  expect(api.seek).toHaveBeenCalledTimes(3); expect(api.open).toHaveBeenCalledTimes(1);
});

it('automatically resynchronizes live using its captured query and ignores retired callbacks', async () => {
  const api = transport(); const stop = vi.fn(); api.follow.mockReturnValue(stop);
  const query = { namespaces: ['fixed'] }; const controller = viewer(api); controller.live(query, false, { languages: ['en'] });
  const old = api.follow.mock.calls[0][2]; old.next({ ...page('old'), ended: false });
  query.namespaces[0] = 'unsaved'; await controller.setPresentation({ languages: ['ru'] });
  expect(stop).toHaveBeenCalledTimes(1); expect(api.follow.mock.calls[1][0]).toEqual({ namespaces: ['fixed'] });
  expect(api.follow.mock.calls[1][3]).toEqual({ languages: ['ru'] }); expect(get(controller).rows).toEqual([]);
  old.next({ ...page('stale'), ended: false }); old.complete();
  api.follow.mock.calls[1][2].next({ ...page('new'), ended: false });
  expect(get(controller).rows[0].entry.text!.text).toBe('new'); expect(get(controller).notice).toContain('resynchronized');
});
