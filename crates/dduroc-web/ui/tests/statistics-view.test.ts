import { afterEach, expect, it, vi } from 'vitest';
import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/svelte';
import { ApiError, HttpTransport } from '../src/lib/api';
import StatisticsBar from '../src/StatisticsBar.svelte';
import type { StorageStatistics } from '../src/lib/types';
afterEach(() => { cleanup(); vi.restoreAllMocks(); });
const statistics: StorageStatistics = { countingState: 'READY', retainedRecords: '9007199254740993', retainedBlocks: '1001', logicalRetainedBytes: '5242880', channels: '17', capturedAt: '2026-11-10T10:11:12.123456789+00:00', roots: [{ index: '0', allocatedBytes: '9007199254740993', logBytes: '2048', rewriteBytes: '1024', metadataBytes: '512', physicalLimitBytes: '18014398509481986' }, { index: '1', allocatedBytes: '7', logBytes: '4', rewriteBytes: '2', metadataBytes: '1', physicalLimitBytes: '14' }], runtime: { recordsWritten: '4', blocksWritten: '1', bytesWritten: '2048', dropped: '2', rejected: '3', ioErrors: '0' } };

it('keeps exact allocated disk and policy budgets separate from logical data and writer activity', async () => {
  render(StatisticsBar, { statistics, error: null });
  expect(screen.getByText('9,007,199,254,740,993')).toBeTruthy();
  expect(screen.getByText('5.0 MiB')).toBeTruthy();
  expect(screen.getByTitle('9007199254741000 bytes allocated across storage roots').textContent).toContain('Allocated disk');
  expect(screen.getByTitle('18014398509482000 bytes configured across storage roots; this is not filesystem capacity.').textContent).toContain('Storage budget');
  expect(screen.getByTitle('2026-11-10T10:11:12.123456789+00:00').textContent).toBe('Snapshot 2026-11-10 10:11:12 UTC');
  expect(screen.getByRole('button', { name: 'Storage and writer details' }).getAttribute('aria-expanded')).toBe('false');
  await fireEvent.click(screen.getByRole('button', { name: 'Storage and writer details' }));
  expect(screen.getByText('4 records written')).toBeTruthy();
  expect(screen.getByText('Writer this process:')).toBeTruthy();
  expect(screen.getByLabelText('Global storage statistics')).toBeTruthy();
});

it('does not turn unknown retained counts or offline runtime counters into zero', () => {
  render(StatisticsBar, { statistics: { ...statistics, countingState: 'BACKFILL_REQUIRED', retainedRecords: null, retainedBlocks: null, runtime: null }, error: null });
  expect(screen.getAllByText('Not indexed')).toHaveLength(2);
  expect(screen.getByText(/backfill required/)).toBeTruthy();
  expect(screen.getByText('Writer counters unavailable for this offline reader.')).toBeTruthy();
  expect(screen.queryByText('0 records written')).toBeNull();
});


it('stops only after the current bounded indexing step settles and refreshes once', async () => {
  const api = new HttpTransport({ graphqlEndpoint: 'http://localhost/graphql', title: 'Test', maxRows: 1000 });
  let finish!: (value: { more: boolean }) => void;
  const pending = new Promise<{ more: boolean }>(resolve => { finish = resolve; });
  const backfill = vi.spyOn(api, 'backfillStatistics').mockReturnValue(pending);
  const refresh = vi.fn<() => Promise<void>>().mockResolvedValue(undefined);
  render(StatisticsBar, { statistics: { ...statistics, retainedRecords: null }, error: null, transport: api, refresh });
  await fireEvent.click(screen.getByRole('button', { name: 'Calculate retained counts' }));
  await waitFor(() => expect(backfill).toHaveBeenCalledTimes(1));
  await fireEvent.click(screen.getByRole('button', { name: 'Stop indexing' }));
  expect(backfill.mock.calls[0][0]?.aborted).toBe(false);
  expect(refresh).not.toHaveBeenCalled();
  finish({ more: true });
  await screen.findByText(/Stopped after 1 completed steps/);
  expect(backfill).toHaveBeenCalledTimes(1); expect(refresh).toHaveBeenCalledTimes(1);
});

it('does not retry an indexing step whose network acknowledgement is uncertain', async () => {
  const api = new HttpTransport({ graphqlEndpoint: 'http://localhost/graphql', title: 'Test', maxRows: 1000 });
  const backfill = vi.spyOn(api, 'backfillStatistics').mockRejectedValue(new ApiError('NETWORK', 'response lost'));
  render(StatisticsBar, { statistics: { ...statistics, retainedRecords: null }, error: null, transport: api });
  await fireEvent.click(screen.getByRole('button', { name: 'Calculate retained counts' }));
  await screen.findByText(/uncertain outcome/);
  expect(backfill).toHaveBeenCalledTimes(1);
  expect(screen.getByRole('button', { name: 'Refresh statistics' })).toBeTruthy();
});

it('does not invent allocated space or a partial budget when roots or policy limits are unavailable', async () => {
  const app = render(StatisticsBar, { statistics: { ...statistics, roots: null }, error: null });
  expect(screen.getByTitle('Disk allocation is unavailable.').textContent).toContain('Unavailable');
  await app.rerender({ statistics: { ...statistics, roots: statistics.roots!.map((root, index) => ({ ...root, physicalLimitBytes: index === 1 ? null : root.physicalLimitBytes })) }, error: null });
  expect(screen.getByTitle('Configured storage budget unavailable; this is not filesystem capacity.').textContent).toContain('Unavailable');
  expect(screen.getByTitle('9007199254741000 bytes allocated across storage roots')).toBeTruthy();
});
