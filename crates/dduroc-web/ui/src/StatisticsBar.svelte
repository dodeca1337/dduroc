<script lang="ts">
  import { onMount } from 'svelte';
  import { Accordion, AccordionItem, Button } from 'carbon-components-svelte';
  import { ApiError } from './lib/api';
  import { calendarTime } from './lib/format';
  import type { StorageStatistics, Transport } from './lib/types';
  let { statistics, error, transport = null, refresh = async () => {} }: { statistics: StorageStatistics | null; error: string | null; transport?: Transport | null; refresh?: () => Promise<void> } = $props();
  let running = $state(false); let stopping = $state(false); let steps = $state(0); let notice = $state('');
  let operation: AbortController | null = null; let disposed = false;
  async function calculate() {
    if (running || !transport?.backfillStatistics) return;
    const backfill = transport.backfillStatistics.bind(transport);
    running = true; stopping = false; steps = 0; notice = ''; operation = new AbortController();
    try {
      let more = true;
      while (more && !stopping && !operation.signal.aborted) {
        const step = await backfill(operation.signal);
        if (typeof step.more !== 'boolean') throw new ApiError('PROTOCOL', 'Invalid indexing step response.');
        more = step.more; if (!disposed) steps++;
        // A bounded step has completed. Let Stop run before admitting the next.
        if (more) await new Promise<void>(resolve => setTimeout(resolve, 0));
      }
      if (!disposed && !operation.signal.aborted) {
        const outcome = more ? `Stopped after ${steps} completed steps. Retained counts may still be incomplete.` : 'Retained counts calculated.';
        try { await refresh(); notice = `${outcome} Statistics refreshed.`; } catch { notice = `${outcome} Statistics could not be refreshed; refresh them before trying again.`; }
      }
    } catch (cause) {
      if (!disposed) {
        const uncertain = !(cause instanceof ApiError) || ['NETWORK', 'PROTOCOL', 'VIEWER_LIMIT'].includes(cause.code);
        notice = uncertain ? 'The last indexing step has an uncertain outcome. Refresh statistics before explicitly trying again; no step was retried automatically.' : `Indexing could not continue: ${cause.message}`;
      }
    } finally { running = false; stopping = false; operation = null; }
  }
  function leave() { stopping = true; operation?.abort(); }
  onMount(() => { window.addEventListener('pagehide', leave); return () => { disposed = true; leave(); window.removeEventListener('pagehide', leave); }; });
  function count(value: string | null): string { return value === null ? 'Not indexed' : BigInt(value).toLocaleString('en-US'); }
  const allocated = $derived(statistics?.roots?.reduce((total, root) => total + BigInt(root.allocatedBytes), 0n) ?? null);
  const budget = $derived(statistics?.roots?.length && statistics.roots.every(root => root.physicalLimitBytes !== null)
    ? statistics.roots.reduce((total, root) => total + BigInt(root.physicalLimitBytes!), 0n) : null);
  function snapshotTime(utc: string): string { return calendarTime(utc).replace(/(\d{2}:\d{2}:\d{2})\.\d+ UTC$/, '$1 UTC'); }
  function bytes(value: string | bigint): string {
    const size = BigInt(value);
    if (size < 1024n) return `${size} B`;
    if (size < 1048576n) return `${size / 1024n}.${(size % 1024n) * 10n / 1024n} KiB`;
    return `${size / 1048576n}.${(size % 1048576n) * 10n / 1048576n} MiB`;
  }
</script>
<section class="storage-statistics" aria-label="Global storage statistics">
  {#if statistics}
    <div class="statistics-retained"><strong>All stored history</strong><span>Records <b>{count(statistics.retainedRecords)}</b></span><span title={`${statistics.logicalRetainedBytes} bytes`}>Retained data <b>{bytes(statistics.logicalRetainedBytes)}</b></span><span title={allocated === null ? 'Disk allocation is unavailable.' : `${allocated} bytes allocated across storage roots`}>Allocated disk <b>{allocated === null ? 'Unavailable' : bytes(allocated)}</b></span><span title={budget === null ? 'Configured storage budget unavailable; this is not filesystem capacity.' : `${budget} bytes configured across storage roots; this is not filesystem capacity.`}>Storage budget <b>{budget === null ? 'Unavailable' : bytes(budget)}</b></span></div>
    {#if statistics.capturedAt}<span class="statistics-note" title={statistics.capturedAt}>Snapshot {snapshotTime(statistics.capturedAt)}</span>{/if}
    {#if statistics.retainedRecords === null || statistics.retainedBlocks === null}<span class="statistics-note">Retained counts unavailable · {statistics.countingState.toLowerCase().replaceAll('_', ' ')}</span>{/if}
    <Accordion class="statistics-details"><AccordionItem title="Storage and writer details">
      <div class="statistics-runtime"><span>Blocks <b>{count(statistics.retainedBlocks)}</b></span><span>Channels <b>{count(statistics.channels)}</b></span></div>
      <p class="statistics-note">Retained data is logical history. Allocated disk includes storage logs, rewrites and metadata. The storage budget is configured policy, not free space or filesystem capacity.</p>
      {#each statistics.roots ?? [] as root (root.index)}<div class="statistics-runtime"><strong>Root {root.index}</strong><span title={`${root.allocatedBytes} bytes`}>Allocated {bytes(root.allocatedBytes)}</span><span title={`${root.logBytes} bytes`}>Logs {bytes(root.logBytes)}</span><span title={`${root.rewriteBytes} bytes`}>Rewrites {bytes(root.rewriteBytes)}</span><span title={`${root.metadataBytes} bytes`}>Metadata {bytes(root.metadataBytes)}</span><span title={root.physicalLimitBytes === null ? 'Configured budget unavailable' : `${root.physicalLimitBytes} bytes`}>Budget {root.physicalLimitBytes === null ? 'Unavailable' : bytes(root.physicalLimitBytes)}</span></div>{/each}
      {#if statistics.runtime}<div class="statistics-runtime"><span>Writer this process:</span><span>{count(statistics.runtime.recordsWritten)} records written</span><span>{count(statistics.runtime.blocksWritten)} blocks written</span><span title={`${statistics.runtime.bytesWritten} bytes`}>{bytes(statistics.runtime.bytesWritten)} written</span><span>{count(statistics.runtime.dropped)} dropped</span><span>{count(statistics.runtime.rejected)} rejected</span><span>{count(statistics.runtime.ioErrors)} I/O errors</span></div>{:else}<span class="statistics-note">Writer counters unavailable for this offline reader.</span>{/if}
    </AccordionItem></Accordion>
    {#if running || ((statistics.retainedRecords === null || statistics.retainedBlocks === null) && statistics.runtime && transport?.backfillStatistics)}
      <div class="statistics-indexing">{#if running}<span role="status">{stopping ? 'Stopping after the current bounded step…' : 'Calculating retained counts…'} {steps} steps completed.</span><Button kind="ghost" size="sm" disabled={stopping} on:click={() => { stopping = true; }}>Stop indexing</Button>{:else}<Button kind="ghost" size="sm" on:click={() => void calculate()}>Calculate retained counts</Button><span>Processes bounded steps; Stop waits for the current step.</span>{/if}</div>
    {/if}
    {#if notice}<p class="statistics-note" role="status">{notice}</p>{#if !running}<Button kind="ghost" size="sm" on:click={async () => { try { await refresh(); notice = 'Statistics refreshed.'; } catch { notice = 'Statistics could not be refreshed. Try again when the connection is available.'; } }}>Refresh statistics</Button>{/if}{/if}
    {#if error}<p class="statistics-note" role="status">{error} The last statistics snapshot is shown.</p>{/if}
  {:else}<span>{error ?? 'Loading global storage statistics…'}</span>{/if}
</section>
