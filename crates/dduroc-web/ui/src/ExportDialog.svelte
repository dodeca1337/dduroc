<script lang="ts">
  import { onMount } from 'svelte';
  import { Button, InlineNotification, Modal, Select, SelectItem, TextInput } from 'carbon-components-svelte';
  import QueryFilters from './QueryFilters.svelte';
  import { defaultFilters, makeQuery, type Filters } from './lib/filters';
  import { exportHistory, ExportCleanupError, type ExportResult } from './lib/export';
  import type { PresentationInput, Transport } from './lib/types';
  let { open = $bindable(false), transport, initialFilters, namespaces, presentation, running = $bindable(false) }: { open?: boolean; transport: Transport; initialFilters: Filters; namespaces: string[]; presentation?: PresentationInput; running?: boolean } = $props();
  let filters = $state<Filters>(defaultFilters());
  let format = $state<'csv' | 'jsonl'>('jsonl');
  let maxRecords = $state('10000');
  let maxMiB = $state('16');
  let cancelling = $state(false);
  let progress = $state({ records: 0, bytes: 0 });
  let result = $state<ExportResult | null>(null);
  let error = $state('');
  let cancelled = $state(false);
  let cleanupBlocked = $state(false);
  let operation: AbortController | null = null;
  let disposed = false;
  const downloadUrls = new Map<string, ReturnType<typeof setTimeout>>();

  function reset() {
    if (running || cleanupBlocked) return;
    filters = structuredClone($state.snapshot(initialFilters));
    result = null; error = ''; cancelled = false; cancelling = false; progress = { records: 0, bytes: 0 };
  }
  function cancel() { if (running) { cancelling = true; operation?.abort(); } }
  function close() { cancel(); result = null; }
  function dismiss() { close(); open = false; }
  function integer(value: string, maximum: number, label: string): number {
    if (!/^[1-9]\d*$/.test(value) || BigInt(value) > BigInt(maximum)) throw new Error(`${label} must be a whole number from 1 to ${maximum}.`);
    return Number(value);
  }
  async function prepare() {
    if (running || cleanupBlocked) return;
    try {
      const query = makeQuery(filters);
      const options = { query, presentation, metadataOnly: filters.metadataOnly, format, maxRecords: integer(maxRecords, 100000, 'Record limit'), maxBytes: integer(maxMiB, 32, 'File limit in MiB') * 1024 * 1024 };
      operation = new AbortController(); running = true; cancelling = false; cancelled = false; error = ''; result = null; progress = { records: 0, bytes: 0 };
      const completed = await exportHistory(transport, options, operation.signal, update => { if (!disposed) progress = update; });
      if (!disposed && !operation.signal.aborted) { result = completed; if (completed.cleanup === 'unconfirmed') cleanupBlocked = true; }
    } catch (cause) {
      if (!disposed) {
        if (cause instanceof ExportCleanupError) { cleanupBlocked = true; error = cause.message; }
        else if ((cause instanceof Error || cause instanceof DOMException) && cause.name === 'AbortError') cancelled = true;
        else error = cause instanceof Error ? cause.message : 'The export could not finish. Retry with a narrower scope.';
      }
    } finally { running = false; cancelling = false; operation = null; }
  }
  function download() {
    if (!result || running) return;
    const url = URL.createObjectURL(result.blob);
    const link = document.createElement('a'); link.href = url; link.download = result.filename;
    document.body.append(link); link.click(); link.remove();
    downloadUrls.set(url, setTimeout(() => { URL.revokeObjectURL(url); downloadUrls.delete(url); }, 1000));
  }
  onMount(() => { window.addEventListener('pagehide', close); return () => { disposed = true; close(); window.removeEventListener('pagehide', close); for (const [url, timer] of downloadUrls) { clearTimeout(timer); URL.revokeObjectURL(url); } downloadUrls.clear(); }; });
</script>

<Modal bind:open size="lg" modalHeading="Export history" aria-label="Export history" modalLabel="Independent finite read" hasForm hasScrollingContent lazy
  selectorPrimaryFocus="#export-format" preventCloseOnClickOutside shouldSubmitOnEnter={false}
  primaryButtonText={result ? result.complete ? 'Download file' : 'Download partial file' : 'Prepare export'}
  primaryButtonDisabled={cleanupBlocked && !result} primaryButtonLoading={running} primaryButtonLoadingDescription={cancelling ? 'Cancelling…' : 'Preparing…'}
  secondaryButtonText={running ? cancelling ? 'Cancelling…' : 'Cancel export' : 'Close'}
  on:open={reset} on:close={close} on:click:button--secondary={running ? cancel : dismiss} on:submit={result ? download : prepare}>
  <p class="detail-note">Message language: {presentation ? presentation.languages.join(' → ') : 'Service default'}; then the first language in each schema. Recorded free text is unchanged.</p>
  <p class="export-intro">Read matching history into a new file. This has its own cursor and editable scope; it does not export only the rows loaded in the viewer.</p>
  {#if !result}
    <fieldset class="export-scope" disabled={running}>
      <legend class="bx--assistive-text">Export scope and limits</legend>
      <div class="export-options">
        <Select id="export-format" labelText="File format" bind:selected={format}><SelectItem value="jsonl" text="JSONL · tagged records" /><SelectItem value="csv" text="CSV · records and metadata" /></Select>
        <TextInput id="export-record-limit" labelText="Record limit" helperText="Up to 100,000 records, including state seeds" bind:value={maxRecords} />
        <TextInput id="export-byte-limit" labelText="File limit (MiB)" helperText="Up to 32 MiB; metadata and summary are included" bind:value={maxMiB} />
      </div>
      <h3 class="export-scope-heading">Matching history</h3>
      <QueryFilters bind:filters {namespaces} prefix="export" advancedOpen />
    </fieldset>
    <p class="detail-note">Both time bounds are inclusive. Boot identifies a journal run; offsets start when that run opens. Nested fields use the bounded viewer projection; raw payloads remain available when requested.</p>
  {/if}
  {#if running}<p class="export-progress" role="status">{cancelling ? 'Cancelling and closing the export cursor…' : 'Reading history…'} {progress.records} records · {progress.bytes} bytes. No file is downloaded until preparation finishes.</p>{/if}
  {#if cancelled}<InlineNotification kind="info" title="Export cancelled" subtitle="The export stopped and cursor cleanup was attempted. No file was downloaded. You can prepare a new export." hideCloseButton />{/if}
  {#if cleanupBlocked}<InlineNotification kind="warning" title="Export cursor cleanup is unconfirmed" subtitle="A new export is disabled. Reload after the server session has expired before preparing another export." hideCloseButton />{/if}
  {#if error}<InlineNotification kind="error" title="Export could not finish" subtitle={error} hideCloseButton />{/if}
  {#if result}
    <InlineNotification kind={result.complete ? 'success' : 'warning'} title={result.complete ? 'File ready' : 'Partial export — review before saving'} subtitle={result.complete ? 'This finite read completed within its limits. Download the file below.' : 'This file does not represent complete matching history. The filename and file summary identify it as partial.'} hideCloseButton />
    <dl class="export-summary"><div><dt>File</dt><dd><code>{result.filename}</code></dd></div><div><dt>Records</dt><dd>{result.records}</dd></div><div><dt>File bytes</dt><dd>{result.bytes}</dd></div><div><dt>Read outcome</dt><dd>{result.complete ? 'Complete finite read' : 'Partial history'}</dd></div></dl>
    {#if result.warnings.length}<ul class="export-warnings">{#each result.warnings as warning}<li>{warning}</li>{/each}</ul>{/if}
    <Button kind="ghost" size="sm" disabled={cleanupBlocked} on:click={() => { result = null; error = ''; }}>Edit scope and prepare again</Button>
  {/if}
</Modal>
