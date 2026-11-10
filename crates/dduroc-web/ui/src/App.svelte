<script lang="ts">
  import { onMount, tick } from 'svelte';
  import type { CarbonTheme } from 'carbon-components-svelte/src/Theme/Theme.svelte';
  import { Accordion, AccordionItem, Button, Content, DataTable, DataTableSkeleton, Form, Header, HeaderGlobalAction, HeaderUtilities, InlineLoading, Pagination, InlineNotification, Select, SelectItem, Tag, Theme } from 'carbon-components-svelte';
  import { ArrowRight, Renew, Stop, Play, Sun, Asleep } from 'carbon-icons-svelte';
  import { HttpTransport, loadBootstrap } from './lib/api';
  import { ViewerController, initialState, type ViewerState } from './lib/controller';
  import type { DemoScenario } from './lib/demo';
  import { activeFilterCount, defaultFilters, makeQuery, type Filters } from './lib/filters';
  import { defaultMessageLanguages, loadMessageLanguages, saveMessageLanguages, messagePresentation, messageLanguageChoices, messageFallbackBadge, type MessageLanguages } from './lib/languages';
  import { entryLevel, entrySummary, kindLabel, timeText, type TimeDisplay } from './lib/format';
  import type { Entry, Transport } from './lib/types';
  import EntryDetails from './EntryDetails.svelte';
  import StatisticsBar from './StatisticsBar.svelte';
  import NamespaceFilter from './NamespaceFilter.svelte';
  import QueryFilters from './QueryFilters.svelte';
  import ExportDialog from './ExportDialog.svelte';

  let view = $state<ViewerState>(initialState());
  let filters = $state<Filters>(defaultFilters());
  let timeDisplay = $state<TimeDisplay>('calendar');
  let messageLanguages = $state<MessageLanguages>(defaultMessageLanguages());
  let exportRunning = $state(false);
  const presentation = $derived(messagePresentation(messageLanguages));
  const languageChoices = $derived(messageLanguageChoices(view.catalog, messageLanguages));
  let theme = $state<CarbonTheme>('white');
  const darkTheme = $derived(theme === 'g100' || theme === 'g90' || theme === 'g80');
  let themeReady = $state(false);
  let controller: ViewerController | undefined;
  let transport = $state<Transport | null>(null);
  let exportOpen = $state(false);
  let unsubscribe: (() => void) | undefined;
  let disposed = false;
  let bootGeneration = 0;
  let title = $state('dduroc');
  let booting = $state(true);
  let bootError = $state('');
  let filterError = $state('');
  let demo = $state(false);
  let scenario = $state<DemoScenario>('history');
  let scenarios = $state<{ value: DemoScenario; label: string }[]>([]);
  let liveAvailable = $state(false);
  let rowLimit = $state(1000);
  let advancedOpen = $state(true);
  let filtersOpen = $state(false);
  const headers = [
    { key: 'time', value: 'Time' }, { key: 'level', value: 'Level' },
    { key: 'namespace', value: 'Namespace' }, { key: 'kind', value: 'Kind' },
    { key: 'message', value: 'Message / value' },
  ] as const;
  const rows = $derived(view.rows.map(({ id, entry }) => ({ id, time: timeText(entry, timeDisplay), level: entryLevel(entry), namespace: entry.namespace, kind: kindLabel(entry), message: entrySummary(entry) })));
  const entries = $derived(new Map<string, Entry>(view.rows.map(row => [row.id, row.entry])));
  const busy = $derived(view.phase === 'counting' || view.phase === 'loading' || view.pageBusy || view.languageBusy);
  const active = $derived(activeFilterCount(filters));
  const names = $derived([...new Set(view.catalog?.namespaces.map(row => row.namespace) ?? [])]);
  const status = $derived(({ idle: 'Ready to query', counting: 'Counting matching records', loading: 'Opening snapshot', ready: 'Snapshot open', 'loading-more': 'Reading next page', live: 'Live stream', stopped: 'Stopped', complete: 'Snapshot complete', error: 'Read stopped', limit: 'Viewer limit reached' })[view.phase]);

  async function start() {
    const generation = ++bootGeneration;
    const current = () => !disposed && generation === bootGeneration;
    controller?.dispose(); unsubscribe?.(); booting = true; bootError = ''; view = initialState();
    try {
      demo = import.meta.env.DEV && new URLSearchParams(location.search).get('demo') === '1';
      if (import.meta.env.DEV && demo) { const preview = await import('./lib/demo'); if (!current()) return; scenarios = preview.scenarios; title = 'dduroc'; rowLimit = scenario === 'limit' ? 6 : 100; liveAvailable = true; transport = new preview.DemoTransport(scenario); controller = new ViewerController(transport, rowLimit); }
      else { const config = await loadBootstrap(); if (!current()) return; title = config.title; rowLimit = config.maxRows; liveAvailable = !!config.subscriptionEndpoint; transport = new HttpTransport(config); controller = new ViewerController(transport, rowLimit); }
      document.title = `${title} · Journal`;
      unsubscribe = controller.subscribe(value => { view = value; });
      await controller.discover();
      if (!current()) return;
      await controller.run(makeQuery(filters), filters.metadataOnly, presentation);
    } catch (error) { if (current()) bootError = error instanceof Error ? error.message : 'The viewer could not connect.'; }
    finally { if (current()) booting = false; }
  }
  function submit(live = false) {
    try { const query = makeQuery(filters, live); filterError = ''; if (live) controller?.live(query, filters.metadataOnly, presentation); else void controller?.run(query, filters.metadataOnly, presentation); }
    catch (error) { filterError = error instanceof Error ? error.message : 'Check the filters.'; advancedOpen = true; filtersOpen = true; }
  }
  function changeLanguage(primary: string, fallback: string) {
    if (exportRunning) return;
    messageLanguages = { primary, fallback: primary && primary.toLowerCase().replaceAll('_', '-') !== fallback.toLowerCase().replaceAll('_', '-') ? fallback : '' };
    saveMessageLanguages(messageLanguages); exportOpen = false; void controller?.setPresentation(messagePresentation(messageLanguages));
  }
  function clear() { filters = defaultFilters(); filterError = ''; submit(); }
  onMount(() => {
    messageLanguages = loadMessageLanguages();
    // Let Carbon restore its persisted theme before mounting the bound control.
    void tick().then(() => { if (!disposed) { theme = darkTheme ? 'g100' : 'white'; themeReady = true; } });
    const hide = () => { ++bootGeneration; controller?.dispose(); };
    const show = (event: PageTransitionEvent) => { if (event.persisted) void start(); };
    window.addEventListener('pagehide', hide); window.addEventListener('pageshow', show);
    void start();
    return () => { disposed = true; hide(); unsubscribe?.(); window.removeEventListener('pagehide', hide); window.removeEventListener('pageshow', show); };
  });
</script>

<Theme bind:theme persist persistKey="dduroc.viewer.theme" />
<Header companyName="" platformName={title} href="#main-content"><HeaderUtilities><HeaderGlobalAction icon={darkTheme ? Sun : Asleep} iconDescription={darkTheme ? 'Switch to light theme' : 'Switch to dark theme'} aria-label="Dark theme" aria-pressed={darkTheme} disabled={!themeReady} tooltipPosition="bottom" tooltipAlignment="end" on:click={() => { theme = darkTheme ? 'white' : 'g100'; }} /></HeaderUtilities></Header>
<Content>
  <div class="journal-page">
    <StatisticsBar statistics={view.statistics} error={view.statisticsError} {transport} refresh={async () => { await controller?.discover(); if (view.statisticsError) throw new Error(view.statisticsError); }} />
    <div class="page-heading"><div><h1>Journal</h1></div><div class="page-tools"><Tag type={view.phase === 'live' ? 'green' : 'gray'}>{status}</Tag></div></div>

    {#if demo}
      <div class="demo-strip"><div><strong>Demo preview</strong><span>Deterministic sample data. No backend connection.</span></div><Select id="preview-scenario" labelText="Preview scenario" size="sm" bind:selected={scenario} on:change={(event) => { scenario = (event.target as HTMLSelectElement).value as DemoScenario; void start(); }}>{#each scenarios as item}<SelectItem value={item.value} text={item.label} />{/each}</Select></div>
    {/if}
    {#if bootError}<InlineNotification kind="error" title="Viewer could not start" subtitle={bootError} hideCloseButton /><div class="retry-action"><Button kind="tertiary" icon={Renew} on:click={() => void start()}>Try again</Button></div>{/if}

    <section class="filter-panel" aria-label="Query filters">
      <Form on:submit={(event) => { event.preventDefault(); submit(); }}>
        <div class="namespace-scope"><NamespaceFilter bind:filters namespaces={names} prefix="query" /></div>
        <Accordion><AccordionItem title={`Filters · ${active ? `${active} selected` : 'All history'}`} bind:open={filtersOpen}>
          <QueryFilters bind:filters namespaces={names} prefix="query" bind:advancedOpen includeNamespace={false} />
        </AccordionItem></Accordion>
        {#if filterError}<div class="filter-error"><InlineNotification kind="error" title="Check your filters" subtitle={filterError} hideCloseButton /></div>{/if}
        <div class="filter-actions"><Button type="submit" icon={ArrowRight} disabled={booting || !!bootError}>Run query</Button><Button kind="ghost" type="button" disabled={booting || !!bootError} on:click={clear}>Clear filters</Button><div class="live-actions">{#if view.phase === 'live' || busy}<Button kind="tertiary" type="button" icon={Stop} on:click={() => controller?.stop()}>Stop reading</Button>{:else}<Button kind="tertiary" type="button" icon={Play} disabled={!liveAvailable || booting || !!bootError} on:click={() => submit(true)}>Start live</Button>{/if}</div></div>
        <p class="filter-footnote">{liveAvailable ? 'Live starts a separate read of available history, then follows new records. It does not continue this snapshot.' : 'Live reading is not configured by this host.'}</p>
      </Form>
    </section>

    <section class="message-language-panel" aria-label="Message rendering language">
      <div class="message-language-controls">
        <Select id="message-language" labelText="Primary message language" size="sm" selected={messageLanguages.primary} disabled={booting || exportRunning} on:change={(event) => changeLanguage((event.target as HTMLSelectElement).value, messageLanguages.fallback)}>
          <SelectItem value="" text="Service default" />{#each languageChoices as language}<SelectItem value={language} text={language} />{/each}
        </Select>
        <Select id="message-fallback-language" labelText="Fallback language" size="sm" selected={messageLanguages.fallback} disabled={!messageLanguages.primary || booting || exportRunning} on:change={(event) => changeLanguage(messageLanguages.primary, (event.target as HTMLSelectElement).value)}>
          <SelectItem value="" text="None" />{#each languageChoices.filter(language => language.toLowerCase().replaceAll('_', '-') !== messageLanguages.primary.toLowerCase().replaceAll('_', '-')) as language}<SelectItem value={language} text={language} />{/each}
        </Select>
      </div>
      <p class="detail-note">Message templates only; recorded free text is unchanged. {messageLanguages.primary ? 'Try the selected order, then the first language in each schema.' : `Service order: ${view.catalog?.defaultLanguages.join(' → ') || 'not available'}; then the first language in each schema.`} {exportRunning ? 'Wait for export preparation or cancel it before changing languages.' : view.languageBusy ? 'Updating the current page…' : 'Updates the current page automatically and also applies to live reads and exports.'}</p>
    </section>
    <div class="catalog-line"><span>{#if view.catalog}{names.length} namespace{names.length === 1 ? '' : 's'} · {view.catalog.schemas.length} schema{view.catalog.schemas.length === 1 ? '' : 's'} available{:else}Discovering available namespaces and schemas…{/if}</span><div class="catalog-actions"><Button kind="tertiary" size="sm" disabled={!transport || booting || !!bootError} on:click={() => { exportOpen = true; }}>Export history</Button><div class="time-display"><Select id="time-display" labelText="Time display" helperText="Boot identifies a journal run; offsets start when that run opens." size="sm" bind:selected={timeDisplay}><SelectItem value="calendar" text="Calendar time (UTC)" /><SelectItem value="boot" text="Boot time (relative)" /></Select></div><Button kind="ghost" size="sm" icon={Renew} disabled={booting || !!bootError} on:click={() => void controller?.discover()}>Refresh catalog</Button></div></div>
    {#if view.catalogError}<InlineNotification kind="warning" title="Catalog unavailable" subtitle={`${view.catalogError}. You can still enter a namespace and run a query.`} hideCloseButton />{/if}
    <div class="read-notices" aria-live="polite">
      {#if view.notice}<InlineNotification kind="info" title="Read status" subtitle={view.notice} hideCloseButton />{/if}
      {#if view.error}<InlineNotification kind={view.phase === 'limit' ? 'warning' : 'error'} title={view.error.title} subtitle={`${view.error.message} (${view.error.code})`} hideCloseButton />{/if}
      {#if view.cleanupWarning}<InlineNotification kind="warning" title="Cursor cleanup is unconfirmed" subtitle={view.cleanupWarning} hideCloseButton />{/if}
      {#if view.truncated}<InlineNotification kind="warning" title="History is truncated" subtitle="This read reached a server limit. The rows below are partial. Narrow the filters and run a new query." hideCloseButton />{/if}
      {#if view.unanchored.length}<InlineNotification kind="info" title="Some records have no UTC anchor" subtitle={`Boots ${view.unanchored.join(', ')} have no UTC anchor and are excluded from UTC windows. Use a boot-relative query to inspect them.`} hideCloseButton />{/if}
      {#if view.damaged.length}<InlineNotification kind="error" title="Damaged history was encountered" subtitle="The read reports gaps. These rows must not be treated as complete history; inspect the diagnostics below." hideCloseButton /><Accordion><AccordionItem title={`Read diagnostics · ${view.damaged.length}`}><ul class="diagnostics">{#each view.damaged as damage}<li><code>Offset {damage.offset}</code><span>{damage.message}</span></li>{/each}</ul></AccordionItem></Accordion>{/if}
    </div>
    {#if view.seeds.length && !view.languageBusy}<section class="seed-panel" aria-label="State before the query"><Accordion><AccordionItem title={`State before the start time · ${view.seeds.length}`}><p class="muted">These samples establish state before the selected window. Their original times are preserved.</p>{#each view.seeds as row (row.id)}<div class="seed-entry"><h3>{row.entry.namespace} · {entrySummary(row.entry)}</h3><EntryDetails entry={row.entry} /></div>{/each}</AccordionItem></Accordion></section>{/if}

    <section class="results" aria-label="Journal records" aria-busy={busy || booting}>
      {#if view.phase === 'counting'}<InlineLoading description="Counting matching records and pages…" />{/if}
      {#if (view.languageBusy || ((booting || view.phase === 'counting' || view.phase === 'loading') && !view.rows.length)) && !bootError}<DataTableSkeleton headers={headers.map(header => header.value)} rows={7} columns={5} showHeader showToolbar={false} />
      {:else if view.rows.length}
        {#key `${view.generation}:${view.mode === 'snapshot' ? view.page : 'live'}`}<DataTable {headers} {rows} title="Records" description={`${view.mode === 'snapshot' ? `Page ${view.page} · ` : ''}${view.rows.length} loaded · ${view.mode === 'live' ? 'Live read · oldest first' : 'Fixed snapshot'} · expand a row to inspect details`} size="short" expandable>
          {#snippet cell({ cell, row })}{#if cell.key === 'level' && cell.value}<Tag size="sm" type={cell.value === 'ERROR' ? 'red' : cell.value === 'WARN' ? 'warm-gray' : 'gray'}>{cell.value}</Tag>{:else if cell.key === 'time'}<span class="table-time">{cell.value}</span>{:else if cell.key === 'namespace'}<code class="namespace-cell">{cell.value}</code>{:else if cell.key === 'message'}<span class="message-cell">{cell.value}</span>{@const badge = entries.get(row.id) ? messageFallbackBadge(entries.get(row.id)!) : ''}{#if badge}<span class="message-language-badge">{badge}</span>{/if}{:else}{cell.value}{/if}{/snippet}
          {#snippet expandedRow({ row })}{@const entry = entries.get(row.id)}{#if entry}<EntryDetails {entry} />{/if}{/snippet}
        </DataTable>{/key}

      {:else if !bootError}<div class="empty-state"><div class="empty-rule"></div><p class="eyebrow">{view.phase === 'stopped' ? 'Read stopped' : view.phase === 'error' || view.phase === 'limit' ? 'Read interrupted' : view.phase === 'live' ? 'Listening' : 'No matching records'}</p><h2>{view.phase === 'stopped' ? 'Reading was stopped.' : view.phase === 'error' || view.phase === 'limit' ? 'This read could not produce a page.' : view.phase === 'live' ? 'Waiting for a matching record.' : view.visitedRecords > 0 || view.canNext ? 'No matching records on this page.' : 'No records match these filters.'}</h2><p>{view.phase === 'stopped' ? 'Run query to open a new snapshot with the current filters and message language.' : view.phase === 'error' || view.phase === 'limit' ? 'Use the message above to resolve the issue, then run a new query.' : view.phase === 'live' ? 'New records will appear here. Stop the read before changing its scope.' : 'Try another namespace or time range, or clear the filters to explore all available history.'}</p>{#if view.phase !== 'error' && view.phase !== 'limit' && view.phase !== 'live' && view.phase !== 'stopped'}<Button kind="tertiary" on:click={clear}>Clear filters and query</Button>{/if}</div>{/if}
      {#if view.mode === 'snapshot' && view.page > 0 && view.knownLastPage !== null && view.totalMatches !== null}
        <!-- Carbon counts actual snapshot pages; byte limits and seeds can shorten a page. -->
        <Pagination size="sm" page={view.page} pageSize={1} pageSizes={[1]} totalItems={view.knownLastPage} pageSizeInputDisabled pageInputType="input" disabled={busy || !view.canSeek} backButtonDisabled={busy || !view.canPrevious} forwardButtonDisabled={busy || !view.canNext} itemRangeText={() => `${view.rows.length} records on this page · ${view.totalMatches} matching records`} on:change={(event) => { if (event.detail.page !== undefined) void controller?.goTo(event.detail.page); }} />
        <div class="results-footer"><span>{view.totalMatches} matching records · {view.knownLastPage} {view.knownLastPage === 1 ? 'page' : 'pages'} in this snapshot{view.totalSeeds !== '0' ? ` · ${view.totalSeeds} state seeds separately` : ''}</span>{#if busy}<InlineLoading description={view.languageBusy ? 'Updating message language…' : `Reading page ${view.navigationProgress} toward page ${view.navigationTarget}…`} />{:else}<span class="footer-status">{status}</span>{/if}</div>
        <p class="cache-note">Cached pages {view.cachedPageNumbers.join(', ') || 'none'} · {view.cachedRecords} records including state seeds · up to 5 pages, {rowLimit} records or 8 MiB. {view.canSeek ? 'Go to any page in the same fixed snapshot; uncached pages are read again with bounded server work.' : 'The snapshot cursor is closed. Only cached pages are available; run a new query for other pages.'}</p>
      {:else if view.mode === 'live' && view.rows.length}<div class="results-footer"><span>{view.rows.length} live records · viewer stops at {rowLimit} records or 8 MiB</span><span>{status}</span></div>{/if}
      {#if view.otherPageWarnings}<p class="cache-note" role="status">Other pages reported damaged or unanchored history. Current-page diagnostics are shown above; this read must not be treated as complete history.</p>{/if}
    </section>
    <footer class="page-footer"><span>Read only · no logging or storage configuration is changed</span><span>API 1 · exact integer values · bounded browser history</span></footer>
  </div>
</Content>

{#if transport}<ExportDialog bind:open={exportOpen} {transport} initialFilters={filters} namespaces={names} {presentation} bind:running={exportRunning} />{/if}
