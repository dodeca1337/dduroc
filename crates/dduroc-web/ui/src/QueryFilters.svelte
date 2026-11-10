<script lang="ts">
  import { Accordion, AccordionItem, Checkbox, Select, SelectItem, TextInput } from 'carbon-components-svelte';
  import NamespaceFilter from './NamespaceFilter.svelte';
  import { activeFilterCount, type Filters } from './lib/filters';
  let { filters = $bindable(), namespaces = [], prefix = 'query', advancedOpen = $bindable(false), includeNamespace = true }: { filters: Filters; namespaces?: string[]; prefix?: string; advancedOpen?: boolean; includeNamespace?: boolean } = $props();
  const active = $derived(activeFilterCount(filters));
</script>
        <div class="filter-grid">
          {#if includeNamespace}<NamespaceFilter bind:filters {namespaces} {prefix} />{/if}
          <Select id={`${prefix}-storage-class`} labelText="Storage class" bind:selected={filters.class}><SelectItem value="" text="All classes" /><SelectItem value="DEFAULT" text="Default" /><SelectItem value="CRITICAL" text="Critical" /><SelectItem value="TELEMETRY" text="Telemetry" /></Select>
          <Select id={`${prefix}-record-kind`} labelText="Record kind" bind:selected={filters.kind}><SelectItem value="all" text="All kinds" /><SelectItem value="messages" text="Messages" /><SelectItem value="text" text="Text" /><SelectItem value="samples" text="Samples" /><SelectItem value="spans" text="Spans" /></Select>
          <Select id={`${prefix}-query-order`} labelText="Snapshot order" bind:selected={filters.order}><SelectItem value="NEWEST" text="Newest first" /><SelectItem value="OLDEST" text="Oldest first" /></Select>
        </div>
        <Accordion class="advanced-filters"><AccordionItem title={`More filters${active ? ` · ${active} selected` : ''}`} bind:open={advancedOpen}>
          <div class="advanced-grid">
            <Select id={`${prefix}-minimum-level`} labelText="Minimum message level" bind:selected={filters.level}><SelectItem value="" text="All levels" />{#each ['DEV_TRACE', 'TRACE', 'DEBUG', 'INFO', 'WARN', 'ERROR'] as level}<SelectItem value={level} text={level.replace('_', ' ')} />{/each}</Select>
            <TextInput id={`${prefix}-event-names`} labelText="Event names" helperText="Exact names, separated by commas" placeholder="Fault, Tick" bind:value={filters.eventNames} />
            <TextInput id={`${prefix}-event-tags`} labelText="Any event tag" helperText="Match any tag, separated by commas" placeholder="network, critical" bind:value={filters.tags} />
            <Select id={`${prefix}-time-clock`} labelText="Time range" bind:selected={filters.clock}><SelectItem value="all" text="Any time" /><SelectItem value="utc" text="UTC timestamp" /><SelectItem value="boot" text="Boot-relative time" /></Select>
            {#if filters.clock === 'boot'}<TextInput id={`${prefix}-boot-number`} labelText="Boot number" helperText="Exact decimal integer" placeholder="1" bind:value={filters.boot} />{/if}
            {#if filters.clock !== 'all'}
              <TextInput id={`${prefix}-from-time`} labelText={filters.clock === 'utc' ? 'From · inclusive' : 'From microseconds · inclusive'} placeholder={filters.clock === 'utc' ? '2026-11-10T12:00:00Z' : '0'} bind:value={filters.from} />
              <TextInput id={`${prefix}-to-time`} labelText={filters.clock === 'utc' ? 'To · inclusive' : 'To microseconds · inclusive'} placeholder={filters.clock === 'utc' ? '2026-11-10T13:00:00Z' : '1000000'} helperText="Leave empty for an open-ended range" bind:value={filters.to} />
            {/if}
          </div>
          <div class="filter-checks"><Checkbox id={`${prefix}-state-seeds`} labelText="Include state before the start time" bind:checked={filters.seedStates} /><Checkbox id={`${prefix}-metadata-only`} labelText="Metadata only · skip payloads and decoded values" bind:checked={filters.metadataOnly} /></div>
        </AccordionItem></Accordion>
