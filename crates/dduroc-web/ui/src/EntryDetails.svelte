<script lang="ts">
  import { Button, Tag, TextArea } from 'carbon-components-svelte';
  import { Copy } from 'carbon-icons-svelte';
  import type { Entry } from './lib/types';
  import { bootTime, calendarTime } from './lib/format';
  import { messageLanguageStatus } from './lib/languages';
  import ValueTree from './ValueTree.svelte';
  let { entry }: { entry: Entry } = $props();
  let copied = $state('');
  const payload = $derived(entry.message?.payload ?? entry.extension);
  async function copy() { try { await navigator.clipboard.writeText(payload?.base64 ?? ''); copied = 'Base64 copied'; } catch { copied = 'Clipboard unavailable. Select and copy the payload below.'; } }
</script>
<section class="entry-details" aria-label="Record details">
  <div class="detail-metadata">
    <div><span class="detail-label">Relative time</span><code>{bootTime(entry)}</code></div>
    <div><span class="detail-label">UTC</span><span>{entry.utc ? calendarTime(entry.utc) : 'UTC unavailable · no time anchor recorded'}</span></div>
    <div><span class="detail-label">Storage class</span><span>{entry.class.toLowerCase()}</span></div>
    {#if entry.span}<div><span class="detail-label">Span</span><code>{entry.span}</code></div>{/if}
    {#if entry.spanEvent?.parent}<div><span class="detail-label">Parent span</span><code>{entry.spanEvent.parent}</code></div>{/if}
    {#if entry.message}<div><span class="detail-label">Event ID</span><code>{entry.message.eventId}</code></div><div><span class="detail-label">Rendered message language</span><span>{messageLanguageStatus(entry)}</span></div>{/if}
    {#if entry.sample}<div><span class="detail-label">Metric</span><span>{entry.sample.name ?? 'Unknown metric'} · <code>{entry.sample.metricId}</code></span></div>{/if}
  </div>
  {#if entry.message}
    <div class="detail-heading"><h3>Fields</h3>{#each entry.message.tags as tag (tag)}<Tag size="sm" type="gray">{tag}</Tag>{/each}</div>
    {#if entry.message.fieldsStatus === 'UNKNOWN_SCHEMA'}<p class="muted">This build does not know the event schema. The event ID and raw payload are preserved.</p>
    {:else if entry.message.fieldsStatus === 'UNSUPPORTED_DECODER'}<p class="muted">This schema has no bounded field decoder. Inspect the raw payload.</p>
    {:else if !entry.message.fields}<p class="muted">Details were not requested. Turn off Metadata only and run a new query to read them.</p>
    {:else}<ValueTree value={entry.message.fields} />{/if}
  {:else if entry.sample}
    <h3>Value</h3><ValueTree value={entry.sample.value} />
    <p class="detail-note">{entry.sample.kind ?? 'Unknown'} series{entry.sample.unit ? ` · ${entry.sample.unit}` : ''}{entry.sample.stateName ? ` · ${entry.sample.stateName}` : ''}. Severity uses schema defaults.</p>
  {:else if entry.text}<h3>Text</h3><p class="detail-note">Target: <code>{entry.text.target}</code></p><pre>{entry.text.text || 'Text was not requested.'}</pre>
  {:else if entry.spanEvent}<h3>{entry.kind === 'SPAN_START' ? 'Span started' : 'Span ended'}</h3><p>{entry.spanEvent.kindName ?? 'Kind is recorded on the start event.'}</p>{/if}
  {#if payload}
    <div class="detail-heading raw-heading"><h3>Raw payload</h3><span class="muted">Base64 · {payload.bytes} bytes</span><Button kind="ghost" size="sm" icon={Copy} on:click={copy}>Copy payload</Button></div>
    <TextArea labelText="Raw payload in base64" readonly rows={4} value={payload.base64} />
    {#if copied}<p class="detail-note" role="status">{copied}</p>{/if}
  {/if}
</section>
