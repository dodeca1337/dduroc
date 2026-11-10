<script lang="ts">
  import ValueTree from './ValueTree.svelte';
  import type { ValueView } from './lib/types';
  import { floatText, scalarText } from './lib/format';
  let { value }: { value: ValueView | null | undefined } = $props();
</script>
{#if !value}
  <span class="muted">Not requested</span>
{:else if value.kind === 'MAP'}
  {#if !value.pairs}<span class="nested-note">More nested fields · inspect the raw payload</span>
  {:else if value.pairs.length === 0}<code>{'{}'}</code>
  {:else}<dl class="value-map">{#each value.pairs as pair, index (index)}<div><dt>{scalarText(pair.key)}</dt><dd><ValueTree value={pair.value} /></dd></div>{/each}</dl>{/if}
{:else if value.kind === 'SEQUENCE' || value.kind === 'VARIANT'}
  {#if value.variant}<span class="value-variant">{value.variant}</span>{/if}
  {#if !value.items}<span class="nested-note">More nested fields · inspect the raw payload</span>
  {:else if value.items.length === 0}<code>[]</code>
  {:else}<ol class="value-sequence">{#each value.items as item, index (index)}<li><ValueTree value={item} /></li>{/each}</ol>{/if}
{:else if value.kind === 'FLOAT' && value.float}
  <span class="exact-value">{floatText(value.float)}</span><span class="value-note"> f{value.float.width} · <code>0x{value.float.bits}</code></span>
{:else if value.kind === 'BYTES' && value.bytes}
  <span>{value.bytes.bytes} bytes</span><code class="raw-value">{value.bytes.base64}</code>
{:else}<span class:exact-value={value.kind === 'SIGNED' || value.kind === 'UNSIGNED'} class:literal-string={value.kind === 'STRING'}>{scalarText(value)}</span>{/if}
