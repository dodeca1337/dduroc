export type Decimal = string;
export type Class = 'DEFAULT' | 'CRITICAL' | 'TELEMETRY';
export type Level = 'DEV_TRACE' | 'TRACE' | 'DEBUG' | 'INFO' | 'WARN' | 'ERROR';
export interface BytesValue { base64: string; bytes: Decimal }
export interface FloatValue { width: 32 | 64; class: 'FINITE' | 'NAN' | 'POSITIVE_INFINITY' | 'NEGATIVE_INFINITY'; decimal: string; bits: string }
export interface ValueView {
  kind: 'NULL' | 'BOOLEAN' | 'SIGNED' | 'UNSIGNED' | 'FLOAT' | 'STRING' | 'BYTES' | 'SEQUENCE' | 'MAP' | 'VARIANT';
  text?: string | null; boolean?: boolean | null; float?: FloatValue | null; bytes?: BytesValue | null; variant?: string | null;
  // Missing children mean the bounded GraphQL projection stopped here, not an empty value.
  items?: ValueView[]; pairs?: { key: ValueView; value: ValueView }[];
}
export interface Entry {
  namespace: string; class: Class; at: { boot: Decimal; micros: Decimal }; utc: string | null; span: Decimal | null;
  kind: 'MESSAGE' | 'TEXT' | 'SPAN_START' | 'SPAN_END' | 'SAMPLE' | 'EXTENSION';
  message?: { eventId: Decimal; name: string | null; level: Level | null; tags: string[]; rendered: string | null; renderStatus: string; language: string | null; languagePreferenceIndex?: number | null; payload: BytesValue | null; fieldsStatus: string; fields: ValueView | null } | null;
  text?: { level: Level; target: string; text: string } | null;
  sample?: { metricId: Decimal; name: string | null; unit: string | null; tags: string[]; kind: string | null; stateName: string | null; severity: string | null; value: ValueView | null } | null;
  spanEvent?: { id: Decimal; kindName: string | null; parent: Decimal | null } | null;
  extension?: BytesValue | null;
}
export interface StoredRow { id: string; entry: Entry }
export interface Damage { offset: Decimal; message: string }
export interface Page { entries: Entry[]; seeds: Entry[]; damaged: Damage[]; unanchored: Decimal[]; exhausted: boolean; truncated: boolean }
export interface Batch { entries: Entry[]; seeds: Entry[]; damaged: Damage[]; unanchored: Decimal[]; ended: boolean }
export interface Session { id: string; revision: Decimal }
export interface CountedSession extends Session { totalMatches: Decimal; totalSeeds: Decimal; totalPages: Decimal }
export interface SessionPage { revision: Decimal; page: Page }
export interface SeekPage { revision: Decimal; reachedPage: Decimal; targetReached: boolean; exhausted: boolean; page: Page | null }
export interface Catalog {
  apiVersion: string; defaultLanguages: string[];
  namespaces: { namespace: string; class: Class; schemaName: string; protocolVersion: Decimal; retainedBytes: Decimal }[];
  schemas: { name: string; version: Decimal; languages: string[]; events: { id: Decimal; name: string; tags: string[]; fields: { name: string; typeName: string }[] }[]; metrics: { id: Decimal; name: string; valueType: string; unit: string; kind: string }[] }[];
}
export interface StorageStatistics {
  capturedAt?: string; countingState: string; retainedRecords: Decimal | null; retainedBlocks: Decimal | null; logicalRetainedBytes: Decimal; channels: Decimal;
  roots: { index: Decimal; allocatedBytes: Decimal; logBytes: Decimal; rewriteBytes: Decimal; metadataBytes: Decimal; physicalLimitBytes: Decimal | null }[] | null;
  runtime: { recordsWritten: Decimal; blocksWritten: Decimal; bytesWritten: Decimal; dropped: Decimal; rejected: Decimal; ioErrors: Decimal } | null;
}
export interface PresentationInput { languages: string[] }
export interface QueryInput { namespaces?: string[]; namespacePrefix?: string; classes?: Class[]; boot?: Decimal; from?: { utc: string } | { boot: Decimal; micros: Decimal }; to?: { utc: string } | { boot: Decimal; micros: Decimal }; order?: 'OLDEST' | 'NEWEST'; seedStates?: boolean; filter?: { minLevel?: Level; eventNames?: string[]; anyTags?: string[]; kinds?: { messages: boolean; text: boolean; samples: boolean; spans: boolean } } }
export interface Bootstrap { graphqlEndpoint: string; subscriptionEndpoint?: string; title: string; maxRows: number }
export interface ViewError { code: string; title: string; message: string }
export interface Transport {
  catalog(signal?: AbortSignal): Promise<Catalog>;
  statistics?(signal?: AbortSignal): Promise<StorageStatistics>;
  backfillStatistics?(signal?: AbortSignal): Promise<{ more: boolean }>;
  open(query: QueryInput, metadataOnly: boolean, presentation?: PresentationInput): Promise<Session>;
  countedOpen?(query: QueryInput, records: number, metadataOnly: boolean, presentation?: PresentationInput): Promise<CountedSession>;
  page(session: Session, records: number, metadataOnly: boolean, signal: AbortSignal): Promise<SessionPage>;
  seek?(session: Session, targetPage: number, records: number, metadataOnly: boolean, signal: AbortSignal, presentation?: PresentationInput): Promise<SeekPage>;
  cancel(id: string): Promise<void>;
  close(session: Session): Promise<void>;
  follow(query: QueryInput, metadataOnly: boolean, callbacks: { next(batch: Batch): void; error(error: unknown): void; complete(): void }, presentation?: PresentationInput): () => void;
}
