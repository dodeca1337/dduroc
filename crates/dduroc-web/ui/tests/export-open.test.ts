import { expect, it, vi } from 'vitest';
import { ApiError } from '../src/lib/api';
import { ExportCleanupError, exportHistory, type ExportOptions } from '../src/lib/export';
import type { Transport } from '../src/lib/types';
const options: ExportOptions = { query: {}, metadataOnly: false, format: 'jsonl', maxRecords: 1, maxBytes: 8192 };
function api(error: unknown): Transport {
  return { catalog: vi.fn(), open: vi.fn().mockRejectedValue(error), page: vi.fn(), close: vi.fn(), cancel: vi.fn(), follow: vi.fn() };
}
it('distinguishes uncertain open delivery from a confirmed server refusal', async () => {
  for (const error of [new TypeError('network'), new DOMException('timeout', 'TimeoutError'), ...['NETWORK', 'PROTOCOL', 'VIEWER_LIMIT'].map(code => new ApiError(code, 'failed'))]) {
    await expect(exportHistory(api(error), options, new AbortController().signal)).rejects.toBeInstanceOf(ExportCleanupError);
  }
  const refusal = new ApiError('CAPACITY', 'busy');
  await expect(exportHistory(api(refusal), options, new AbortController().signal)).rejects.toBe(refusal);
});
