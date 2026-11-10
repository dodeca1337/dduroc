import { afterEach, expect, it, vi } from 'vitest';
import { HttpTransport } from '../src/lib/api';

afterEach(() => vi.unstubAllGlobals());

it('opens a counted fixed snapshot with the same byte and record page shape as seek while keeping legacy export opens separate', async () => {
  const calls: { query: string; variables: Record<string, unknown> }[] = [];
  vi.stubGlobal('fetch', vi.fn(async (_url, init) => {
    const request = JSON.parse(init.body); calls.push(request);
    const operation = request.query.includes('openCountedReadSession') ? 'openCountedReadSession' : request.query.includes('seekReadSession') ? 'seekReadSession' : 'openReadSession';
    const value = operation === 'openCountedReadSession' ? { id: 'fixed', revision: '0', totalMatches: '9007199254740993', totalSeeds: '2', totalPages: '14' } : operation === 'openReadSession' ? { id: 'export', revision: '0' } : { revision: '1', reachedPage: '1', targetReached: true, exhausted: false, page: { entries: [], seeds: [], damaged: [], unanchored: [], exhausted: false, truncated: false } };
    return new Response(JSON.stringify({ data: { [operation]: value } }), { headers: { 'Content-Type': 'application/json' } });
  }));
  const api = new HttpTransport({ graphqlEndpoint: 'https://example.test/graphql', title: 'test', maxRows: 1000 });
  const query = { namespaces: ['selected'], order: 'NEWEST' as const };
  const session = await api.countedOpen(query, 64, false, { languages: ['en'] });
  expect(session.totalMatches).toBe('9007199254740993');
  await api.seek(session, 1, 64, false, new AbortController().signal, { languages: ['ru'] });
  await api.open(query, false, { languages: ['en'] });
  expect(calls[0].query).toContain('openCountedReadSession');
  expect(calls[0].query).toContain('totalMatches totalSeeds totalPages');
  expect(calls[0].variables).toEqual({ query, presentation: { languages: ['en'] }, records: '64' });
  expect(calls[1].variables.records).toBe('64');
  expect(calls.slice(0, 2).every(call => call.query.includes('maxBytes:"1048576"'))).toBe(true);
  expect(calls[2].query).toContain('openReadSession(');
  expect(calls[2].query).not.toContain('totalMatches');
});
