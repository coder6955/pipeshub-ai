/**
 * (e) The scanner must catch what it claims to. These controls need no stack: each plants one
 * leak in a body and expects exactly that rule to fire; the clean bodies must pass.
 */
import { describe, expect, test } from 'vitest';
import { ResponseScanner, BLOCK_STRIDE, listingProblems } from './leak-scanner';

const IDENTITY = {
  files: {
    'large-manual': { recordId: 'rec-large', virtualRecordId: 'vrid-large', split: true, pages: 1138 },
    'short-report': { recordId: 'rec-short', virtualRecordId: 'vrid-short', split: false, pages: 12 },
  },
};

function scan(body: unknown, url = 'http://stack/api/v1/search'): ResponseScanner {
  const scanner = new ResponseScanner(IDENTITY);
  scanner.record(url, 200, typeof body === 'string' ? body : JSON.stringify(body));
  return scanner;
}

const hit = (over: Record<string, unknown>) => ({
  searchResults: [{ metadata: { recordName: 'large-manual', recordId: 'rec-large', virtualRecordId: 'vrid-large', pageNum: [871], blockNum: [6709], ...over } }],
});

describe('response scanner controls', () => {
  test('a clean split-file hit passes', () => {
    expect(scan(hit({})).violations).toEqual([]);
  });

  test('a clean unsplit hit passes', () => {
    const body = { metadata: { recordName: 'short-report', recordId: 'rec-short', virtualRecordId: 'vrid-short', pageNum: [3], blockNum: [41] } };
    expect(scan(body).violations).toEqual([]);
  });

  test('a part connector id anywhere fires', () => {
    expect(scan({ connector: 'pdfparts_6aef597746c68226aa8639fb' }).violations.map((v) => v.rule)).toContain('part-connector-id');
    const byUrl = new ResponseScanner(IDENTITY);
    byUrl.scanText('http://stack/api/v1/connectors/pdfparts_abc', 'http://stack/api/v1/connectors/pdfparts_abc', 'url');
    expect(byUrl.violations.map((v) => v.rule)).toContain('part-connector-id');
  });

  test('a known part id fires', () => {
    const s = new ResponseScanner({ ...IDENTITY, partIds: ['7b9c6f7b-6853-5358-84e4-d3b932a5ec4c'] });
    s.record('u', 200, JSON.stringify({ x: 'a 7b9c6f7b-6853-5358-84e4-d3b932a5ec4c b' }));
    expect(s.violations.map((v) => v.rule)).toContain('part-id');
  });

  test('a part table or a "Part k of N" header in a stream fires, ordinary text does not', () => {
    const stream = (text: string) => `event: tool\ndata: ${JSON.stringify({ result: text })}\n\n`;
    for (const text of ['This document has 3 parts.', 'Part 2: pages 500-998', 'Part 2 of 3']) {
      expect(scan(stream(text), 'http://stack/api/v1/conversations/stream').violations.map((v) => v.rule), text).toContain('part-table');
    }
    expect(scan(stream('Pages 500-998 of 1138 of large-manual'), 'http://stack/api/v1/conversations/stream').violations).toEqual([]);
  });

  test('a part label fires', () => {
    expect(scan({ name: 'large-manual · pages 1-500' }).violations.map((v) => v.rule)).toContain('part-label');
  });

  test('split internals fire', () => {
    expect(scan({ record: { splitPlan: '[]' } }).violations.map((v) => v.rule)).toContain('split-internals');
    expect(scan({ pageOffset: 500 }).violations.map((v) => v.rule)).toContain('split-internals');
  });

  test('a record or virtual record id that is not the file’s fires', () => {
    const hiddenOnly = new ResponseScanner({ ...IDENTITY, visibleRecordIds: ['rec-large', 'rec-short', 'rec-other'], visibleVirtualRecordIds: ['vrid-large', 'vrid-short'] });
    hiddenOnly.record('u', 200, JSON.stringify(hit({ recordId: 'rec-part-2' })));
    expect(hiddenOnly.violations.map((v) => v.rule)).toContain('hidden-record-id');
    // The same name in another collection is another visible file, not a leak.
    const other = new ResponseScanner({ ...IDENTITY, visibleRecordIds: ['rec-large', 'rec-short', 'rec-other'], visibleVirtualRecordIds: ['vrid-large', 'vrid-short', 'vrid-other'] });
    other.record('u', 200, JSON.stringify(hit({ recordId: 'rec-other', virtualRecordId: 'vrid-other', blockNum: [8580] })));
    expect(other.violations).toEqual([]);
    expect(scan(hit({ virtualRecordId: 'vrid-part-2' })).violations.map((v) => v.rule)).toContain('foreign-virtual-record-id');
  });

  test('a block address shown as a paragraph number, or a page outside the file, fires', () => {
    expect(scan(hit({ blockNum: [9 * BLOCK_STRIDE + 1] })).violations.map((v) => v.rule)).toContain('block-address-displayed');
    expect(scan(hit({ blockNum: [2 * BLOCK_STRIDE + 5] })).violations.map((v) => v.rule)).toContain('block-address-displayed');
    expect(scan(hit({ pageNum: [1139] })).violations.map((v) => v.rule)).toContain('page-out-of-file');
    const short = { metadata: { recordName: 'short-report', recordId: 'rec-short', pageNum: [3], blockNum: [2 * BLOCK_STRIDE + 1] } };
    expect(scan(short).violations.map((v) => v.rule)).toContain('block-address-displayed');
  });

  test('a link to the split file’s own manifest blocks passes, a link into a part it lacks fires', () => {
    const link = (n: number) => `[1](http://x/record/9a0f6a1e-1111-4222-8333-444455556666/preview#blockIndex=${n})`;
    const identity = { files: { 'large-manual': { recordId: '9a0f6a1e-1111-4222-8333-444455556666', split: true, pages: 1138 } } };
    const ok = new ResponseScanner(identity);
    ok.record('u', 200, JSON.stringify({ text: link(3) + link(2 * BLOCK_STRIDE + 7) }));
    expect(ok.violations).toEqual([]);
    const bad = new ResponseScanner(identity);
    bad.record('u', 200, JSON.stringify({ text: link(7 * BLOCK_STRIDE + 7) }));
    expect(bad.violations.map((v) => v.rule)).toContain('block-resolves-elsewhere');
  });

  test('an SSE stream is read line by line', () => {
    const stream = `event: complete\ndata: ${JSON.stringify({ citations: [hit({ blockNum: [8 * BLOCK_STRIDE] }).searchResults[0]] })}\n\n`;
    expect(scan(stream, 'http://stack/api/v1/conversations/stream').violations.map((v) => v.rule)).toContain('block-address-displayed');
  });

  test('a page-only object (no block numbers) outside the file fires', () => {
    const body = { metadata: { recordName: 'large-manual', recordId: 'rec-large', pageNum: [1139] } };
    expect(scan(body).violations.map((v) => v.rule)).toContain('page-out-of-file');
    expect(scan({ metadata: { recordName: 'large-manual', recordId: 'rec-large', pageNum: [1138] } }).violations).toEqual([]);
    expect(scan({ metadata: { recordName: 'short-report', recordId: 'rec-short', pageNum: [13] } }).violations.map((v) => v.rule)).toContain('page-out-of-file');
  });

  test.each(['segmentIndex', 'splitGen', 'blockAddress', 'segmentVirtualRecordId', 'contentVrid', 'identity_vrid', 'segment_index'])(
    'the backend internal key %s fires with no part id in the body',
    (key) => {
      expect(scan({ metadata: { [key]: 3 } }).violations.map((v) => v.rule)).toContain('split-internals');
    },
  );

  test('an unreadable body is reported unless it is an explained navigation discard', () => {
    const s = new ResponseScanner(IDENTITY);
    s.unscanned.push({ url: 'a', reason: 'Error: Protocol error (Network.getResponseBody): No resource with given identifier found' });
    s.unscanned.push({ url: 'b', reason: 'Error: something else broke' });
    expect(s.unexpectedUnscanned().map((u) => u.url)).toEqual(['b']);
  });
});

describe('listing that seeds the allowlist', () => {
  const base = { collection: 'Split', rowNames: ['large-manual'], rowIds: ['rec-large'], partIds: ['part-1'], ownCollections: ['Split'], ownFiles: ['large-manual'], journeyUploads: ['progress-501'] };

  test('the journey’s own files pass, with its own upload', () => {
    expect(listingProblems(base)).toEqual([]);
    expect(listingProblems({ ...base, rowNames: ['large-manual', 'progress-501'], rowIds: ['rec-large', 'x'] })).toEqual([]);
  });

  test('a listed part cannot whitelist itself', () => {
    expect(listingProblems({ ...base, rowIds: ['rec-large', 'part-1'] })).toEqual(['row id part-1 is a known part']);
    expect(listingProblems({ ...base, rowNames: ['large-manual · pages 1-500'] })).not.toEqual([]);
    expect(listingProblems({ ...base, rowNames: ['large-manual', 'large-manual (1)'] })).toEqual(['unexpected row "large-manual (1)"']);
    expect(listingProblems({ ...base, rowNames: ['large-manual', 'large-manual'] })).toEqual(['a file is listed twice']);
  });

  test('another collection may hold other files, but never parts', () => {
    expect(listingProblems({ ...base, collection: 'Other', rowNames: ['anything'] })).toEqual([]);
    expect(listingProblems({ ...base, collection: 'Other', rowNames: ['pdfparts_abc'] })).not.toEqual([]);
  });
});
