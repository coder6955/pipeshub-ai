/**
 * Fixtures for the large-PDF journey: the prepared stack's records (looked up by name through the
 * API, read-only), the response scanner attached to every page, and step screenshots.
 *
 * Every test in the journey gets the scanner for free: it fails at teardown if any API
 * response the browser saw leaked a part id, so (e) holds for (a) to (d) without each spec asking.
 */
import * as fs from 'fs';
import * as path from 'path';
import { test as apiTest, expect } from '../../fixtures/api-context.fixture';
import type { APIRequestContext, Page } from '@playwright/test';
import { ResponseScanner, listingProblems, type ScanIdentity } from './leak-scanner';
import { LARGE_NAME, LARGE_PAGE_COUNT, SHORT_NAME, SHORT_PAGE_COUNT, SPLIT_KB_NAME, UNSPLIT_KB_NAME } from './split-oracle';

export interface SplitRoll {
  splitState?: string | null;
  splitDone?: number | null;
  splitTotal?: number | null;
  splitFailed?: number | null;
}

export interface Stack {
  /** The collection with the split file, and the one with the unsplit file. */
  kbId: string;
  shortKbId: string;
  large: { id: string; virtualRecordId: string };
  short: { id: string; virtualRecordId: string };
  identity: ScanIdentity;
}

interface KbRow {
  id: string;
  name: string;
}

async function getJson<T>(api: APIRequestContext, url: string): Promise<T> {
  const response = await api.get(url);
  if (!response.ok()) throw new Error(`GET ${url} failed [${response.status()}]: ${(await response.text()).slice(0, 300)}`);
  return (await response.json()) as T;
}

/** The split roll-up of the large record, from the record API the page itself uses. */
export async function readRoll(api: APIRequestContext, recordId: string): Promise<SplitRoll & { indexingStatus?: string }> {
  const body = await getJson<{ record: SplitRoll & { indexingStatus?: string } }>(api, `/api/v1/knowledgeBase/record/${recordId}`);
  return body.record;
}

/** Part record keys and content vrids from the owner's store (E2E_PART_IDS_FILE, a JSON array of strings); none when unset. */
function readPartIds(): string[] {
  const file = process.env.E2E_PART_IDS_FILE;
  return file && fs.existsSync(file) ? (JSON.parse(fs.readFileSync(file, 'utf-8')) as string[]) : [];
}

export async function resolveStack(api: APIRequestContext): Promise<Stack> {
  const kbs = await getJson<{ knowledgeBases: KbRow[] }>(api, '/api/v1/knowledgeBase/?limit=100');
  const kbIdOf = (name: string) => {
    const matches = kbs.knowledgeBases.filter((k) => k.name === name);
    expect(matches, `exactly one collection named "${name}"`).toHaveLength(1);
    return matches[0].id;
  };
  const kbId = kbIdOf(SPLIT_KB_NAME);
  const shortKbId = kbIdOf(UNSPLIT_KB_NAME);
  const rowsOf = async (id: string) => {
    const listing = await getJson<{ items?: Array<Record<string, unknown>>; nodes?: Array<Record<string, unknown>> }>(api, `/api/v1/knowledgeBase/knowledge-hub/nodes/app/${id}?limit=200`);
    return listing.items ?? listing.nodes ?? [];
  };
  const largeRow = (await rowsOf(kbId)).find((r) => r.name === LARGE_NAME) as { id: string } | undefined;
  const shortRow = (await rowsOf(shortKbId)).find((r) => r.name === SHORT_NAME) as { id: string } | undefined;
  expect(largeRow, `${LARGE_NAME} in "${SPLIT_KB_NAME}"`).toBeTruthy();
  expect(shortRow, `${SHORT_NAME} in "${UNSPLIT_KB_NAME}"`).toBeTruthy();
  const vrid = async (id: string) => (await getJson<{ record: { virtualRecordId: string } }>(api, `/api/v1/knowledgeBase/record/${id}`)).record.virtualRecordId;
  // Every id a user can see, in every collection, so an id outside them is a hidden (part) record.
  const visibleRecordIds: string[] = [];
  const visibleVirtualRecordIds: string[] = [];
  const partIds = readPartIds();
  for (const kb of kbs.knowledgeBases) {
    const kbList = await getJson<{ items?: Array<{ id: string; name?: string }>; nodes?: Array<{ id: string; name?: string }> }>(api, `/api/v1/knowledgeBase/knowledge-hub/nodes/app/${kb.id}?limit=200`);
    // The allowlist is built from these listings, so a listed part would whitelist itself: check the rows first.
    const names = (kbList.items ?? kbList.nodes ?? []).map((r) => r.name ?? '');
    const problems = listingProblems({
      collection: kb.name,
      rowNames: names,
      rowIds: (kbList.items ?? kbList.nodes ?? []).map((r) => r.id),
      partIds,
      ownCollections: [SPLIT_KB_NAME, UNSPLIT_KB_NAME],
      ownFiles: [LARGE_NAME, SHORT_NAME],
      journeyUploads: ['progress-501'],
    });
    expect(problems, `the listing of "${kb.name}" cannot seed the allowlist`).toEqual([]);
    for (const row of kbList.items ?? kbList.nodes ?? []) {
      visibleRecordIds.push(row.id);
      const detail = await api.get(`/api/v1/knowledgeBase/record/${row.id}`);
      if (detail.ok()) {
        const v = ((await detail.json()) as { record?: { virtualRecordId?: string } }).record?.virtualRecordId;
        if (v) visibleVirtualRecordIds.push(v);
      }
    }
  }
  const large = { id: largeRow!.id, virtualRecordId: await vrid(largeRow!.id) };
  const short = { id: shortRow!.id, virtualRecordId: await vrid(shortRow!.id) };
  return {
    kbId,
    shortKbId,
    large,
    short,
    identity: {
      partIds,
      collectionIds: kbs.knowledgeBases.map((k) => k.id),
      visibleRecordIds,
      visibleVirtualRecordIds,
      files: {
        [LARGE_NAME]: { recordId: large.id, virtualRecordId: large.virtualRecordId, split: true, pages: LARGE_PAGE_COUNT },
        [SHORT_NAME]: { recordId: short.id, virtualRecordId: short.virtualRecordId, split: false, pages: SHORT_PAGE_COUNT },
      },
    },
  };
}

type Fixtures = {
  stack: Stack;
  scanner: ResponseScanner;
  shot: (page: Page, name: string) => Promise<void>;
};

const EVIDENCE = process.env.E2E_EVIDENCE_DIR ?? path.resolve(__dirname, '../../../../test-results/large-pdf-steps');

export const test = apiTest.extend<Fixtures>({
  stack: async ({ apiContext }, use) => {
    await use(await resolveStack(apiContext));
  },

  scanner: [
    async ({ context, stack }, use, testInfo) => {
      const scanner = new ResponseScanner(stack.identity);
      scanner.attach(context);
      await use(scanner);
      await scanner.settle();
      const report = { test: testInfo.title, summary: scanner.summary(), violations: scanner.violations, unscanned: scanner.unscanned, scanned: scanner.scanned };
      const dir = path.join(EVIDENCE, '..', 'scanner');
      fs.mkdirSync(dir, { recursive: true });
      fs.writeFileSync(path.join(dir, `${testInfo.titlePath.join('-').replace(/[^a-z0-9]+/gi, '_').slice(0, 120)}.json`), JSON.stringify(report, null, 1));
      scanner.streams.forEach((st, i) => fs.writeFileSync(path.join(dir, `${testInfo.titlePath.join('-').replace(/[^a-z0-9]+/gi, '_').slice(0, 100)}.stream${i}.sse`), st.body));
      await testInfo.attach('scanner.json', { body: JSON.stringify(report, null, 1), contentType: 'application/json' });
      expect(scanner.unexpectedUnscanned(), 'every API response body was read and scanned').toEqual([]);
      expect(scanner.violations, `part ids or split internals leaked: ${scanner.summary()}`).toEqual([]);
    },
    { auto: true },
  ],

  shot: async ({}, use, testInfo) => {
    let n = 0;
    const slug = (t: string | undefined, max: number) => (t ?? 'x').replace(/@large-pdf/g, '').replace(/[^a-z0-9]+/gi, '_').replace(/^_|_$/g, '').slice(0, max);
    const dir = path.join(EVIDENCE, `${path.basename(testInfo.file, '.spec.ts')}`, slug(testInfo.title, 60));
    await use(async (page, name) => {
      fs.mkdirSync(dir, { recursive: true });
      n += 1;
      await page.screenshot({ path: path.join(dir, `${String(n).padStart(2, '0')}-${name}.png`) });
    });
  },
});

export { expect };
