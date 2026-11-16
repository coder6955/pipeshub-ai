/**
 * (b) Search page and (f) duplicates. Searches the page oracle's queries through the UI. Hits
 * must be titled with the original name, carry the original page numbers (the same as the unsplit
 * file on `main`, matched by passage text), come from at least two parts, and never repeat.
 */
import { test, expect } from './large-pdf.fixture';
import { LARGE_NAME, PAGE_ORACLE, SPLIT_KB_NAME, partOfPage } from './split-oracle';
import { SearchPage, type SearchHit } from './pages/search.page';
import { expectNoPartInDom } from './pages/dom-guard';
import { BLOCK_STRIDE } from './leak-scanner';

const BASELINE_KB = 'Large PDF baseline (main)';
const key = (text: string) => text.replace(/\s+/g, ' ').trim().slice(0, 80);

test.describe('large PDF: search page @large-pdf', () => {
  test.describe.configure({ mode: 'serial' });
  const mine: Array<{ query: string; hit: SearchHit }> = [];
  // A passage's opening text can repeat (running heads, boilerplate), so each maps to every page it is on in the unsplit file.
  const baseline = new Map<string, Set<number>>();
  const baselineByQuery = new Map<string, Set<number>>();
  const addTo = (m: Map<string, Set<number>>, k: string, page: number) => m.set(k, (m.get(k) ?? new Set<number>()).add(page));

  test('every oracle query returns original-titled hits on the original pages', async ({ page, stack, shot }) => {
    test.setTimeout(240_000);
    const search = new SearchPage(page);
    await search.open();
    let n = 0;
    for (const query of Object.keys(PAGE_ORACLE)) {
      const { hits } = await search.search(query);
      const fresh = hits.filter((h) => h.recordId === stack.large.id);
      for (const h of hits.filter((x) => x.collection === BASELINE_KB && x.pages[0])) {
        addTo(baseline, key(h.text), h.pages[0]);
        addTo(baselineByQuery, query, h.pages[0]);
      }
      expect(fresh.length, `"${query}" should hit the split file`).toBeGreaterThan(0);
      for (const hit of fresh) {
        mine.push({ query, hit });
        expect(hit.recordName, 'titled with the original name').toBe(LARGE_NAME);
        expect(hit.virtualRecordId).toBe(stack.large.virtualRecordId);
        expect(hit.pages, 'one original page per hit').toHaveLength(1);
        expect(partOfPage(hit.pages[0]), `page ${hit.pages[0]} is inside the original`).not.toBeNull();
        // The paragraph number is a file-wide ordinal, never a block address.
        for (const n of hit.paragraphs) expect(n, 'paragraph number is an ordinal').toBeLessThan(BLOCK_STRIDE);
      }
      // What the user reads on the cards is what the API sent: the original name and pages.
      const allCards = await search.cards();
      const cards = allCards.filter((c) => c.collection === SPLIT_KB_NAME);
      test.info().annotations.push({ type: 'cards', description: JSON.stringify(allCards.map((c) => [c.name, c.collection, c.page])) });
      expect(cards.map((c) => c.name), `cards of the split file read as the original; all cards: ${JSON.stringify(allCards.map((c) => [c.name, c.collection, c.page]))}`).toEqual(fresh.map(() => LARGE_NAME));
      expect(cards.map((c) => c.page).sort(), 'cards show the original pages').toEqual(fresh.map((h) => h.pages[0]).sort());
      if (n++ < 3) await shot(page, `search-${query.replace(/\W+/g, '-').slice(0, 30)}`);
      await expectNoPartInDom(page, `search "${query}"`);
      await search.clear();
    }
    expect(mine.length).toBeGreaterThan(Object.keys(PAGE_ORACLE).length);
  });

  test('the original page numbers equal the unsplit file on main', async () => {
    // Static oracle: hits `main` returned for the unsplit file, matched by passage text.
    const staticPages = new Map<string, Set<number>>();
    for (const hits of Object.values(PAGE_ORACLE)) for (const h of hits) addTo(staticPages, key(h.text), h.page);
    let compared = 0;
    const wrong: string[] = [];
    for (const { query, hit } of mine) {
      const expected = new Set([...(staticPages.get(key(hit.text)) ?? []), ...(baseline.get(key(hit.text)) ?? [])]);
      // A short opening (a running head such as a chapter title) repeats on many pages, so it cannot say which page is right.
      if (expected.size === 0 || key(hit.text).replace(/\W+/g, '').length < 50) continue;
      compared += 1;
      if (!expected.has(hit.pages[0])) wrong.push(`"${query}": "${key(hit.text)}" on page ${hit.pages[0]}, main has it on ${[...expected]}`);
    }
    expect(wrong, 'a passage must be on the page it is on in the unsplit file').toEqual([]);
    expect(compared, 'enough distinctive passages were matched against the oracle for the check to mean something').toBeGreaterThanOrEqual(6);
  });

  test('every query shares at least one page with the oracle, and the queries together reach at least two parts', async () => {
    const byQuery = new Map<string, Set<number>>();
    for (const { query, hit } of mine) byQuery.set(query, (byQuery.get(query) ?? new Set()).add(hit.pages[0]));
    const noOverlap: string[] = [];
    for (const [query, oracle] of Object.entries(PAGE_ORACLE)) {
      const got = byQuery.get(query) ?? new Set<number>();
      // main's saved hits, or what the unsplit baseline collection returns for the same query now
      const live = baselineByQuery.get(query) ?? new Set<number>();
      const same = oracle.filter((o) => got.has(o.page)).length + [...live].filter((p) => got.has(p)).length;
      if (same === 0) noOverlap.push(`"${query}": oracle ${oracle.map((o) => o.page)}, got ${[...got]}`);
    }
    // Semantic search is not byte-stable across index builds: one query may rank other pages of the same chapter.
    expect(noOverlap.length, `queries with no page in common with main: ${noOverlap.join('; ')}`).toBeLessThanOrEqual(2);
    const parts = new Set(mine.map((m) => partOfPage(m.hit.pages[0])));
    expect(parts.size, `hits came from parts ${[...parts]}`).toBeGreaterThanOrEqual(2);
  });

  test('(f) no two hits of a query share the same page and text', async () => {
    for (const query of Object.keys(PAGE_ORACLE)) {
      const seen = new Set<string>();
      for (const { hit } of mine.filter((m) => m.query === query)) {
        const k = `${hit.pages[0]}|${hit.text.replace(/\s+/g, ' ').trim()}`;
        expect(seen.has(k), `"${query}": page ${hit.pages[0]} "${key(hit.text)}" is returned twice`).toBe(false);
        seen.add(k);
      }
    }
    // And across all the queries, one passage keeps one page. A passage is its whole text, not an 80-character opening
    // (two real passages can open alike on different pages), and an opening that the unsplit file has on several pages is skipped.
    const fullKey = (text: string) => text.replace(/\s+/g, ' ').trim();
    const knownPages = (text: string) => new Set([...(baseline.get(key(text)) ?? []), ...Object.values(PAGE_ORACLE).flat().filter((o) => key(o.text) === key(text)).map((o) => o.page)]);
    const pageOf = new Map<string, number>();
    for (const { hit } of mine) {
      if (knownPages(hit.text).size > 1) continue;
      const prior = pageOf.get(fullKey(hit.text));
      if (prior !== undefined) expect(prior, `"${key(hit.text)}"`).toBe(hit.pages[0]);
      pageOf.set(fullKey(hit.text), hit.pages[0]);
    }
  });
});
