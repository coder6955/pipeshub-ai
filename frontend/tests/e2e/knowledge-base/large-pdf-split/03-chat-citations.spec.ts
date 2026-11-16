/**
 * (c) Chat with citations. Questions whose answers need passages from different parts of the
 * 1,138-page file. The answer must cite the file at least twice, from at least two parts;
 * clicking a citation must open the ORIGINAL PDF in the viewer, on the cited page, with all
 * 1,138 pages and the cited passage on screen.
 */
import { test, expect } from './large-pdf.fixture';
import { LARGE_NAME, LARGE_PAGE_COUNT, CROSS_PART_QUESTIONS, partOfPage, SPLIT_KB_NAME } from './split-oracle';
import { ChatPage } from './pages/chat.page';
import { PdfViewer, snippetOverlap } from './pages/viewer';
import { expectLinksToVisibleRecords, expectNoPartInDom } from './pages/dom-guard';

test.describe('large PDF: chat with citations @large-pdf', () => {
  test.describe.configure({ mode: 'serial' });

  for (const [n, q] of CROSS_PART_QUESTIONS.entries()) {
    test(`question ${n + 1} cites the original from at least two parts and opens it at the cited page`, async ({ page, stack, scanner, shot }) => {
      test.setTimeout(900_000);
      const chat = new ChatPage(page);
      const viewer = new PdfViewer(page);

      const { stream } = await chat.askInCollection(stack.kbId, q.question);
      expect(stream, 'the stream finished the run').toContain('RUN_FINISHED');
      expect(await chat.waitForCitations(2), 'the answer should carry at least two citations').toBeGreaterThanOrEqual(2);
      await shot(page, 'answer');

      // The answer's own links go to the original file, which the user can see.
      await expectLinksToVisibleRecords(page, stack.identity.visibleRecordIds ?? [], 'answer');
      const chipHrefs = await chat.inlineChips().evaluateAll((els) => els.map((e) => e.getAttribute('href')));
      expect(chipHrefs.length, 'the answer should show inline citation chips').toBeGreaterThanOrEqual(2);
      for (const href of chipHrefs) expect(href).toBe(`/record/${stack.large.id}`);

      await chat.openCitationTab();
      const cards = await chat.cards();
      await shot(page, 'citation-tab');
      const large = cards.filter((c) => c.name === LARGE_NAME);
      expect(large.length, `the answer cites ${LARGE_NAME} at least twice`).toBeGreaterThanOrEqual(2);
      for (const c of cards) {
        expect(c.collection, 'retrieval stayed in the collection that was asked').toBe(SPLIT_KB_NAME);
        expect(c.page, `citation ${c.index} has a page`).not.toBeNull();
      }
      const parts = new Set(large.map((c) => partOfPage(c.page!)));
      expect(parts.size, `cited pages ${large.map((c) => c.page)} fall in parts ${[...parts]}; ${q.needs}`).toBeGreaterThanOrEqual(2);
      // A citation shows its own paragraph number, not a part's.
      test.info().annotations.push({ type: 'observation', description: `paragraph labels: ${large.map((c) => c.paragraph).join(', ')}` });

      // Open one citation from each part cited, and read the viewer.
      const opened = new Set<number>();
      for (const card of large) {
        const part = partOfPage(card.page!)!;
        if (opened.has(part)) continue;
        opened.add(part);
        await chat.previewCard(card.index);
        await viewer.waitForPage(card.page!);
        const pos = await viewer.position();
        expect(pos.total, `citation ${card.index}: the viewer holds the original PDF, not a part`).toBe(LARGE_PAGE_COUNT);
        expect(await viewer.title()).toBe(LARGE_NAME);
        expect(await viewer.pdfPages()).toBe(LARGE_PAGE_COUNT);
        const text = await viewer.pageText(card.page!);
        expect(snippetOverlap(card.snippet, text), `the cited passage is on page ${card.page} of the viewer`).toBeGreaterThanOrEqual(0.6);
        await shot(page, `viewer-citation-${card.index}-page-${card.page}`);
        await expectNoPartInDom(page, `viewer for citation ${card.index}`);
      }
      expect(opened.size, 'opened citations from at least two parts').toBeGreaterThanOrEqual(2);

      // Every PDF the viewer fetched is the original file (its size, its record id), never a part.
      await scanner.settle();
      expect(scanner.documents.length, 'the viewer fetched the PDF').toBeGreaterThan(0);
      for (const d of scanner.documents.filter((x) => /pdf/i.test(x.contentType))) {
        expect(d.url, 'the PDF is the original record').toContain(stack.large.id);
        expect(d.bytes, 'the PDF is the 5.5 MB original, not a part of a few hundred pages').toBeGreaterThan(5_000_000);
      }
    });
  }

  test('an inline citation chip opens the original at its citation page', async ({ page, stack, shot }) => {
    test.setTimeout(600_000);
    const chat = new ChatPage(page);
    const viewer = new PdfViewer(page);
    await chat.askInCollection(stack.kbId, CROSS_PART_QUESTIONS[0].question);
    expect(await chat.waitForCitations(2), 'the answer should carry at least two citations').toBeGreaterThanOrEqual(2);
    await chat.openCitationTab();
    const cards = await chat.cards();
    await chat.openAnswerTab();
    // The first chip carries citation 1; clicking it opens a card with the page, and its Preview opens the viewer.
    await chat.inlineChips().first().click();
    const popupPage = Number((await page.getByText(/^Page \d+$/).first().innerText()).replace(/\D+/g, ''));
    expect(popupPage, 'the chip card shows the same page as citation 1 of the Citation tab').toBe(cards[0].page);
    await shot(page, 'chip-card');
    await page.getByRole('button', { name: 'Preview' }).first().click();
    await viewer.waitForPage(popupPage);
    const pos = await viewer.position();
    expect(pos.total, 'the chip opens the original PDF').toBe(LARGE_PAGE_COUNT);
    expect(await viewer.title()).toBe(LARGE_NAME);
    await shot(page, `viewer-chip-page-${popupPage}`);
  });

  test('the tool activity the chat shows does not spell out the file’s parts', async ({ page, stack, scanner, shot }) => {
    test.setTimeout(600_000);
    const chat = new ChatPage(page);
    const { stream } = await chat.askInCollection(stack.kbId, CROSS_PART_QUESTIONS[0].question);
    expect(await chat.waitForCitations(2)).toBeGreaterThanOrEqual(2);
    // Open every "Used N tools" group and every tool call under it, as a curious user would.
    const groups = page.getByText(/^Used \d+ tools?$/);
    expect(await groups.count(), 'the answer shows a "Used N tools" group').toBeGreaterThan(0);
    for (const group of await groups.all()) await group.click({ timeout: 5_000 });
    const toggles = await page.getByText(/^Fetching additional data$|^Searched/).all();
    expect(toggles.length, 'the groups hold tool calls to expand').toBeGreaterThan(0);
    // A toggle that cannot be opened fails the test: its content would go unchecked.
    for (const toggle of toggles) await toggle.click({ timeout: 5_000 });
    await shot(page, 'tools-expanded');
    const visible = await page.locator('body').innerText();
    const PART_TABLE = /This document has \d+ parts?|Part \d+:\s*pages\s*\d+|\bPart \d+ of \d+\b/i;
    // D-41: the model never sees parts either, so the stream (tool results included) holds no part table and no "Part k of N" header.
    expect(stream, 'no part table or "Part k of N" header in the chat stream').not.toMatch(PART_TABLE);
    await scanner.settle();
    expect(scanner.violations.filter((v) => v.rule === 'part-table'), 'the scanner saw no part table in any response').toEqual([]);
    expect(visible, 'no part table is readable on the chat page').not.toMatch(PART_TABLE);
  });
});
