/**
 * (d) The unsplit path. The short file shows no split UI, and a question answered
 * from it cites it with correct pages and opens it in the viewer at the cited page.
 */
import { test, expect } from './large-pdf.fixture';
import { SHORT_NAME, SHORT_PAGE_COUNT, UNSPLIT_KB_NAME } from './split-oracle';
import { KbListPage } from './pages/kb-list.page';
import { RecordPage } from './pages/record.page';
import { ChatPage } from './pages/chat.page';
import { PdfViewer, snippetOverlap } from './pages/viewer';
import { expectNoPartInDom } from './pages/dom-guard';
import { BLOCK_STRIDE } from './leak-scanner';

test.describe('large PDF: the unsplit file @large-pdf', () => {
  test('the 12-page report has no split fields, label or part text', async ({ page, apiContext, stack, shot }) => {
    const detail = (await (await apiContext.get(`/api/v1/knowledgeBase/record/${stack.short.id}`)).json()) as { record: Record<string, unknown> };
    expect(detail.record.splitState ?? null, 'an unsplit record carries no split roll-up').toBeNull();
    expect(detail.record.splitTotal ?? null).toBeNull();
    expect(detail.record.indexingStatus).toBe('COMPLETED');

    const list = new KbListPage(page, stack.shortKbId);
    await list.open();
    expect(await list.statusLabel(SHORT_NAME)).toBe('Completed');
    expect(await list.rowText(SHORT_NAME)).not.toMatch(/\bparts?\b/i);

    const record = new RecordPage(page, stack.short.id);
    await record.open();
    expect(await record.fileName()).toBe(SHORT_NAME);
    expect(await record.indexingStatus()).toBe('Completed');
    expect((await record.pageIndicator()).total).toBe(SHORT_PAGE_COUNT);
    await shot(page, 'short-record-page');
    await expectNoPartInDom(page, 'unsplit record page');
  });

  test('a question about it cites it on the right page, and the viewer shows that page', async ({ page, stack, shot }) => {
    test.setTimeout(600_000);
    const chat = new ChatPage(page);
    const viewer = new PdfViewer(page);
    const { stream } = await chat.askInCollection(stack.shortKbId, `What is the audit period covered by the independent auditor's report in the "${SHORT_NAME}" document? Answer from that document.`);
    expect(stream).toContain('RUN_FINISHED');
    expect(await chat.waitForCitations(1), 'the answer should carry a citation').toBeGreaterThanOrEqual(1);
    await chat.openCitationTab();
    const cards = await chat.cards();
    await shot(page, 'citation-tab');
    const short = cards.filter((c) => c.name === SHORT_NAME);
    expect(short.length, `the answer cites ${SHORT_NAME}`).toBeGreaterThanOrEqual(1);
    for (const c of short) {
      expect(c.collection).toBe(UNSPLIT_KB_NAME);
      expect(c.page, 'a page inside the 12-page file').toBeGreaterThanOrEqual(1);
      expect(c.page!).toBeLessThanOrEqual(SHORT_PAGE_COUNT);
      expect(c.paragraph!, 'an unsplit file keeps small block numbers').toBeLessThan(BLOCK_STRIDE);
    }
    const card = short[0];
    await chat.previewCard(card.index);
    await viewer.waitForPage(card.page!);
    expect((await viewer.position()).total).toBe(SHORT_PAGE_COUNT);
    expect(await viewer.title()).toBe(SHORT_NAME);
    expect(snippetOverlap(card.snippet, await viewer.pageText(card.page!))).toBeGreaterThanOrEqual(0.6);
    await shot(page, `viewer-page-${card.page}`);
  });
});
