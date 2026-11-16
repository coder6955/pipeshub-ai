/**
 * (a) List and record page. A split file is one row with its own name and the normal status; the
 * record page shows the same; no part id, "pdfparts_" or "· pages" appears on either.
 * The progress label while parts index is covered live in 06-progress-live.spec.ts.
 */
import { test, expect, readRoll } from './large-pdf.fixture';
import { LARGE_NAME, LARGE_PAGE_COUNT } from './split-oracle';
import { KbListPage } from './pages/kb-list.page';
import { RecordPage } from './pages/record.page';
import { expectNoPartInDom } from './pages/dom-guard';

test.describe('large PDF: list and record page @large-pdf', () => {
  test('the large split file has finished and the record API says so', async ({ apiContext, stack }) => {
    const record = await readRoll(apiContext, stack.large.id);
    expect(record.indexingStatus).toBe('COMPLETED');
    expect(record.splitState).toBe('COMPLETED');
    expect(record.splitTotal).toBe(3);
    expect(record.splitDone).toBe(3);
    expect(record.splitFailed ?? 0).toBe(0);
  });

  test('the collection lists one row per original file, with the normal Completed label', async ({ page, stack, shot }) => {
    const list = new KbListPage(page, stack.kbId);
    const listed = page.waitForResponse((r) => r.url().includes(`/knowledge-hub/nodes/app/${stack.kbId}`) && r.ok());
    await list.open();
    await shot(page, 'collection-list');

    const names = (await list.rowNames()).filter(Boolean).sort();
    expect(names, 'one row for the split file, nothing for its parts').toEqual([LARGE_NAME]);

    const body = (await (await listed).json()) as { items?: Array<Record<string, unknown>>; nodes?: Array<Record<string, unknown>> };
    const items = body.items ?? body.nodes ?? [];
    expect(items.map((i) => i.name), 'the listing API returns the same one file').toEqual([LARGE_NAME]);
    const large = items.find((i) => i.name === LARGE_NAME)!;
    expect(large.id, 'the row is the original file').toBe(stack.large.id);
    expect(large.splitState).toBe('COMPLETED');

    expect(await list.statusLabel(LARGE_NAME)).toBe('Completed');
    await shot(page, 'status-tooltip-completed');
    for (const name of names) {
      const text = await list.rowText(name);
      expect(text, `row ${name}`).not.toMatch(/pdfparts_|·\s*pages?|\bpart\s+\d+\s+of\s+\d+/i);
    }
    await expectNoPartInDom(page, 'collection list');
  });

  test('the record page shows the original name, the Completed status and all 1,138 pages', async ({ page, stack, shot }) => {
    const record = new RecordPage(page, stack.large.id);
    await record.open();
    expect(await record.fileName()).toBe(LARGE_NAME);
    expect(await record.indexingStatus()).toBe('Completed');
    const { total } = await record.pageIndicator();
    expect(total, 'the viewer opens the original PDF, not a part').toBe(LARGE_PAGE_COUNT);
    await shot(page, 'record-page');
    await expect(page.getByText(/pdfparts_/i)).toHaveCount(0);
    await expectNoPartInDom(page, 'record page');
  });
});
