/**
 * (a), the part that needs a file mid-index: upload a 501-page cut of the manual (two parts), and
 * watch the list and the record page while its parts index. The row keeps the original name, shows
 * "Indexing N of 2 parts" (or "Preparing parts"), and ends on the normal "Completed" label.
 * The upload is this journey's own content: it is deleted at the end, and it is not in the
 * collection while the other specs run (this one sorts last).
 *
 * Needs E2E_PROGRESS_PDF: the file made by make-progress-pdf.py (pages 600-1100 of the manual).
 */
import * as fs from 'fs';
import { test, expect, readRoll } from './large-pdf.fixture';
import { uploadFileByApi } from '../kb-upload.helpers';
import { KbListPage } from './pages/kb-list.page';
import { LARGE_NAME } from './split-oracle';
import { RecordPage } from './pages/record.page';
import { expectNoPartInDom } from './pages/dom-guard';

const NAME = 'progress-501';
const PARTS = 2;

test.describe('large PDF: progress while parts index @large-pdf', () => {
  test('the row and record page show the roll-up, then Completed, under the original name', async ({ page, apiContext, stack, scanner, shot }) => {
    test.setTimeout(1_500_000);
    const path = process.env.E2E_PROGRESS_PDF;
    expect(path && fs.existsSync(path), 'E2E_PROGRESS_PDF must point at the 501-page PDF (see make-progress-pdf.py)').toBeTruthy();
    const buffer = fs.readFileSync(path!);

    const recordId = await uploadFileByApi(stack.kbId, { name: `${NAME}.pdf`, mimeType: 'application/pdf', buffer });
    scanner.allow(recordId);
    try {
      const list = new KbListPage(page, stack.kbId);
      const labels: string[] = [];
      const recordLabels: string[] = [];
      let sawProgress = false;
      const deadline = Date.now() + 1_300_000;
      let completed = false;
      while (Date.now() < deadline && !completed) {
        const vrid = ((await (await apiContext.get(`/api/v1/knowledgeBase/record/${recordId}`)).json()) as { record?: { virtualRecordId?: string } }).record?.virtualRecordId;
        scanner.allow(recordId, vrid);
        await list.open();
        await expect(list.row(NAME), 'the uploaded file is listed under its own name').toBeVisible({ timeout: 30_000 });
        await expect(list.rows()).toHaveCount(2, { timeout: 30_000 });
        const rows = (await list.rowNames()).filter(Boolean);
        expect(rows.filter((r) => r === NAME), 'one row for the file, however many parts it has').toHaveLength(1);
        expect(rows.sort(), 'no extra row for a part').toEqual([NAME, LARGE_NAME].sort());
        const label = await list.statusLabel(NAME);
        if (labels[labels.length - 1] !== label) labels.push(label);
        expect(await list.rowText(NAME)).not.toMatch(/pdfparts_|·\s*pages?/i);
        await expectNoPartInDom(page, `list while "${label}"`);

        if (/^Indexing \d+ of \d+ parts?$/.test(label) || label === 'Preparing parts') {
          if (!sawProgress) await shot(page, `list-${label.replace(/\W+/g, '-')}`);
          const record = new RecordPage(page, recordId);
          await record.open();
          const chip = await record.indexingStatus();
          recordLabels.push(chip);
          expect(await record.fileName(), 'the record page names the original').toBe(NAME);
          if (!sawProgress) await shot(page, `record-${chip.replace(/\W+/g, '-')}`);
          expect(chip, 'the record page shows the same roll-up as the list').toMatch(/^(Preparing parts|Indexing \d+ of \d+ parts?|Completed)$/);
          await expectNoPartInDom(page, `record while "${chip}"`);
          sawProgress = true;
        }
        completed = label === 'Completed' && (await readRoll(apiContext, recordId)).splitState === 'COMPLETED';
        if (!completed) await page.waitForTimeout(15_000);
      }
      test.info().annotations.push({ type: 'labels', description: `list: ${labels.join(' → ')}; record: ${recordLabels.join(' → ')}` });
      expect(sawProgress, `a progress label was seen while parts indexed (list labels: ${labels.join(' → ')})`).toBe(true);
      expect(labels.some((l) => new RegExp(`^Indexing \\d+ of ${PARTS} parts$`).test(l)), `"Indexing N of ${PARTS} parts" was shown (saw: ${labels.join(' → ')})`).toBe(true);
      expect(completed, 'the file reached Completed').toBe(true);

      await list.open();
      expect(await list.statusLabel(NAME)).toBe('Completed');
      await shot(page, 'list-completed');
      const record = new RecordPage(page, recordId);
      await record.open();
      expect(await record.indexingStatus()).toBe('Completed');
      expect((await record.pageIndicator()).total, 'the original 501 pages').toBe(501);
      await shot(page, 'record-completed');
    } finally {
      // Only this journey's own upload.
      const roll = await apiContext.get(`/api/v1/knowledgeBase/record/${recordId}`);
      const name = ((await roll.json()) as { record?: { recordName?: string } }).record?.recordName;
      if (name === NAME) {
        let removed = false;
        for (let attempt = 0; attempt < 2 && !removed; attempt += 1) {
          removed = (await apiContext.delete(`/api/v1/knowledgeBase/record/${recordId}`)).ok();
        }
        expect(removed, 'cleanup removed the upload').toBe(true);
      }
    }
  });
});
