import { expect, type Page } from '@playwright/test';

/** The record page (`/record/<id>`): viewer on the left, "Document details" on the right. */
export class RecordPage {
  constructor(private readonly page: Page, private readonly recordId: string) {}

  async open(): Promise<void> {
    await this.page.goto(`/record/${this.recordId}/`);
    await expect(this.page.getByText('Indexing status', { exact: true }), 'the record page should show the status row').toBeVisible({ timeout: 45_000 });
  }

  fileName(): Promise<string> {
    return this.valueOf('File name');
  }

  /** The text of the indexing-status chip (the roll-up label while parts index, else the status). */
  indexingStatus(): Promise<string> {
    return this.valueOf('Indexing status');
  }

  /** The viewer's page indicator, e.g. "1/1138". */
  async pageIndicator(): Promise<{ current: number; total: number }> {
    const el = this.page.locator('span', { hasText: /^\d+\/\d+$/ }).first();
    await expect(el).toBeVisible({ timeout: 45_000 });
    const [current, total] = (await el.innerText()).split('/').map(Number);
    return { current, total };
  }

  /** The value beside a label of the details panel: climb from the label to the row that also holds the value. */
  private async valueOf(label: string): Promise<string> {
    const el = this.page.getByText(label, { exact: true }).first();
    await expect(el).toBeVisible();
    return el.evaluate((node, text) => {
      let row: HTMLElement | null = node as HTMLElement;
      while (row && row.innerText.trim() === text) row = row.parentElement;
      const lines = (row?.innerText ?? '').split('\n').map((l) => l.trim()).filter(Boolean);
      return lines.find((l) => l !== text) ?? '';
    }, label);
  }
}
