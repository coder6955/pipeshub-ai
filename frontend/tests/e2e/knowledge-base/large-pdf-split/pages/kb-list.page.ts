import { expect, type Locator, type Page } from '@playwright/test';

/** The collection page (`/knowledge-base?nodeType=app&nodeId=<kb>`), list view. */
export class KbListPage {
  constructor(private readonly page: Page, private readonly kbId: string) {}

  async open(): Promise<void> {
    await this.page.goto(`/knowledge-base/?nodeType=app&nodeId=${this.kbId}`);
    await expect(this.page.getByRole('row').nth(1), 'the collection should list its files').toBeVisible({ timeout: 30_000 });
  }

  /** Reload the list in place (a running split changes its label without a page change). */
  async reload(): Promise<void> {
    await this.page.reload();
    await expect(this.page.getByRole('row').nth(1)).toBeVisible({ timeout: 30_000 });
  }

  /** Data rows only: the header row has role=row too but carries column headers. */
  rows(): Locator {
    return this.page.getByRole('row').filter({ hasNot: this.page.getByRole('columnheader') });
  }

  rowNames(): Promise<string[]> {
    return this.rows().evaluateAll((rows) => rows.map((r) => r.getAttribute('aria-label') ?? ''));
  }

  row(name: string): Locator {
    return this.page.getByRole('row', { name, exact: true });
  }

  /** The status shown for a row: hover its status cell and read the tooltip. */
  async statusLabel(name: string): Promise<string> {
    const row = this.row(name);
    // checkbox, name, status, size, ... : the status cell is the first narrow cell after the name.
    const cell = row.locator('> div').nth(2);
    await cell.hover();
    const tip = this.page.getByRole('tooltip').last();
    await expect(tip, `the status tooltip of ${name}`).toBeVisible({ timeout: 5_000 });
    const text = (await tip.innerText()).trim();
    await this.page.mouse.move(0, 0);
    return text;
  }

  /** All the text of a row, for "no part label anywhere" checks. */
  rowText(name: string): Promise<string> {
    return this.row(name).innerText();
  }

  /** Open the row's file in the record page. */
  async openRecord(name: string): Promise<void> {
    await this.row(name).getByRole('button', { name: /preview/i }).first().click().catch(async () => {
      await this.row(name).dblclick();
    });
  }
}
