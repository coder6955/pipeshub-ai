import { expect, type Page } from '@playwright/test';

/** The PDF viewer panel opened by "Preview" (chat, search) — pdf.js with a page indicator "n/total". */
export class PdfViewer {
  constructor(private readonly page: Page) {}

  private indicator() {
    return this.page.locator('span', { hasText: /^\d+\/\d+$/ }).last();
  }

  /** The page the viewer shows and how many pages the opened document has. */
  async position(): Promise<{ current: number; total: number }> {
    await expect(this.indicator(), 'the viewer should show a page indicator').toBeVisible({ timeout: 60_000 });
    const [current, total] = (await this.indicator().innerText()).split('/').map(Number);
    return { current, total };
  }

  /** Wait until the viewer has settled on *page* (it first shows 1/n, then scrolls to the citation). */
  async waitForPage(page: number, timeout = 30_000): Promise<void> {
    await expect
      .poll(async () => (await this.position()).current, { message: `the viewer should scroll to page ${page}`, timeout })
      .toBe(page);
  }

  /** The document title in the viewer header. */
  async title(): Promise<string> {
    return (await this.page.getByLabel(/^Open .* in source$/).first().getAttribute('aria-label'))?.replace(/^Open (.*) in source$/, '$1') ?? '';
  }

  /** How many pages pdf.js laid out. For a split file this must be the original's count. */
  pdfPages(): Promise<number> {
    return this.page.locator('.page[data-page-number]').count();
  }

  /** Text of one rendered page's text layer, once pdf.js has drawn it. */
  async pageText(page: number): Promise<string> {
    const layer = this.page.locator(`.page[data-page-number="${page}"] .textLayer`);
    await expect(layer, `page ${page} should be rendered`).toBeVisible({ timeout: 30_000 });
    await expect.poll(async () => (await layer.innerText()).length, { timeout: 30_000 }).toBeGreaterThan(20);
    return layer.innerText();
  }

  async close(): Promise<void> {
    await this.page.getByLabel('Close preview').click();
  }
}

const norm = (s: string) => s.toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim().split(' ').filter((w) => w.length > 2);

/** Share of the first words of *snippet* that appear in *pageText*: how well a citation matches its page. */
export function snippetOverlap(snippet: string, pageText: string, words = 14): number {
  const want = norm(snippet).slice(0, words);
  if (!want.length) return 0;
  const have = new Set(norm(pageText));
  return want.filter((w) => have.has(w)).length / want.length;
}
