import type { Page, Response } from '@playwright/test';

export interface SearchHit {
  recordId: string;
  recordName: string;
  virtualRecordId: string;
  pages: number[];
  /** File-wide paragraph ordinals shown to the user. */
  paragraphs: number[];
  text: string;
  collection: string;
}

/** The fields of a search hit's metadata this journey reads. */
export interface SearchHitMetadata {
  recordId: string;
  recordName: string;
  virtualRecordId: string;
  pageNum?: number[];
  blockNum?: number[];
  connectorInstanceName?: string;
}

/** Search view of the chat page: the magnifier toggles it, Enter runs the query. */
export class SearchPage {
  constructor(private readonly page: Page) {}

  async open(): Promise<void> {
    await this.page.goto('/chat/');
    await this.page.getByLabel('Switch to search view').click();
  }

  /** Run a query and return what the page asked for and got: the hits of `/api/v1/search`. */
  async search(query: string): Promise<{ hits: SearchHit[]; response: Response }> {
    const answered = this.page.waitForResponse((r) => r.url().includes('/api/v1/search') && r.request().method() === 'POST', { timeout: 120_000 });
    const input = this.page.locator('textarea').last();
    await input.fill(query);
    await input.press('Enter');
    const response = await answered;
    const body = (await response.json()) as { searchResponse?: { searchResults?: Array<{ content?: string; metadata: SearchHitMetadata }> } };
    const hits = (body.searchResponse?.searchResults ?? []).map((r) => ({
      recordId: r.metadata.recordId,
      recordName: r.metadata.recordName,
      virtualRecordId: r.metadata.virtualRecordId,
      pages: r.metadata.pageNum ?? [],
      paragraphs: r.metadata.blockNum ?? [],
      text: r.content ?? '',
      collection: r.metadata.connectorInstanceName ?? '',
    }));
    // The cards render after the response; wait for the first one.
    await this.page.getByRole('button', { name: 'Preview' }).first().waitFor({ timeout: 30_000 });
    return { hits, response };
  }

  /** The result cards as the user reads them: name, collection, page label. */
  cards(): Promise<Array<{ name: string; collection: string; page: number | null; text: string }>> {
    return this.page.getByRole('button', { name: 'Preview' }).evaluateAll((buttons) =>
      buttons.map((b) => {
        let el: HTMLElement | null = b as HTMLElement;
        while (el && !/Relevance/.test(el.innerText)) el = el.parentElement;
        const lines = (el?.innerText ?? '').split('\n').map((l) => l.trim()).filter((l) => l && l !== 'Preview' && l !== 'Open in collections');
        const page = lines.map((l) => l.match(/^Page (\d+)$/)?.[1]).find(Boolean);
        return { name: lines[0] ?? '', collection: lines[1] ?? '', page: page ? Number(page) : null, text: lines.sort((a, c) => c.length - a.length)[0] ?? '' };
      }),
    );
  }

  /** Reset to a fresh search for the next query. */
  async clear(): Promise<void> {
    await this.page.goto('/chat/');
    await this.page.getByLabel('Switch to search view').click();
  }
}
