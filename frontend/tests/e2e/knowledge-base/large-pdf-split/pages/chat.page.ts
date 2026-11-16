import { expect, type Page } from '@playwright/test';

export interface CitationCard {
  index: number;
  name: string;
  collection: string;
  page: number | null;
  paragraph: number | null;
  snippet: string;
}

/** Chat started from a collection page ("Ask in '<collection>' anything…"), so retrieval is scoped to it. */
export class ChatPage {
  constructor(private readonly page: Page) {}

  /** Ask from the collection page; resolves with the SSE body once the stream has ended. */
  async askInCollection(kbId: string, question: string): Promise<{ stream: string; conversationId: string }> {
    await this.page.goto(`/knowledge-base/?nodeType=app&nodeId=${kbId}`);
    // The placeholder goes away once there is text, so wait on it and then hold the field itself.
    await expect(this.page.getByPlaceholder(/^Ask in /)).toBeVisible({ timeout: 30_000 });
    const box = this.page.locator('textarea').last();
    const streamed = this.page.waitForResponse((r) => /\/conversations\/.*stream|\/conversations\/stream/.test(r.url()) && r.request().method() === 'POST', { timeout: 60_000 });
    await box.fill(question);
    await box.press('Enter');
    const response = await streamed;
    await this.page.waitForURL(/\/chat\/\?conversationId=/, { timeout: 60_000 });
    const stream = await response.text();
    return { stream, conversationId: new URL(this.page.url()).searchParams.get('conversationId') ?? '' };
  }

  /** The agent may ask a clarifying question before answering; skip it so the run goes on with its best match. */
  async skipClarification(): Promise<boolean> {
    const skip = this.page.getByRole('button', { name: 'Skip', exact: true });
    if (await skip.isVisible().catch(() => false)) {
      await skip.click();
      return true;
    }
    return false;
  }

  /** Wait for the answer's citations, skipping any clarifying question on the way. */
  async waitForCitations(atLeast: number, timeoutMs = 540_000): Promise<number> {
    const deadline = Date.now() + timeoutMs;
    let count = 0;
    while (Date.now() < deadline) {
      await this.skipClarification();
      const tab = this.page.getByText('Citation', { exact: true }).last();
      count = await tab
        .scrollIntoViewIfNeeded({ timeout: 2_000 })
        .then(() => tab.locator('xpath=..').innerText())
        .then((t) => Number(t.replace(/\D+/g, '')) || 0)
        .catch(() => 0);
      if (count >= atLeast) return count;
      await this.page.waitForTimeout(2_000);
    }
    return count;
  }

  /** The number on the Citation tab. Disabled (0) when the answer cites nothing. */
  async citationCount(): Promise<number> {
    const tab = this.page.getByText('Citation', { exact: true }).last();
    await expect(tab).toBeVisible({ timeout: 60_000 });
    const text = await tab.locator('xpath=..').innerText();
    return Number(text.replace(/\D+/g, '')) || 0;
  }

  async openCitationTab(): Promise<void> {
    await this.page.getByText('Citation', { exact: true }).last().click();
    await expect(this.page.getByRole('button', { name: 'Preview' }).first()).toBeVisible({ timeout: 15_000 });
  }

  async openAnswerTab(): Promise<void> {
    await this.page.getByText('Answer', { exact: true }).first().click();
  }

  /** Cards of the Citation tab, in citation order. */
  async cards(): Promise<CitationCard[]> {
    const raw = await this.page.getByRole('button', { name: 'Preview' }).evaluateAll((buttons) =>
      buttons.map((b) => {
        let el: HTMLElement | null = b as HTMLElement;
        while (el && !/Page \d+/.test(el.innerText)) el = el.parentElement;
        const lines = (el?.innerText ?? '').split('\n').map((l) => l.trim()).filter(Boolean);
        const page = lines.map((l) => l.match(/^Page (\d+)$/)?.[1]).find(Boolean);
        const para = lines.map((l) => l.match(/^Paragraph (\d+)$/)?.[1]).find(Boolean);
        const named = lines.indexOf('Preview');
        return {
          collection: lines[0] ?? '',
          name: lines[named + 1] ?? '',
          page: page ? Number(page) : null,
          paragraph: para ? Number(para) : null,
          snippet: lines.reduce((a, c) => (c.length > a.length ? c : a), ''),
        };
      }),
    );
    return raw.map((c, i) => ({ index: i + 1, ...c }));
  }

  /** Click "Preview" on card *index* (1-based) and wait for the viewer. */
  async previewCard(index: number): Promise<void> {
    await this.page.getByRole('button', { name: 'Preview' }).nth(index - 1).click();
  }

  /** Inline citation chips of the answer: `<a href="/record/<id>">` links. */
  inlineChips() {
    return this.page.locator('a[href^="/record/"][aria-haspopup="dialog"]');
  }
}
