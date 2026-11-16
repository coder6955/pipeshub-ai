/**
 * The DOM half of (a)/(e): nothing a user can see or hover on a page, and no link or attribute
 * behind it, names a part. Network responses are covered by the response scanner.
 */
import { expect, type Page } from '@playwright/test';

const VISIBLE_TEXT = [
  { name: 'part connector id', re: /pdfparts_/i },
  { name: 'part page label (" · pages")', re: /·\s*pages?\b/i },
  { name: 'part label ("Part k of n")', re: /\bpart\s+\d+\s+of\s+\d+\b/i },
];

/** Visible text, attributes (href, title, aria-label, alt, data-*) and tooltips of the current page. */
export async function expectNoPartInDom(page: Page, where: string): Promise<void> {
  const snapshot = await page.evaluate(() => {
    const attrs: string[] = [];
    for (const el of Array.from(document.querySelectorAll('body *'))) {
      for (const a of ['href', 'src', 'title', 'aria-label', 'alt', 'data-id', 'data-record-id', 'data-testid']) {
        const v = el.getAttribute(a);
        if (v) attrs.push(`${a}=${v}`);
      }
    }
    return { text: document.body.innerText, attrs: attrs.join('\n') };
  });
  for (const { name, re } of VISIBLE_TEXT) {
    expect(snapshot.text, `${where}: visible text must not contain a ${name}`).not.toMatch(re);
    expect(snapshot.attrs, `${where}: attributes must not contain a ${name}`).not.toMatch(re);
  }
}

/** Every `/record/<id>` link on the page must point at a file the user can see. */
export async function expectLinksToVisibleRecords(page: Page, visible: readonly string[], where: string): Promise<string[]> {
  const ids = await page.evaluate(() =>
    Array.from(document.querySelectorAll('a[href*="/record/"]')).map((a) => (a.getAttribute('href') ?? '').match(/\/record\/([0-9a-f-]{36})/i)?.[1] ?? ''),
  );
  for (const id of ids.filter(Boolean)) {
    expect(visible, `${where}: link to record ${id} must be a visible file`).toContain(id);
  }
  return ids.filter(Boolean);
}
