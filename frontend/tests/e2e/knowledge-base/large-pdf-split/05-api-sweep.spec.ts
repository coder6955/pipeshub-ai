/**
 * (e) The API half of the response scanner: the endpoints the pages use, called directly with the
 * session, must be clean too. The browser journeys scan their own traffic through the same scanner.
 */
import { test, expect } from './large-pdf.fixture';
import { ResponseScanner } from './leak-scanner';
import { PAGE_ORACLE } from './split-oracle';

test.describe('large PDF: API sweep @large-pdf', () => {
  test('listing, record, details and search responses carry no part id or split internals', async ({ apiContext, stack }) => {
    const sweep = new ResponseScanner(stack.identity);
    const get = async (url: string) => {
      const r = await apiContext.get(url);
      expect(r.ok(), url).toBe(true);
      sweep.record(url, r.status(), await r.text());
    };
    await get('/api/v1/knowledgeBase/?limit=100');
    await get(`/api/v1/knowledgeBase/knowledge-hub/nodes/app/${stack.kbId}?limit=200`);
    await get(`/api/v1/knowledgeBase/record/${stack.large.id}`);
    await get(`/api/v1/knowledgeBase/record/${stack.short.id}`);
    for (const query of Object.keys(PAGE_ORACLE)) {
      const r = await apiContext.post('/api/v1/search', { data: { query, limit: 10, filters: { apps: [stack.kbId], kb: [] } } });
      expect(r.ok(), query).toBe(true);
      sweep.record(`/api/v1/search?${query}`, r.status(), await r.text());
    }
    expect(sweep.scanned.length).toBeGreaterThanOrEqual(4 + Object.keys(PAGE_ORACLE).length);
    expect(sweep.violations, sweep.summary()).toEqual([]);
  });
});
