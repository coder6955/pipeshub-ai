import { describe, expect, it } from 'vitest';
import { createInstance } from 'i18next';
import type { TFunction } from 'i18next';
import enUS from '@/lib/i18n/locales/en-US.json';
import { getKbItemStatusIcon, getKbItemStatusLabel } from '../components/kb-list-view';
import { getReindexLabel, getReindexMenuOptions, getReindexMenuLabel, requiresForceReindexConfirmation } from '../utils/reindex-label';
import { hubNode } from './kb-page-harness';

const i18n = createInstance();
void i18n.init({ lng: 'en-US', resources: { 'en-US': { translation: enUS } }, interpolation: { escapeValue: false } });
const t = i18n.t.bind(i18n) as unknown as TFunction;

const file = (fields: Partial<Parameters<typeof hubNode>[0]>) =>
  hubNode({ id: 'r1', name: 'big.pdf', indexingStatus: 'COMPLETED', ...fields });

describe('status label of a split file', () => {
  it.each([
    [{ splitState: 'SPLITTING', splitDone: 0, splitTotal: 4 }, 'Preparing parts'],
    [{ splitState: 'INDEXING_PARTS', splitDone: 1, splitTotal: 4 }, 'Indexing 1 of 4 parts'],
    [{ splitState: 'COMPLETED', splitDone: 4, splitTotal: 4 }, 'Completed'],
    [{ splitState: 'PARTIAL', splitDone: 4, splitTotal: 4, splitFailed: 1 }, '1 of 4 parts failed'],
    [{ splitState: 'FAILED', splitDone: 4, splitTotal: 4, splitFailed: 4 }, 'Failed'],
  ] as const)('%o shows "%s"', (fields, expected) => {
    expect(getKbItemStatusLabel(file(fields), t)).toBe(expected);
  });

  it('shows exactly the old labels when the API sends no split fields', () => {
    expect(getKbItemStatusLabel(file({}), t)).toBe('Completed');
    expect(getKbItemStatusLabel(file({ indexingStatus: 'FAILED', reason: 'boom' }), t)).toBe('Failed - boom');
    expect(getKbItemStatusLabel(file({ splitState: null, splitDone: null, splitTotal: null }), t)).toBe('Completed');
  });

  it('ignores malformed split fields', () => {
    expect(getKbItemStatusLabel(file({ splitState: 'INDEXING_PARTS', splitDone: 1, splitTotal: 0 }), t)).toBe('Completed');
    expect(getKbItemStatusLabel(file({ splitState: 'NOPE' as never, splitDone: 1, splitTotal: 4 }), t)).toBe('Completed');
  });

  it('shows nothing for a record with no status, split or not', () => {
    expect(getKbItemStatusLabel(file({ indexingStatus: null, splitState: 'SPLITTING', splitDone: 0, splitTotal: 4 }), t)).toBe('');
  });

  it('keeps the short label short', () => {
    expect(getKbItemStatusLabel(file({ splitState: 'INDEXING_PARTS', splitDone: 1, splitTotal: 4 }), t, false)).toBe('In progress');
  });

  it('shows Partly indexed as the short label of PARTIAL, and Failed for FAILED', () => {
    expect(getKbItemStatusLabel(file({ splitState: 'PARTIAL', splitDone: 4, splitTotal: 4, splitFailed: 1 }), t, false)).toBe('Partly indexed');
    expect(getKbItemStatusLabel(file({ splitState: 'FAILED', splitDone: 4, splitTotal: 4, splitFailed: 4 }), t, false)).toBe('Failed');
  });

  it('shows no part id or part name in any state', () => {
    const states = ['SPLITTING', 'INDEXING_PARTS', 'COMPLETED', 'PARTIAL', 'FAILED'] as const;
    for (const splitState of states) {
      const item = file({
        splitState, splitDone: 2, splitTotal: 5, splitFailed: 1,
        externalRecordId: 'part:7f3a9c21:2', connectorId: 'pdfparts_org1', name: 'big.pdf',
      });
      for (const withReason of [true, false]) {
        expect(getKbItemStatusLabel(item, t, withReason)).not.toMatch(/pdfparts_|part:\w+:\d+|V_\d+|[0-9a-f]{8}/i);
      }
    }
  });
});

describe('status icon of a split file', () => {
  it('differs from the completed icon while the parts are indexing, and for failed parts', () => {
    const iconProps = (f: Partial<Parameters<typeof hubNode>[0]>) => JSON.stringify(getKbItemStatusIcon(file(f), false)?.valueOf());
    const completed = iconProps({});
    expect(iconProps({ splitState: 'COMPLETED', splitDone: 2, splitTotal: 2 })).toBe(completed);
    expect(iconProps({ splitState: 'INDEXING_PARTS', splitDone: 1, splitTotal: 2 })).not.toBe(completed);
    const partial = iconProps({ splitState: 'PARTIAL', splitDone: 2, splitTotal: 2, splitFailed: 1 });
    const failed = iconProps({ splitState: 'FAILED', splitDone: 2, splitTotal: 2, splitFailed: 2 });
    expect(partial).not.toBe(completed);
    expect(partial).not.toBe(failed); // a warning, not the error icon
    expect(partial).toContain('warning_amber');
  });
});

describe('reindex action of a split file', () => {
  const node = (fields: Record<string, unknown>) => ({ nodeType: 'record', indexingStatus: 'COMPLETED', hasChildren: false, ...fields });
  const retryLabel = (fields: Record<string, unknown>) =>
    getReindexMenuOptions(node(fields)).map((o) => getReindexMenuLabel(o, t as unknown as (k: string) => string));

  it('is "Retry failed parts" for PARTIAL and FAILED, without a force-reindex confirmation', () => {
    for (const splitState of ['PARTIAL', 'FAILED']) {
      const n = node({ splitState, splitDone: 3, splitTotal: 3, splitFailed: 1 });
      expect(retryLabel(n)).toEqual(['Retry failed parts']);
      expect(getReindexLabel(n)).toBe('Retry failed parts');
      expect(requiresForceReindexConfirmation(n)).toBe(false);
    }
  });

  it('has no action while the parts are being prepared or indexed', () => {
    for (const splitState of ['SPLITTING', 'INDEXING_PARTS']) {
      expect(getReindexMenuOptions(node({ splitState, splitDone: 1, splitTotal: 3 }))).toEqual([]);
    }
  });

  it('is unchanged for a completed split and for a file that is not split', () => {
    expect(retryLabel({ splitState: 'COMPLETED', splitDone: 3, splitTotal: 3 })).toEqual(['Force reindex']);
    expect(retryLabel({})).toEqual(['Force reindex']);
    expect(requiresForceReindexConfirmation(node({}))).toBe(true);
  });
});

describe('status icon name of a split file', () => {
  const iconName = (f: Partial<Parameters<typeof hubNode>[0]>) =>
    (getKbItemStatusIcon(file(f), false) as { props: { name: string } }).props.name;

  it('is the failure icon for a failed split, the check for a completed one', () => {
    expect(iconName({ splitState: 'FAILED', splitDone: 2, splitTotal: 2, splitFailed: 2 })).toBe('error_outline');
    expect(iconName({ splitState: 'COMPLETED', splitDone: 2, splitTotal: 2 })).toBe('check_circle');
    expect(iconName({})).toBe('check_circle');
  });
});

describe('reindex label of a sidebar tree node', () => {
  it('keeps the split fields, so a partly failed file shows "Retry failed parts"', async () => {
    const { nodeToTreeNode } = await import('../utils/tree-builder');
    const { getReindexNodeFromHubItem } = await import('../utils/reindex-label');
    const tree = nodeToTreeNode(file({ splitState: 'PARTIAL', splitDone: 3, splitTotal: 3, splitFailed: 1 }) as never);
    expect(tree).toMatchObject({ splitState: 'PARTIAL', splitDone: 3, splitTotal: 3, splitFailed: 1 });
    const reindexNode = getReindexNodeFromHubItem({ nodeType: 'record', indexingStatus: tree.indexingStatus, ...tree });
    expect(getReindexLabel(reindexNode)).toBe('Retry failed parts');
  });
});
