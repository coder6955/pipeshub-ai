import { describe, expect, it } from 'vitest';
import { createInstance } from 'i18next';
import type { TFunction } from 'i18next';
import enUS from '@/lib/i18n/locales/en-US.json';
import { isSplitRetryable, isSplitRunning, splitDisplayStatus, splitProgressOf, splitStatusLabel } from '../split-progress';

const i18n = createInstance();
void i18n.init({ lng: 'en-US', resources: { 'en-US': { translation: enUS } }, interpolation: { escapeValue: false } });
const t = i18n.t.bind(i18n) as unknown as TFunction;

const label = (fields: Parameters<typeof splitProgressOf>[0], detailed = true) =>
  splitStatusLabel(splitProgressOf(fields), t, detailed);

describe('splitProgressOf', () => {
  it('is null for a record with no split fields', () => {
    expect(splitProgressOf({})).toBeNull();
    expect(splitProgressOf({ splitState: null, splitDone: null, splitTotal: null })).toBeNull();
  });

  it('reads a valid roll-up', () => {
    expect(splitProgressOf({ splitState: 'INDEXING_PARTS', splitDone: 3, splitTotal: 9, splitFailed: 1 })).toEqual({
      state: 'INDEXING_PARTS', done: 3, total: 9, failed: 1,
    });
  });

  it.each([
    ['unknown state', { splitState: 'SOMETHING', splitDone: 1, splitTotal: 2 }],
    ['missing total', { splitState: 'INDEXING_PARTS', splitDone: 1 }],
    ['zero total', { splitState: 'INDEXING_PARTS', splitDone: 0, splitTotal: 0 }],
    ['negative done', { splitState: 'INDEXING_PARTS', splitDone: -1, splitTotal: 4 }],
    ['NaN total', { splitState: 'INDEXING_PARTS', splitDone: 1, splitTotal: Number.NaN }],
    ['infinite total', { splitState: 'INDEXING_PARTS', splitDone: 1, splitTotal: Number.POSITIVE_INFINITY }],
    ['string count', { splitState: 'INDEXING_PARTS', splitDone: '1', splitTotal: 4 }],
    ['numeric state', { splitState: 3, splitDone: 1, splitTotal: 4 }],
  ])('ignores a malformed roll-up (%s)', (_name, fields) => {
    expect(splitProgressOf(fields as never)).toBeNull();
  });

  it('holds counts that ran ahead of the total at the total, and a missing failed count at zero', () => {
    expect(splitProgressOf({ splitState: 'INDEXING_PARTS', splitDone: 12, splitTotal: 9, splitFailed: 20 })).toEqual({
      state: 'INDEXING_PARTS', done: 9, total: 9, failed: 9,
    });
    expect(splitProgressOf({ splitState: 'COMPLETED', splitDone: 9, splitTotal: 9 })?.failed).toBe(0);
  });
});

describe('splitStatusLabel', () => {
  it('SPLITTING says the parts are being prepared', () => {
    expect(label({ splitState: 'SPLITTING', splitDone: 0, splitTotal: 9 })).toBe('Preparing parts');
  });

  it('INDEXING_PARTS shows done of total, with the singular for one', () => {
    expect(label({ splitState: 'INDEXING_PARTS', splitDone: 3, splitTotal: 9 })).toBe('Indexing 3 of 9 parts');
    expect(label({ splitState: 'INDEXING_PARTS', splitDone: 0, splitTotal: 1 })).toBe('Indexing 0 of 1 part');
  });

  it('INDEXING_PARTS keeps the short label short', () => {
    expect(label({ splitState: 'INDEXING_PARTS', splitDone: 3, splitTotal: 9 }, false)).toBe('In progress');
  });

  it('COMPLETED defers to the normal label', () => {
    expect(label({ splitState: 'COMPLETED', splitDone: 9, splitTotal: 9 })).toBeNull();
  });

  it('PARTIAL shows how many parts failed', () => {
    expect(label({ splitState: 'PARTIAL', splitDone: 9, splitTotal: 9, splitFailed: 2 })).toBe('2 of 9 parts failed');
    expect(label({ splitState: 'PARTIAL', splitDone: 1, splitTotal: 1, splitFailed: 1 })).toBe('1 of 1 part failed');
  });

  it('PARTIAL with no failed count says partly indexed', () => {
    expect(label({ splitState: 'PARTIAL', splitDone: 9, splitTotal: 9 })).toBe('Partly indexed');
  });

  it('PARTIAL short label is Partly indexed, not Failed', () => {
    expect(label({ splitState: 'PARTIAL', splitDone: 9, splitTotal: 9, splitFailed: 2 }, false)).toBe('Partly indexed');
  });

  it('FAILED short label stays Failed', () => {
    expect(label({ splitState: 'FAILED', splitDone: 9, splitTotal: 9, splitFailed: 9 }, false)).toBe('Failed');
  });

  it('FAILED shows Failed', () => {
    expect(label({ splitState: 'FAILED', splitDone: 9, splitTotal: 9, splitFailed: 9 })).toBe('Failed');
  });

  it('is null with no split', () => {
    expect(label({})).toBeNull();
  });

  it.each([
    [{ splitState: 'SPLITTING', splitDone: 0, splitTotal: 9 }],
    [{ splitState: 'INDEXING_PARTS', splitDone: 3, splitTotal: 9 }],
    [{ splitState: 'PARTIAL', splitDone: 9, splitTotal: 9, splitFailed: 2 }],
    [{ splitState: 'FAILED', splitDone: 9, splitTotal: 9, splitFailed: 9 }],
  ])('never shows a part id or a part name (%o)', (fields) => {
    for (const detailed of [true, false]) {
      const text = label(fields as never, detailed) ?? '';
      expect(text).not.toMatch(/part[:_ -]?[0-9a-f]{8}|pdfparts_|part:\w+:\d+|V_\d+|[0-9a-f]{8}-[0-9a-f]{4}/i);
    }
  });
});

describe('split helpers', () => {
  it('classifies running and retryable states', () => {
    const p = (splitState: string) => splitProgressOf({ splitState, splitDone: 1, splitTotal: 2, splitFailed: 1 });
    expect(['SPLITTING', 'INDEXING_PARTS'].map((s) => isSplitRunning(p(s)))).toEqual([true, true]);
    expect(['COMPLETED', 'PARTIAL', 'FAILED'].map((s) => isSplitRunning(p(s)))).toEqual([false, false, false]);
    expect(['PARTIAL', 'FAILED'].map((s) => isSplitRetryable(p(s)))).toEqual([true, true]);
    expect(['SPLITTING', 'INDEXING_PARTS', 'COMPLETED'].map((s) => isSplitRetryable(p(s)))).toEqual([false, false, false]);
    expect(isSplitRunning(null)).toBe(false);
    expect(isSplitRetryable(null)).toBe(false);
  });

  it('overrides the manifest COMPLETED status while running or failed, and nothing else', () => {
    const p = (splitState: string) => splitProgressOf({ splitState, splitDone: 1, splitTotal: 2, splitFailed: 1 });
    expect(splitDisplayStatus('COMPLETED', p('INDEXING_PARTS'))).toBe('IN_PROGRESS');
    expect(splitDisplayStatus('COMPLETED', p('PARTIAL'))).toBe('PARTLY_INDEXED');
    expect(splitDisplayStatus('COMPLETED', p('FAILED'))).toBe('FAILED');
    expect(splitDisplayStatus('COMPLETED', p('COMPLETED'))).toBe('COMPLETED');
    expect(splitDisplayStatus('COMPLETED', null)).toBe('COMPLETED');
    expect(splitDisplayStatus(undefined, null)).toBeUndefined();
  });
});
