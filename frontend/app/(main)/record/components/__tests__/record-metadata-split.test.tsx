import React from 'react';
import { afterEach, beforeAll, describe, expect, it } from 'vitest';
import { cleanup, render, screen } from '@testing-library/react';
import { Theme } from '@radix-ui/themes';
import '@/lib/__tests__/test-i18n';
import type { RecordDetailsResponse } from '@/app/(main)/knowledge-base/types';
import { RecordMetadataPanel } from '../record-metadata-panel';

beforeAll(() => {
  globalThis.ResizeObserver ??= class {
    observe() {}
    unobserve() {}
    disconnect() {}
  } as unknown as typeof ResizeObserver;
});
afterEach(cleanup);

const details = (fields: Record<string, unknown>) =>
  ({
    record: { id: 'r1', recordName: 'big.pdf', recordType: 'FILE', origin: 'UPLOAD', indexingStatus: 'COMPLETED', ...fields },
  }) as unknown as RecordDetailsResponse;

const chipText = (fields: Record<string, unknown>) => {
  render(
    <Theme>
      <RecordMetadataPanel recordDetails={details(fields)} />
    </Theme>,
  );
  return screen.getByText('Indexing status').parentElement!.textContent ?? '';
};

describe('the indexing status chip of the record page', () => {
  it.each([
    [{ splitState: 'INDEXING_PARTS', splitDone: 1, splitTotal: 4 }, 'Indexing 1 of 4 parts'],
    [{ splitState: 'PARTIAL', splitDone: 4, splitTotal: 4, splitFailed: 1 }, '1 of 4 parts failed'],
    [{ splitState: 'SPLITTING', splitDone: 0, splitTotal: 4 }, 'Preparing parts'],
  ])('shows the roll-up of a split file (%o)', (fields, expected) => {
    expect(chipText(fields)).toContain(expected);
  });

  it('shows the stored status of a file that is not split', () => {
    const text = chipText({});
    expect(text).toContain('Completed');
    expect(text).not.toMatch(/parts/);
  });
});
