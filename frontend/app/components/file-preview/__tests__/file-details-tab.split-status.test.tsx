import React from 'react';
import { afterEach, describe, expect, it } from 'vitest';
import { cleanup, render } from '@testing-library/react';
import { Theme } from '@radix-ui/themes';
import '@/lib/__tests__/test-i18n';
import type { RecordDetailsResponse } from '@/app/(main)/knowledge-base/types';
import { FileDetailsTab } from '../file-details-tab';

afterEach(cleanup);

const statusRow = (fields: Record<string, unknown>) => {
  const recordDetails = {
    record: { id: 'r1', recordName: 'big.pdf', recordType: 'FILE', origin: 'UPLOAD', indexingStatus: 'COMPLETED', ...fields },
    knowledgeBase: null,
    folder: null,
    metadata: { departments: [], categories: [], subcategories1: [], subcategories2: [], subcategories3: [], topics: [], languages: [] },
    permissions: [],
  } as unknown as RecordDetailsResponse;
  const { getByText } = render(
    <Theme>
      <FileDetailsTab recordDetails={recordDetails} />
    </Theme>,
  );
  return getByText('Indexing status').parentElement!.textContent ?? '';
};

describe('the indexing status of the file-preview details tab', () => {
  it.each([
    [{ splitState: 'INDEXING_PARTS', splitDone: 1, splitTotal: 4 }, 'Indexing 1 of 4 parts'],
    [{ splitState: 'PARTIAL', splitDone: 4, splitTotal: 4, splitFailed: 1 }, '1 of 4 parts failed'],
    [{ splitState: 'FAILED', splitDone: 4, splitTotal: 4, splitFailed: 4 }, 'Failed'],
  ])('shows the roll-up of a split file (%o), not its stored COMPLETED', (fields, expected) => {
    const text = statusRow(fields);
    expect(text).toContain(expected);
    expect(text).not.toContain('COMPLETED');
  });

  it('shows the stored status of a file that is not split, as before', () => {
    expect(statusRow({})).toContain('COMPLETED');
  });
});
