import { describe, expect, it } from 'vitest';
import { buildStageTrackSteps } from '../stage-track-steps';

const t = (key: string) => key;
const base = { parsingStatus: 'COMPLETED', indexingStatus: 'COMPLETED', extractionStatus: 'COMPLETED' } as const;
const searchable = (extra: Record<string, unknown>) =>
  buildStageTrackSteps({ ...base, ...extra } as never, [], t).find((s) => s.key === 'searchable')?.status;

describe('stage track of a split record', () => {
  it('shows Searchable as in progress while parts are indexing and as partly indexed when some parts failed', () => {
    expect(searchable({ splitState: 'INDEXING_PARTS', splitDone: 1, splitTotal: 4 })).toBe('IN_PROGRESS');
    expect(searchable({ splitState: 'SPLITTING', splitDone: 0, splitTotal: 4 })).toBe('IN_PROGRESS');
    expect(searchable({ splitState: 'PARTIAL', splitDone: 4, splitTotal: 4, splitFailed: 1 })).toBe('PARTLY_INDEXED');
    expect(searchable({ splitState: 'FAILED', splitDone: 4, splitTotal: 4, splitFailed: 4 })).toBe('FAILED');
  });

  it('is unchanged when completed, absent or malformed', () => {
    expect(searchable({ splitState: 'COMPLETED', splitDone: 4, splitTotal: 4 })).toBe('COMPLETED');
    expect(searchable({})).toBe('COMPLETED');
    expect(searchable({ splitState: 'INDEXING_PARTS', splitDone: 1, splitTotal: 0 })).toBe('COMPLETED');
  });
});
