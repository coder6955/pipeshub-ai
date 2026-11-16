// Roll-up of a large file that is indexed in pieces. The API sends the four fields only for such a
// file; every other record has none, so every function here returns "no split" for it.

export type SplitState = 'SPLITTING' | 'INDEXING_PARTS' | 'COMPLETED' | 'PARTIAL' | 'FAILED';

const SPLIT_STATES: readonly string[] = ['SPLITTING', 'INDEXING_PARTS', 'COMPLETED', 'PARTIAL', 'FAILED'];

/** Display-only status of a split file with some failed parts: searchable, so a warning rather than an error. */
export const PARTLY_INDEXED_STATUS = 'PARTLY_INDEXED';

export interface SplitFields {
  splitState?: SplitState | string | null;
  splitDone?: number | null;
  splitTotal?: number | null;
  splitFailed?: number | null;
}

export interface SplitProgress {
  state: SplitState;
  done: number;
  total: number;
  failed: number;
}

/** Just the four split fields of a record or row, for passing on without the rest. */
export function splitFieldsOf(item: SplitFields | null | undefined): SplitFields {
  return {
    splitState: item?.splitState,
    splitDone: item?.splitDone,
    splitTotal: item?.splitTotal,
    splitFailed: item?.splitFailed,
  };
}

function wholeCount(value: unknown): number | null {
  return typeof value === 'number' && Number.isFinite(value) && value >= 0 ? Math.floor(value) : null;
}

/** The split roll-up of a record, or null when it has none or the API sent nothing usable. */
export function splitProgressOf(item: SplitFields): SplitProgress | null {
  const { splitState } = item;
  if (typeof splitState !== 'string' || !SPLIT_STATES.includes(splitState)) return null;
  const total = wholeCount(item.splitTotal);
  const done = wholeCount(item.splitDone);
  if (total === null || total < 1 || done === null) return null;
  const failed = wholeCount(item.splitFailed) ?? 0;
  return {
    state: splitState as SplitState,
    done: Math.min(done, total),
    total,
    failed: Math.min(failed, total),
  };
}

/** Whether the file's split roll-up is still running (its parts are being prepared or indexed). */
export function isSplitRunning(progress: SplitProgress | null): boolean {
  return progress?.state === 'SPLITTING' || progress?.state === 'INDEXING_PARTS';
}

/** Whether the file has parts that failed, so "retry failed parts" is the useful action. */
export function isSplitRetryable(progress: SplitProgress | null): boolean {
  return progress?.state === 'PARTIAL' || progress?.state === 'FAILED';
}

/**
 * The indexing status to show for a split file: the parent record is COMPLETED once its manifest is
 * indexed, so a running or failed roll-up overrides it. Any other status is returned as it is.
 */
export function splitDisplayStatus(status: string | null | undefined, progress: SplitProgress | null): string | null | undefined {
  if (isSplitRunning(progress)) return 'IN_PROGRESS';
  if (progress?.state === 'PARTIAL') return PARTLY_INDEXED_STATUS;
  if (progress?.state === 'FAILED') return 'FAILED';
  return status;
}

type Translate = (key: string, options?: Record<string, unknown>) => string;

/** The roll-up in words, or null when the normal status label applies (no split, or it completed). */
export function splitStatusLabel(progress: SplitProgress | null, t: Translate, detailed = true): string | null {
  if (!progress || progress.state === 'COMPLETED') return null;
  switch (progress.state) {
    case 'SPLITTING':
      return t('kb.indexingStatus.splitPreparing');
    case 'INDEXING_PARTS':
      return detailed
        ? t('kb.indexingStatus.splitIndexing', { count: progress.total, done: progress.done, total: progress.total })
        : t('kb.indexingStatus.inProgress');
    case 'PARTIAL': {
      // The short label says the file is mostly indexed; so does a roll-up with no usable failed count.
      const failed = progress.failed;
      return detailed && failed > 0
        ? t('kb.indexingStatus.splitPartial', { count: progress.total, failed, total: progress.total })
        : t('kb.indexingStatus.partlyIndexed');
    }
    case 'FAILED':
    default:
      return t('kb.indexingStatus.failed');
  }
}
