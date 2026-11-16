/**
 * The split plan of the file the journey reads: the page ranges of its three parts. They mirror the plan for a
 * 1,138-page file at 500 pages a part file, overlap included (a part owns 499 core pages and also carries one
 * overlap page of the next part as trailing context, never as hits). No environment is read here, so the scanner's unit tests can import it.
 */
export interface PartRange {
  part: number;
  first: number;
  last: number;
}

export const PART_RANGES: readonly PartRange[] = [
  { part: 1, first: 1, last: 499 },
  { part: 2, first: 500, last: 998 },
  { part: 3, first: 999, last: 1138 },
];

/** The part (1-based) that owns an original page, or null when the page is outside the file. */
export function partOfPage(page: number): number | null {
  return PART_RANGES.find((r) => page >= r.first && page <= r.last)?.part ?? null;
}
