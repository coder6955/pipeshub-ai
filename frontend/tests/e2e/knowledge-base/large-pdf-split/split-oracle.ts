/**
 * What the large-PDF journey knows about the prepared stack: the two collections, the files in
 * them, the page ranges of the three parts, and the page oracle captured from an unsplit run.
 *
 * The part ranges mirror the split plan for a 1,138-page file at 500 pages a part file, overlap included
 * (a part owns 499 core pages and carries one overlap page of the next part as trailing context, never as hits).
 */
import * as fs from 'fs';
import * as path from 'path';

/** The collection holding the split 1,138-page file, and the one holding the unsplit 12-page file (may be the same). */
export const SPLIT_KB_NAME = process.env.E2E_SPLIT_KB_NAME ?? 'Large PDF split (verified)';
export const UNSPLIT_KB_NAME = process.env.E2E_UNSPLIT_KB_NAME ?? 'Large PDF split (fresh upload)';
/** The names the two files carry in the stack, and their page counts. */
export const LARGE_NAME = process.env.E2E_LARGE_FILE_NAME ?? 'large-manual';
export const LARGE_PAGE_COUNT = Number(process.env.E2E_LARGE_FILE_PAGES ?? 1138);
export const SHORT_NAME = process.env.E2E_SHORT_FILE_NAME ?? 'short-report';
export const SHORT_PAGE_COUNT = Number(process.env.E2E_SHORT_FILE_PAGES ?? 12);

export { PART_RANGES, partOfPage, type PartRange } from './split-plan';

export interface OracleHit {
  page: number;
  text: string;
}

export interface CrossPartQuestion {
  question: string;
  /** What the answer must draw on, for the failure message. */
  needs: string;
  /** The parts (1-based) the cited pages must fall in. */
  partsExpected: readonly number[];
}

interface OracleFile {
  queries: Record<string, OracleHit[]>;
  crossPart?: CrossPartQuestion[];
}

/**
 * The page oracle is not part of the repository: it quotes the document under test. Point `E2E_PAGE_ORACLE` at a JSON
 * file captured from an unsplit run of the same file, shaped `{ "queries": { "<query>": [{ "page": n, "text": "<start of
 * the passage>" }] }, "crossPart": [{ "question", "needs", "partsExpected" }] }` (see the README).
 */
function loadOracle(): OracleFile {
  const file = process.env.E2E_PAGE_ORACLE;
  if (!file) {
    throw new Error('E2E_PAGE_ORACLE must name the page-oracle JSON file for the large file under test (see tests/e2e/README.md).');
  }
  return JSON.parse(fs.readFileSync(path.resolve(file), 'utf-8')) as OracleFile;
}

const oracleFile = loadOracle();

/** Queries and the hits the unsplit file returned: original page and the start of the text. */
export const PAGE_ORACLE: Readonly<Record<string, readonly OracleHit[]>> = oracleFile.queries;

/** Questions whose answers sit in different parts, with what must be cited. */
export const CROSS_PART_QUESTIONS: readonly CrossPartQuestion[] = oracleFile.crossPart ?? [];

/** The records of the stack this journey reads, resolved by name at run time. */
export interface KnownRecord {
  id: string;
  name: string;
  virtualRecordId?: string;
  pageCount: number;
}
