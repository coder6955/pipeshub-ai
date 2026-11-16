/**
 * The no-part-id-leak scanner. It reads every API response the browser receives, including
 * the chat server-sent-event stream, and fails on anything that shows the split to a user:
 * part connector ids (`pdfparts_`), the split plan or page-offset fields, a record or virtual
 * record id that is not the file's own, and a block address that does not belong to the file.
 *
 * A split file's block address is `part * 2^20 + local block`, so the address itself is
 * expected on search and citation payloads. What must hold is that it decodes to a part the
 * file has, on a page that part holds. An unsplit file's addresses stay below 2^20.
 */
import type { BrowserContext, Page, Response } from '@playwright/test';
import { PART_RANGES, partOfPage } from './split-plan';

export const BLOCK_STRIDE = 2 ** 20;
/** One overlap page of the next part is carried as trailing context and may be cited. */

export interface Violation {
  rule: string;
  url: string;
  detail: string;
}

export interface ScanIdentity {
  /** Original file's record id and virtual record id, by display name. */
  files: Record<string, { recordId: string; virtualRecordId?: string; split: boolean; pages: number }>;
  /**
   * Every record and virtual record id a user can see (all collections of the account). An id
   * outside these sets is a hidden record, which for this feature means a part.
   */
  /** Known part record keys and part content vrids: any of them anywhere in a response is a leak. */
  partIds?: readonly string[];
  /** Ids of the collections whose records `visibleRecordIds` enumerates; files of other connectors are out of scope. */
  collectionIds?: readonly string[];
  visibleRecordIds?: readonly string[];
  visibleVirtualRecordIds?: readonly string[];
}

const RECORD_LINK = /\/record\/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})[^\s)"']*?#blockIndex=(\d+)/gi;

const FORBIDDEN_KEYS = new Set([
  'splitplan',
  'pageoffset',
  'partrecordkey',
  'partvrid',
  'partkey',
  'overlappages',
  'trailing_context',
  'trailingcontext',
  'pdfpart',
  'pdfparts',
  // The backend's INTERNAL_PAYLOAD_KEYS / INTERNAL_HIT_KEYS (modules/retrieval/hit_shape.py), lower-cased.
  'segmentvirtualrecordid',
  'segmentindex',
  'splitgen',
  'contentvrid',
  'blockaddress',
  'identity_vrid',
  'segment_index',
]);

/**
 * Responses that may go unread without failing a run: a body the browser discarded because the page
 * navigated or closed while it was being read. Anything else unread is a response nobody scanned.
 */
export const UNSCANNED_ALLOWED_REASONS: readonly RegExp[] = [
  /No resource with given identifier/i,
  /No data found for resource/i,
  /Target (page|closed)|has been closed/i,
];

const BLOCK_KEYS = new Set(['blocknum', 'blockindex', 'block_index']);
const ID_KEYS = new Set(['recordid', 'record_id']);
const VRID_KEYS = new Set(['virtualrecordid', 'virtual_record_id']);
const NAME_KEYS = new Set(['recordname', 'record_name']);

const PART_TEXT = [
  { rule: 'part-connector-id', re: /pdfparts_/i },
  { rule: 'part-label', re: /·\s*pages?\s*\d/i },
  // D-41: nothing the model sees names parts, so neither the part table nor a "Part k of N" header may reach a stream.
  { rule: 'part-table', re: /This document has \d+ parts?|\bParts?\s+\d+:\s*pages\s*\d+|\bPart \d+ of \d+\b/ },
  { rule: 'part-label', re: /\bpart\s+\d+\s+of\s+\d+\b.*\bpages?\s+\d+\s*[-–]\s*\d+/i },
];

function asNumbers(value: unknown): number[] {
  const list = Array.isArray(value) ? value : [value];
  return list.filter((v): v is number => typeof v === 'number' && Number.isFinite(v));
}

/**
 * What is wrong with a collection listing that is about to seed the visible-id allowlist. The allowlist is built from
 * these listings, so a part that leaked into one would whitelist itself: no row may look like a part or be a known part,
 * and the journey's own collections hold exactly its own files, once each.
 */
export function listingProblems(args: {
  collection: string;
  rowNames: readonly string[];
  rowIds: readonly string[];
  partIds: readonly string[];
  ownCollections: readonly string[];
  ownFiles: readonly string[];
  /** Files the journey itself uploads into its collections while it runs. */
  journeyUploads: readonly string[];
}): string[] {
  const out: string[] = [];
  for (const n of args.rowNames) if (/pdfparts_|·\s*pages?\s*\d|^part:/i.test(n)) out.push(`row "${n}" looks like a part`);
  for (const id of args.rowIds) if (args.partIds.includes(id)) out.push(`row id ${id} is a known part`);
  if (args.ownCollections.includes(args.collection)) {
    for (const n of args.rowNames) if (!args.ownFiles.includes(n) && !args.journeyUploads.includes(n)) out.push(`unexpected row "${n}"`);
    if (new Set(args.rowNames).size !== args.rowNames.length) out.push('a file is listed twice');
  }
  return out;
}

export class ResponseScanner {
  readonly violations: Violation[] = [];
  readonly scanned: { url: string; status: number; bytes: number; kind: 'json' | 'sse' | 'text' }[] = [];
  readonly unscanned: { url: string; reason: string }[] = [];
  /** Documents the viewer fetched: URL, content type, size. Kept so a test can prove which file was shown. */
  /** Chat streams (server-sent events) as received, kept as evidence. */
  readonly streams: { url: string; body: string }[] = [];
  readonly documents: { url: string; contentType: string; bytes: number }[] = [];
  private pending: Promise<void>[] = [];
  private readonly visibleIds: Set<string> | null;
  private readonly visibleVrids: Set<string> | null;

  constructor(private readonly identity: ScanIdentity) {
    this.visibleIds = identity.visibleRecordIds ? new Set(identity.visibleRecordIds) : null;
    this.visibleVrids = identity.visibleVirtualRecordIds ? new Set(identity.visibleVirtualRecordIds) : null;
  }

  /** A file the journey itself added mid-run is visible too. */
  allow(recordId: string, virtualRecordId?: string): void {
    this.visibleIds?.add(recordId);
    if (virtualRecordId) this.visibleVrids?.add(virtualRecordId);
  }

  /** Start scanning every API response made by pages of *target*. */
  attach(target: Page | BrowserContext): void {
    target.on('response', (response) => {
      this.pending.push(this.scanResponse(response));
    });
  }

  private async scanResponse(response: Response): Promise<void> {
    const url = response.url();
    const { pathname } = new URL(url);
    if (!/^\/(api|mcp|socket\.io)\b/.test(pathname) && !/\/api\//.test(pathname)) return;
    if (response.request().resourceType() === 'websocket') return;
    // Redirect and preflight responses have no body.
    if (response.status() >= 300 && response.status() < 400) return;
    // The URL itself must not carry a part id either (record preview and stream URLs).
    this.scanText(url, decodeURIComponent(url), 'url');
    const contentType = response.headers()['content-type'] ?? '';
    if (/pdf|octet-stream|image\//i.test(contentType)) {
      try {
        const bytes = (await response.body()).length;
        this.documents.push({ url, contentType, bytes });
        this.scanned.push({ url, status: response.status(), bytes, kind: 'text' });
      } catch (error) {
        this.unscanned.push({ url, reason: String(error).slice(0, 120) });
      }
      return;
    }
    let text: string;
    try {
      text = await response.text();
    } catch (error) {
      this.unscanned.push({ url, reason: String(error).slice(0, 120) });
      return;
    }
    this.record(url, response.status(), text);
  }

  /** Scan a body already in hand (used for the API sweeps and the self-test). */
  record(url: string, status: number, text: string): void {
    const sse = /^\s*(event|data):/m.test(text);
    this.scanned.push({ url, status, bytes: text.length, kind: sse ? 'sse' : text.trimStart().startsWith('{') || text.trimStart().startsWith('[') ? 'json' : 'text' });
    this.scanText(url, text, 'body');
    if (sse) {
      this.streams.push({ url, body: text });
      for (const line of text.split('\n')) {
        if (!line.startsWith('data:')) continue;
        const payload = line.slice(5).trim();
        if (payload.startsWith('{') || payload.startsWith('[')) this.tryJson(url, payload);
      }
    } else if (text.trimStart().startsWith('{') || text.trimStart().startsWith('[')) {
      this.tryJson(url, text);
    }
  }

  private tryJson(url: string, text: string): void {
    try {
      this.walk(url, JSON.parse(text), undefined);
    } catch {
      // A payload that is not JSON is covered by the text rules.
    }
  }

  /** Text rules: part ids and part labels, in a URL, body or the page's visible text. */
  scanText(url: string, text: string, where: string): void {
    for (const link of text.matchAll(RECORD_LINK)) {
      const at = link.index ?? 0;
      this.checkLink(url, link[1], Number(link[2]), text.slice(Math.max(0, at - 160), at + 40).replace(/\s+/g, ' '));
    }
    for (const id of this.identity.partIds ?? []) {
      if (text.includes(id)) this.violations.push({ rule: 'part-id', url, detail: `${where}: contains part id ${id}` });
    }
    for (const { rule, re } of PART_TEXT) {
      const m = re.exec(text);
      if (m) this.violations.push({ rule, url, detail: `${where}: …${text.slice(Math.max(0, m.index - 40), m.index + 80).replace(/\s+/g, ' ')}…` });
    }
  }

  /** A citation link `/record/<id>/preview#blockIndex=<address>`: the id must be a visible file and the address one of its own. */
  private checkLink(url: string, recordId: string, address: number, context = ''): void {
    if (this.visibleIds && !this.visibleIds.has(recordId)) {
      this.violations.push({ rule: 'hidden-record-id', url, detail: `link to record ${recordId}, which is not a file the user can see (blockIndex=${address})` });
      return;
    }
    const owner = Object.values(this.identity.files).find((x) => x.recordId === recordId);
    if (!owner) return;
    // Address 0..stride-1 on a split file is the file's own manifest (its part table); k >= 1 is part k.
    const part = Math.floor(address / BLOCK_STRIDE);
    if (owner.split ? part !== 0 && !PART_RANGES.some((r) => r.part === part) : part !== 0) {
      this.violations.push({ rule: 'block-resolves-elsewhere', url, detail: `link blockIndex=${address} decodes to part ${part}, which is not a part of ${recordId}; context: …${context}…` });
    }
  }

  private walk(url: string, node: unknown, parentKey: string | undefined): void {
    if (Array.isArray(node)) {
      for (const item of node) this.walk(url, item, parentKey);
      return;
    }
    if (!node || typeof node !== 'object') return;
    const obj = node as Record<string, unknown>;
    const lower = new Map<string, unknown>();
    for (const [k, v] of Object.entries(obj)) {
      lower.set(k.toLowerCase(), v);
      if (FORBIDDEN_KEYS.has(k.toLowerCase())) {
        this.violations.push({ rule: 'split-internals', url, detail: `field "${k}" is in a response` });
      }
    }
    this.checkIdentity(url, lower);
    for (const [k, v] of Object.entries(obj)) this.walk(url, v, k);
  }

  /** A record id, virtual id or block address on this object that does not belong to its file. */
  private checkIdentity(url: string, f: Map<string, unknown>): void {
    const name = [...NAME_KEYS].map((k) => f.get(k)).find((v) => typeof v === 'string') as string | undefined;
    const recordId = [...ID_KEYS].map((k) => f.get(k)).find((v) => typeof v === 'string') as string | undefined;
    const vrid = [...VRID_KEYS].map((k) => f.get(k)).find((v) => typeof v === 'string') as string | undefined;
    // Other collections hold files with the same names (the `main` baseline), so identity is by id:
    // an object is one of this journey's files when its recordId is, and a foreign id is a leak only
    // when it is not a file the user can see.
    const connectorId = f.get('connectorid');
    // Only files that belong to the enumerated collections can be judged hidden; other connectors' files are not listed.
    const inScope = !this.identity.collectionIds || typeof connectorId !== 'string' || this.identity.collectionIds.includes(connectorId);
    const byId = recordId ? Object.values(this.identity.files).find((x) => x.recordId === recordId) : undefined;
    const file = byId ?? (!recordId && name ? this.identity.files[name] : undefined);
    if (inScope && recordId && !byId && this.visibleIds && !this.visibleIds.has(recordId)) {
      this.violations.push({ rule: 'hidden-record-id', url, detail: `${name ?? 'record'}: recordId ${recordId} is not a file the user can see` });
    }
    if (inScope && vrid && this.visibleVrids && !this.visibleVrids.has(vrid)) {
      this.violations.push({ rule: 'hidden-virtual-record-id', url, detail: `${name ?? 'record'}: virtualRecordId ${vrid} is not one of the visible files'` });
    }
    if (byId && vrid && file?.virtualRecordId && vrid !== file.virtualRecordId) {
      this.violations.push({ rule: 'foreign-virtual-record-id', url, detail: `${name}: virtualRecordId ${vrid} is not ${file.virtualRecordId}` });
    }
    const blocks = [...BLOCK_KEYS].flatMap((k) => asNumbers(f.get(k)));
    const owner = file;
    if (!owner) return;
    // Pages are checked whether or not the object carries block numbers: page-only metadata can leak too.
    const pages = asNumbers(f.get('pagenum') ?? f.get('page_num') ?? f.get('pagenumber'));
    // Paragraph numbers in payload metadata are display ordinals: a block address
    // (part << 20 | local) must never be shown. Addresses live only in citation links,
    // which `scanLinks` decodes and checks against the file's parts.
    for (const num of blocks) {
      if (num >= BLOCK_STRIDE) {
        this.violations.push({ rule: 'block-address-displayed', url, detail: `${name ?? 'record'}: paragraph number ${num} is a block address, not a file-wide ordinal` });
      }
    }
    for (const page of pages) {
      if (page < 1 || page > owner.pages || (owner.split && partOfPage(page) === null)) {
        this.violations.push({ rule: 'page-out-of-file', url, detail: `page ${page} is outside 1-${owner.pages}` });
      }
    }
  }

  /** Unread responses that are not an explained navigation discard: each is a body the scanner never saw. */
  unexpectedUnscanned(): { url: string; reason: string }[] {
    return this.unscanned.filter((u) => !UNSCANNED_ALLOWED_REASONS.some((re) => re.test(u.reason)));
  }

  /** Wait for every in-flight body read. */
  async settle(): Promise<void> {
    while (this.pending.length) {
      const batch = this.pending;
      this.pending = [];
      await Promise.all(batch);
    }
  }

  summary(): string {
    const uniq = new Map<string, number>();
    for (const s of this.scanned) {
      const path = s.url.replace(/^https?:\/\/[^/]+/, '').split('?')[0];
      uniq.set(path, (uniq.get(path) ?? 0) + 1);
    }
    return `${this.scanned.length} responses (${uniq.size} endpoints), ${this.unscanned.length} unreadable, ${this.violations.length} violations`;
  }
}
