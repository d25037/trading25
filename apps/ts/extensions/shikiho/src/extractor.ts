import type { ShikihoQuoteV1, ShikihoSnapshotV1, ShikihoTraceField } from './contract';
import { normalizeShikihoCode } from './contract';

export type ShikihoExtractionResult =
  | { kind: 'success'; snapshot: ShikihoSnapshotV1 }
  | { kind: 'login_required' | 'page_changed'; code: string };

export interface ShikihoPageInspection {
  result: ShikihoExtractionResult;
  fields: ShikihoTraceField[];
  candidate: ShikihoSnapshotV1 | null;
}

const SECTION_SELECTOR = 'section, article, [role="region"], table, dl';
const COMMENTARY_PATTERN = /^【([^】]+)】\s*(.+)$/;
const QUOTE_TIMESTAMP_PATTERN = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/;

interface InspectionMemo {
  visibility: WeakMap<Element, boolean>;
  visibleText: WeakMap<Element, string>;
}

let activeInspectionMemo: InspectionMemo | null = null;

function memoizeVisibility(elements: Element[], visible: boolean): boolean {
  const visibility = activeInspectionMemo?.visibility;
  if (visibility) {
    for (const element of elements) visibility.set(element, visible);
  }
  return visible;
}

function isExplicitlyHidden(element: Element): boolean {
  if (
    element.tagName.toLowerCase() === 'template' ||
    element.hasAttribute('hidden') ||
    element.getAttribute('aria-hidden')?.toLowerCase() === 'true'
  ) {
    return true;
  }
  const style = element.ownerDocument.defaultView?.getComputedStyle(element);
  return style?.display === 'none' || style?.visibility === 'hidden';
}

export function normalizeText(value: string | null | undefined): string {
  return (value ?? '')
    .replace(/[\u200b-\u200d\ufeff]/g, '')
    .replace(/\s+/g, ' ')
    .trim();
}

export function isElementVisible(element: Element): boolean {
  const cached = activeInspectionMemo?.visibility.get(element);
  if (cached !== undefined) return cached;
  const visited: Element[] = [];
  let current: Element | null = element;
  while (current !== null) {
    const ancestorVisibility = activeInspectionMemo?.visibility.get(current);
    if (ancestorVisibility !== undefined) {
      return memoizeVisibility(visited, ancestorVisibility);
    }
    visited.push(current);
    if (isExplicitlyHidden(current)) return memoizeVisibility(visited, false);
    current = current.parentElement;
  }
  return memoizeVisibility(visited, true);
}

function visibleText(element: Element): string {
  const cached = activeInspectionMemo?.visibleText.get(element);
  if (cached !== undefined) return cached;
  if (!isElementVisible(element)) {
    activeInspectionMemo?.visibleText.set(element, '');
    return '';
  }
  const values: string[] = [];
  for (const child of element.childNodes) {
    if (child.nodeType === 3) {
      values.push(child.nodeValue ?? '');
    } else if (child.nodeType === 1) {
      values.push(visibleText(child as Element));
    }
  }
  const result = normalizeText(values.join(''));
  activeInspectionMemo?.visibleText.set(element, result);
  return result;
}

export function findExactLabel(root: ParentNode, label: string): Element | null {
  const normalizedLabel = normalizeText(label);
  const selectors = ['dt, th, h1, h2, h3, h4, h5, h6, legend', '*'];
  for (const [index, selector] of selectors.entries()) {
    for (const element of root.querySelectorAll(selector)) {
      if (!isElementVisible(element) || visibleText(element) !== normalizedLabel) continue;
      if (index === 0) return element;
      const childRepeatsLabel = Array.from(element.children).some((child) => visibleText(child) === normalizedLabel);
      if (!childRepeatsLabel) return element;
    }
  }
  return null;
}

function findSection(label: Element): Element {
  return label.closest(SECTION_SELECTOR) ?? label.parentElement ?? label;
}

function labelValueElement(label: Element): Element | null {
  const value = label.nextElementSibling;
  return value !== null && isElementVisible(value) ? value : null;
}

export function extractLabelValue(root: ParentNode, label: string): string | null {
  const labelElement = findExactLabel(root, label);
  if (labelElement === null) return null;

  const valueElement = labelValueElement(labelElement);
  const directValue = valueElement === null ? '' : visibleText(valueElement);
  if (directValue !== '') return directValue;

  const section = findSection(labelElement);
  const values = Array.from(section.children)
    .filter((child) => child !== labelElement)
    .filter(isElementVisible)
    .map(visibleText)
    .filter(Boolean);
  return values.length === 0 ? null : values.join(' ');
}

export function extractStockLinks(root: ParentNode): Array<{ code: string | null; name: string }> {
  const companies: Array<{ code: string | null; name: string }> = [];
  const seen = new Set<string>();

  for (const link of root.querySelectorAll('a[href]')) {
    if (!isElementVisible(link)) continue;
    const href = link.getAttribute('href') ?? '';
    const rawCode = /(?:^|\/)stocks\/(\d[0-9A-Z]\d[0-9A-Z]0?)(?:$|[/?#])/i.exec(href)?.[1];
    const code = normalizeShikihoCode(rawCode);
    const name = visibleText(link)
      .replace(/^\d[0-9A-Z]\d[0-9A-Z]0?\s*[：:\-－]?\s*/i, '')
      .trim();
    if (name === '') continue;

    const key = `${code ?? ''}\u0000${name}`;
    if (seen.has(key)) continue;
    seen.add(key);
    companies.push({ code, name });
  }
  return companies;
}

export function parseScore(value: string | null): number | null {
  const normalized = normalizeText(value);
  return /^[0-5]$/.test(normalized) ? Number(normalized) : null;
}

function canonicalize(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonicalize);
  if (value === null || typeof value !== 'object') return value;
  return Object.fromEntries(
    Object.entries(value as Record<string, unknown>)
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([key, nested]) => [key, canonicalize(nested)])
  );
}

function rotateRight(value: number, amount: number): number {
  return (value >>> amount) | (value << (32 - amount));
}

// biome-ignore lint/complexity/noExcessiveCognitiveComplexity: SHA-256 compression is kept local and dependency-free for browser use.
function sha256(value: string): string {
  const constants = [
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5, 0xd807aa98,
    0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786,
    0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8,
    0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13,
    0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819,
    0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a,
    0x5b9cca4f, 0x682e6ff3, 0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
    0xc67178f2,
  ];
  const hash = [0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19];
  const input = new TextEncoder().encode(value);
  const bitLength = input.length * 8;
  const paddedLength = Math.ceil((input.length + 9) / 64) * 64;
  const bytes = new Uint8Array(paddedLength);
  bytes.set(input);
  bytes[input.length] = 0x80;
  const view = new DataView(bytes.buffer);
  view.setUint32(paddedLength - 8, Math.floor(bitLength / 0x1_0000_0000));
  view.setUint32(paddedLength - 4, bitLength >>> 0);

  const words = new Uint32Array(64);
  for (let offset = 0; offset < bytes.length; offset += 64) {
    for (let index = 0; index < 16; index += 1) words[index] = view.getUint32(offset + index * 4);
    for (let index = 16; index < 64; index += 1) {
      const word15 = words[index - 15] ?? 0;
      const word2 = words[index - 2] ?? 0;
      const sigma0 = rotateRight(word15, 7) ^ rotateRight(word15, 18) ^ (word15 >>> 3);
      const sigma1 = rotateRight(word2, 17) ^ rotateRight(word2, 19) ^ (word2 >>> 10);
      words[index] = ((words[index - 16] ?? 0) + sigma0 + (words[index - 7] ?? 0) + sigma1) >>> 0;
    }

    let [a, b, c, d, e, f, g, h] = hash as [number, number, number, number, number, number, number, number];
    for (let index = 0; index < 64; index += 1) {
      const sum1 = rotateRight(e, 6) ^ rotateRight(e, 11) ^ rotateRight(e, 25);
      const choose = (e & f) ^ (~e & g);
      const temp1 = (h + sum1 + choose + (constants[index] ?? 0) + (words[index] ?? 0)) >>> 0;
      const sum0 = rotateRight(a, 2) ^ rotateRight(a, 13) ^ rotateRight(a, 22);
      const majority = (a & b) ^ (a & c) ^ (b & c);
      const temp2 = (sum0 + majority) >>> 0;
      h = g;
      g = f;
      f = e;
      e = (d + temp1) >>> 0;
      d = c;
      c = b;
      b = a;
      a = (temp1 + temp2) >>> 0;
    }
    const next = [a, b, c, d, e, f, g, h];
    for (let index = 0; index < hash.length; index += 1) hash[index] = ((hash[index] ?? 0) + (next[index] ?? 0)) >>> 0;
  }

  return hash.map((word) => word.toString(16).padStart(8, '0')).join('');
}

function computeContentHashSync(snapshotWithoutCaptureTime: unknown): string {
  return `sha256:${sha256(JSON.stringify(canonicalize(snapshotWithoutCaptureTime)))}`;
}

export function computeContentHash(snapshotWithoutCaptureTime: unknown): Promise<string> {
  return Promise.resolve(computeContentHashSync(snapshotWithoutCaptureTime));
}

function isLoginRequired(document: Document): boolean {
  const pageText = document.body === null ? '' : visibleText(document.body);
  const hasVisiblePassword = Array.from(document.querySelectorAll('input[type="password"]')).some(isElementVisible);
  return (
    /ログインして.*閲覧/.test(pageText) ||
    /ログインが必要/.test(pageText) ||
    /会員ログイン/.test(pageText) ||
    /ベーシック・プレミアムプランでは、記事本文など、すべての情報が閲覧できます/.test(pageText) ||
    hasVisiblePassword
  );
}

function extractIdentity(document: Document, code: string): { companyName: string } | null {
  const heading = Array.from(document.querySelectorAll('h1, [itemprop="name"]')).find((candidate) => {
    if (!isElementVisible(candidate)) return false;
    const text = visibleText(candidate);
    return text !== '' && text !== 'ログイン';
  });
  if (heading === undefined) return null;

  const headingText = visibleText(heading);
  const companyName = normalizeText(headingText.replace(new RegExp(`(^|\\s)${code}(?=\\s|$)`), ' '));
  const identityRoot = heading.closest('header, main, article') ?? document;
  const hasMatchingCode =
    new RegExp(`(^|\\s)${code}(?=\\s|$)`).test(headingText) || findExactLabel(identityRoot, code) !== null;
  if (!hasMatchingCode || companyName === '') return null;
  return { companyName };
}

function extractLabelledCommentary(document: Document): ShikihoSnapshotV1['commentary'] {
  const commentary: ShikihoSnapshotV1['commentary'] = [];
  const label = findExactLabel(document, '会社四季報');
  if (label === null) return commentary;
  const candidates = findSection(label).querySelectorAll('p, li, dd, div');
  for (const candidate of candidates) {
    if (!isElementVisible(candidate)) continue;
    if (candidate.tagName.toLowerCase() === 'div' && candidate.querySelector('p, li, dd') !== null) {
      continue;
    }
    const match = COMMENTARY_PATTERN.exec(visibleText(candidate));
    if (match === null) continue;
    commentary.push({ heading: normalizeText(match[1]), body: normalizeText(match[2]) });
  }
  return commentary;
}

function extractTableCommentary(document: Document): ShikihoSnapshotV1['commentary'] {
  const commentary: ShikihoSnapshotV1['commentary'] = [];
  for (const row of document.querySelectorAll('table tr')) {
    if (!isElementVisible(row)) continue;
    const headingCell = Array.from(row.children).find((child) => child.tagName.toLowerCase() === 'th');
    const bodyCell = Array.from(row.children).find((child) => child.tagName.toLowerCase() === 'td');
    if (
      headingCell === undefined ||
      bodyCell === undefined ||
      !isElementVisible(headingCell) ||
      !isElementVisible(bodyCell)
    ) {
      continue;
    }
    const headingMatch = /^【([^】]+)】$/.exec(visibleText(headingCell));
    const body = visibleText(bodyCell);
    if (headingMatch === null || body === '') continue;
    commentary.push({ heading: normalizeText(headingMatch[1]), body });
  }
  return commentary;
}

function extractCommentary(document: Document): ShikihoSnapshotV1['commentary'] {
  const labelled = extractLabelledCommentary(document);
  return labelled.length > 0 ? labelled : extractTableCommentary(document);
}

function extractEditionLabel(document: Document): string | null {
  const labelled = extractLabelValue(document, '掲載号');
  if (labelled !== null) return labelled;
  for (const paragraph of document.querySelectorAll('p')) {
    if (!isElementVisible(paragraph)) continue;
    const text = visibleText(paragraph);
    if (/^\d{4}年\d+集[^\s（）]+号（\d{4}年\d{1,2}月\d{1,2}日発売）$/.test(text)) return text;
  }
  return null;
}

function extractEarningsAnnouncementDate(document: Document): string | null {
  const liveDateElement = Array.from(document.querySelectorAll('.planned-disclosure-date .date')).find(
    isElementVisible
  );
  const value =
    liveDateElement === undefined ? extractLabelValue(document, '決算発表予定日') : visibleText(liveDateElement);
  const match = /^(\d{4})\/(\d{2})\/(\d{2})$/.exec(normalizeText(value));
  if (match === null) return null;
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  const leapYear = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
  const daysInMonth = [31, leapYear ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
  if (month < 1 || month > 12 || day < 1 || day > (daysInMonth[month - 1] ?? 0)) return null;
  return `${match[1]}-${match[2]}-${match[3]}`;
}

function extractSectionList(document: Document, label: string): string[] | null {
  const labelElement = findExactLabel(document, label);
  if (labelElement === null) return null;
  const section = findSection(labelElement);
  const itemElements = Array.from(section.querySelectorAll('li'));
  const source: Element[] = itemElements.length > 0 ? itemElements : Array.from(section.querySelectorAll('a'));
  const values = source
    .filter(isElementVisible)
    .map(visibleText)
    .filter((item) => item !== '' && item !== label);
  return Array.from(new Set(values));
}

function extractComparisonCompanies(document: Document): ShikihoSnapshotV1['comparisonCompanies'] | null {
  const label = findExactLabel(document, '比較会社');
  if (label === null) return null;
  return extractStockLinks(findSection(label));
}

function extractProfile(document: Document): ShikihoSnapshotV1['profile'] | null {
  const label = findExactLabel(document, '会社概要');
  if (label === null) return null;
  const section = findSection(label);
  const profile: ShikihoSnapshotV1['profile'] = [];
  for (const term of section.querySelectorAll('dt')) {
    if (!isElementVisible(term)) continue;
    const value = term.nextElementSibling;
    if (value?.tagName.toLowerCase() !== 'dd' || !isElementVisible(value)) continue;
    const normalizedLabel = visibleText(term);
    const normalizedValue = visibleText(value);
    if (normalizedLabel !== '' && normalizedValue !== '')
      profile.push({ label: normalizedLabel, value: normalizedValue });
  }
  return profile;
}

function extractDateTime(document: Document, label: string): string | null {
  const labelElement = findExactLabel(document, label);
  if (labelElement === null) return null;
  const valueElement = labelValueElement(labelElement) ?? findSection(labelElement);
  const time = Array.from(valueElement.querySelectorAll('time[datetime]')).find(isElementVisible);
  const dateTime = time?.getAttribute('datetime');
  return dateTime !== null && dateTime !== undefined && !Number.isNaN(Date.parse(dateTime)) ? dateTime : null;
}

function extractScore(document: Document): { score: ShikihoSnapshotV1['score']; present: boolean } {
  const label = findExactLabel(document, '四季報スコア');
  const emptyScore: ShikihoSnapshotV1['score'] = {
    overall: null,
    growth: null,
    profitability: null,
    safety: null,
    scale: null,
    value: null,
    priceMomentum: null,
  };
  if (label === null) return { score: emptyScore, present: false };
  const section = label.closest('.score') ?? findSection(label);
  const detailScore = (detailLabel: string): number | null => {
    const term = Array.from(section.querySelectorAll('dt')).find(
      (candidate) => isElementVisible(candidate) && visibleText(candidate) === detailLabel
    );
    const value = term?.nextElementSibling;
    if (value?.tagName.toLowerCase() !== 'dd' || !isElementVisible(value)) return null;
    return parseScore(visibleText(value));
  };
  const adjacentOverallValue = labelValueElement(label);
  const overallValue =
    (adjacentOverallValue !== null && parseScore(visibleText(adjacentOverallValue)) !== null
      ? adjacentOverallValue
      : null) ??
    Array.from(label.parentElement?.children ?? []).find(
      (candidate) => candidate !== label && isElementVisible(candidate) && parseScore(visibleText(candidate)) !== null
    ) ??
    null;
  const score: ShikihoSnapshotV1['score'] = {
    overall: parseScore(overallValue === null ? null : visibleText(overallValue)) ?? detailScore('総合'),
    growth: detailScore('成長性'),
    profitability: detailScore('収益性'),
    safety: detailScore('安全性'),
    scale: detailScore('規模'),
    value: detailScore('割安度'),
    priceMomentum: detailScore('値上がり'),
  };
  return {
    present: Object.values(score).some((value) => value !== null),
    score,
  };
}

function exactAdjacentValue(section: Element, label: string): Element | null {
  const term = Array.from(section.querySelectorAll('dt')).find(
    (candidate) => isElementVisible(candidate) && visibleText(candidate) === label
  );
  const value = term?.nextElementSibling;
  return value?.tagName.toLowerCase() === 'dd' && isElementVisible(value) ? value : null;
}

function visibleTextWithoutTime(element: Element): string {
  const values: string[] = [];
  for (const child of element.childNodes) {
    if (child.nodeType === 3) values.push(child.nodeValue ?? '');
    else if (child.nodeType === 1 && (child as Element).tagName.toLowerCase() !== 'time') {
      values.push(visibleText(child as Element));
    }
  }
  return normalizeText(values.join(''));
}

function parseQuoteNumber(value: Element | null, allowZero = false): number | null {
  if (value === null) return null;
  const normalized = visibleTextWithoutTime(value);
  if (!/^(?:\d{1,3}(?:,\d{3})*|\d+)(?:\.\d+)?$/.test(normalized)) return null;
  const parsed = Number(normalized.replaceAll(',', ''));
  if (!Number.isFinite(parsed) || (allowZero ? parsed < 0 : parsed <= 0)) return null;
  return parsed;
}

function extractQuoteTime(value: Element | null): string | null {
  const time = value === null ? null : Array.from(value.querySelectorAll('time[datetime]')).find(isElementVisible);
  const dateTime = time?.getAttribute('datetime') ?? null;
  return dateTime !== null && /^(?:[01]\d|2[0-3]):[0-5]\d$/.test(dateTime) ? dateTime : null;
}

function isExactCalendarDate(value: string): boolean {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) return false;
  const date = new Date(`${value}T00:00:00Z`);
  return !Number.isNaN(date.getTime()) && date.toISOString().slice(0, 10) === value;
}

function extractQuote(document: Document): ShikihoQuoteV1 | undefined {
  const label = findExactLabel(document, '株価');
  if (label === null) return undefined;
  const section = findSection(label);
  if (findExactLabel(section, '会社四季報オンライン') === null || findExactLabel(section, '15分遅れ') === null) {
    return undefined;
  }
  const updateTime = Array.from(section.querySelectorAll('time[datetime]')).find((candidate) => {
    const dateTime = candidate.getAttribute('datetime') ?? '';
    return isElementVisible(candidate) && dateTime.includes('T');
  });
  const observedAt = updateTime?.getAttribute('datetime') ?? '';
  const tradingDate = observedAt.slice(0, 10);
  if (
    !isExactCalendarDate(tradingDate) ||
    !observedAt.startsWith(`${tradingDate}T`) ||
    !QUOTE_TIMESTAMP_PATTERN.test(observedAt) ||
    Number.isNaN(Date.parse(observedAt))
  ) {
    return undefined;
  }

  const currentPrice = parseQuoteNumber(exactAdjacentValue(section, '現在値'));
  const openValue = exactAdjacentValue(section, '始値');
  const highValue = exactAdjacentValue(section, '高値');
  const lowValue = exactAdjacentValue(section, '安値');
  const open = parseQuoteNumber(openValue);
  const high = parseQuoteNumber(highValue);
  const low = parseQuoteNumber(lowValue);
  const previousClose = parseQuoteNumber(exactAdjacentValue(section, '前日終値'));
  const volumeValue = exactAdjacentValue(section, '出来高');
  const volume = volumeValue === null ? null : parseQuoteNumber(volumeValue, true);
  if (
    currentPrice === null ||
    open === null ||
    high === null ||
    low === null ||
    previousClose === null ||
    (volumeValue !== null && volume === null) ||
    currentPrice < low ||
    currentPrice > high ||
    open < low ||
    open > high
  ) {
    return undefined;
  }
  return {
    tradingDate,
    observedAt,
    delayMinutes: 15,
    currentPrice,
    open,
    high,
    low,
    previousClose,
    volume,
    openTime: extractQuoteTime(openValue),
    highTime: extractQuoteTime(highValue),
    lowTime: extractQuoteTime(lowValue),
    sourceLabel: '会社四季報オンライン',
  };
}

function parseLiveQuoteNumber(value: Element | null, suffix = ''): number | null {
  if (value === null) return null;
  const normalized = visibleText(value);
  const escapedSuffix = suffix.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const match = new RegExp(`^((?:\\d{1,3}(?:,\\d{3})*|\\d+)(?:\\.\\d+)?)${escapedSuffix}$`).exec(normalized);
  if (match === null) return null;
  const parsed = Number((match[1] ?? '').replaceAll(',', ''));
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
}

function extractLiveQuoteTime(item: Element | null): string | null {
  const dateElement = item?.querySelector('.date') ?? null;
  const text = dateElement === null ? '' : visibleText(dateElement);
  const match = /^\(((?:[01]\d|2[0-3]):[0-5]\d)\)$/.exec(text);
  return match?.[1] ?? null;
}

function extractCurrentLiveQuote(document: Document): ShikihoQuoteV1 | undefined {
  const delayLabel = findExactLabel(document, '15分ディレイ');
  const section = delayLabel?.closest('.stock-index') ?? null;
  if (delayLabel === null || section === null) return undefined;

  const updateElement = section.querySelector('.stock-index__content__update .date');
  const updateText = updateElement === null ? '' : visibleText(updateElement);
  const updateMatch = /^(\d{4})\/(\d{2})\/(\d{2}) ((?:[01]\d|2[0-3]):[0-5]\d)$/.exec(updateText);
  if (updateMatch === null) return undefined;
  const tradingDate = `${updateMatch[1]}-${updateMatch[2]}-${updateMatch[3]}`;
  if (!isExactCalendarDate(tradingDate)) return undefined;
  const observedAt = `${tradingDate}T${updateMatch[4]}:00+09:00`;
  if (!QUOTE_TIMESTAMP_PATTERN.test(observedAt) || Number.isNaN(Date.parse(observedAt))) return undefined;

  const quoteItem = (label: string): Element | null => {
    const labelElement = findExactLabel(section, label);
    return labelElement?.parentElement ?? null;
  };
  const quoteValue = (item: Element | null): number | null =>
    parseLiveQuoteNumber(item?.querySelector('.value') ?? null);
  const openItem = quoteItem('始値');
  const highItem = quoteItem('高値');
  const lowItem = quoteItem('安値');
  const currentPrice = parseLiveQuoteNumber(section.querySelector('.stock-index__price__current'), '円');
  const open = quoteValue(openItem);
  const high = quoteValue(highItem);
  const low = quoteValue(lowItem);
  const previousClose = quoteValue(quoteItem('前日終値'));
  if (
    currentPrice === null ||
    open === null ||
    high === null ||
    low === null ||
    previousClose === null ||
    currentPrice < low ||
    currentPrice > high ||
    open < low ||
    open > high
  ) {
    return undefined;
  }
  return {
    tradingDate,
    observedAt,
    delayMinutes: 15,
    currentPrice,
    open,
    high,
    low,
    previousClose,
    volume: null,
    openTime: extractLiveQuoteTime(openItem),
    highTime: extractLiveQuoteTime(highItem),
    lowTime: extractLiveQuoteTime(lowItem),
    sourceLabel: '会社四季報オンライン',
  };
}

function extractDelayedQuote(document: Document): ShikihoQuoteV1 | undefined {
  return extractQuote(document) ?? extractCurrentLiveQuote(document);
}

function isExactShikihoStockUrl(location: URL, code: string): boolean {
  return (
    location.protocol === 'https:' &&
    location.hostname === 'shikiho.toyokeizai.net' &&
    location.port === '' &&
    location.username === '' &&
    location.password === '' &&
    location.pathname === `/stocks/${code}` &&
    location.search === '' &&
    location.hash === ''
  );
}

function hasCompleteCoreCapture(
  features: string | null,
  consolidatedBusinesses: string | null,
  commentary: ShikihoSnapshotV1['commentary']
): boolean {
  return features !== null && consolidatedBusinesses !== null && commentary.length > 0;
}

function hasRecognizableCaptureContent(
  features: string | null,
  consolidatedBusinesses: string | null,
  commentary: ShikihoSnapshotV1['commentary']
): boolean {
  return commentary.length > 0 || (features !== null && consolidatedBusinesses !== null);
}

// biome-ignore lint/complexity/noExcessiveCognitiveComplexity: one pass intentionally validates identity, extracts every supported field, and builds one shared result.
function inspectShikihoPageWithActiveMemo(
  document: Document,
  location: URL,
  now: Date,
  extractorVersion: string
): ShikihoPageInspection {
  const code = normalizeShikihoCode(/^\/stocks\/([^/]+)/.exec(location.pathname)?.[1]) ?? '';
  if (isLoginRequired(document)) {
    return { result: { kind: 'login_required', code }, fields: [], candidate: null };
  }
  if (code === '' || !isExactShikihoStockUrl(location, code)) {
    return { result: { kind: 'page_changed', code }, fields: [], candidate: null };
  }
  const identity = extractIdentity(document, code);
  if (identity === null) {
    return { result: { kind: 'page_changed', code }, fields: [], candidate: null };
  }

  const features = extractLabelValue(document, '特色');
  const consolidatedBusinesses = extractLabelValue(document, '連結事業');
  const commentary = extractCommentary(document);
  const { score, present: hasScore } = extractScore(document);
  const comparisonCompanies = extractComparisonCompanies(document);
  const industries = extractSectionList(document, '所属業界');
  const marketThemes = extractSectionList(document, '市場テーマ');
  const profile = extractProfile(document);
  const editionLabel = extractEditionLabel(document);
  const earningsAnnouncementDate = extractEarningsAnnouncementDate(document);
  const pageUpdatedAt = extractDateTime(document, '更新日時');
  const quote = extractDelayedQuote(document);
  const hasCoreCapture = hasCompleteCoreCapture(features, consolidatedBusinesses, commentary);
  const optionalPresence: Array<[ShikihoTraceField, boolean]> = [
    ['quote', quote !== undefined],
    ['features', features !== null],
    ['consolidatedBusinesses', consolidatedBusinesses !== null],
    ['commentary', commentary.length > 0],
    ['score', hasScore],
    ['comparisonCompanies', (comparisonCompanies?.length ?? 0) > 0],
    ['industries', (industries?.length ?? 0) > 0],
    ['marketThemes', (marketThemes?.length ?? 0) > 0],
    ['profile', (profile?.length ?? 0) > 0],
    ['editionLabel', editionLabel !== null],
    ['earningsAnnouncementDate', earningsAnnouncementDate !== null],
    ['pageUpdatedAt', pageUpdatedAt !== null],
    ['coreReady', hasCoreCapture],
  ];
  const fields: ShikihoTraceField[] = [
    'identity',
    ...optionalPresence.filter(([, present]) => present).map(([field]) => field),
  ];

  const optionalFields: Array<[string, boolean]> = [
    ['features', features !== null],
    ['consolidatedBusinesses', consolidatedBusinesses !== null],
    ['commentary', commentary.length > 0],
    ['score', hasScore],
    ['comparisonCompanies', comparisonCompanies !== null && comparisonCompanies.length > 0],
    ['industries', industries !== null && industries.length > 0],
    ['marketThemes', marketThemes !== null && marketThemes.length > 0],
    ['profile', profile !== null && profile.length > 0],
    ['editionLabel', editionLabel !== null],
    ['earningsAnnouncementDate', earningsAnnouncementDate !== null],
    ['pageUpdatedAt', pageUpdatedAt !== null],
  ];
  const missingFields = optionalFields.filter(([, present]) => !present).map(([field]) => field);
  const snapshotWithoutCaptureTime = {
    schemaVersion: 1 as const,
    extractorVersion,
    code,
    companyName: identity.companyName,
    sourceUrl: `${location.origin}/stocks/${code}`,
    pageUpdatedAt,
    editionLabel,
    earningsAnnouncementDate,
    status: hasCoreCapture ? ('captured' as const) : ('partial' as const),
    features,
    consolidatedBusinesses,
    commentary,
    score,
    comparisonCompanies: comparisonCompanies ?? [],
    industries: industries ?? [],
    marketThemes: marketThemes ?? [],
    profile: profile ?? [],
    quote,
    missingFields,
  };

  const snapshot: ShikihoSnapshotV1 = {
    ...snapshotWithoutCaptureTime,
    capturedAt: now.toISOString(),
    contentHash: computeContentHashSync(snapshotWithoutCaptureTime),
  };
  const hasCandidateContent = fields.some((field) => field !== 'identity');
  const result: ShikihoExtractionResult = hasRecognizableCaptureContent(features, consolidatedBusinesses, commentary)
    ? { kind: 'success', snapshot }
    : { kind: 'page_changed', code };
  return { result, fields, candidate: hasCandidateContent ? snapshot : null };
}

export function inspectShikihoPage(
  document: Document,
  location: URL,
  now: Date,
  extractorVersion: string
): ShikihoPageInspection {
  const previousMemo = activeInspectionMemo;
  activeInspectionMemo = { visibility: new WeakMap(), visibleText: new WeakMap() };
  try {
    return inspectShikihoPageWithActiveMemo(document, location, now, extractorVersion);
  } finally {
    activeInspectionMemo = previousMemo;
  }
}

export function probeShikihoFields(document: Document, location: URL): ShikihoTraceField[] {
  return inspectShikihoPage(document, location, new Date(0), 'probe').fields;
}

export function extractShikihoPage(
  document: Document,
  location: URL,
  now: Date,
  extractorVersion: string
): ShikihoExtractionResult {
  return inspectShikihoPage(document, location, now, extractorVersion).result;
}
