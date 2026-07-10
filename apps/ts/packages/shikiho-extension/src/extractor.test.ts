import { describe, expect, test } from 'bun:test';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { Window } from 'happy-dom';
import { extractShikihoPage } from './extractor';

const NOW = new Date('2026-07-10T01:02:03.000Z');
const FIXTURE_URL = new URL('https://shikiho.toyokeizai.net/stocks/7203');

function parseFixture(name: string): Document {
  const html = readFileSync(join(import.meta.dir, 'fixtures', name), 'utf8');
  const window = new Window({ url: FIXTURE_URL.href });
  window.document.write(html);
  return window.document as unknown as Document;
}

function extractFixture(name: string) {
  return extractShikihoPage(parseFixture(name), FIXTURE_URL, NOW, '1.0.0');
}

describe('Shikiho page extractor', () => {
  test('extracts approved 7203 fields without retaining markup', () => {
    const document = parseFixture('7203-authenticated.html');
    const result = extractShikihoPage(document, FIXTURE_URL, NOW, '1.0.0');

    expect(result.kind).toBe('success');
    if (result.kind !== 'success') throw new Error('expected success');
    expect(result.snapshot.features).toContain('4輪世界首位');
    expect(result.snapshot.commentary.map((item) => item.heading)).toEqual(['連続減益', '対応策']);
    expect(result.snapshot.score).toMatchObject({ overall: 4, growth: 5, profitability: 5, safety: 2 });
    expect(result.snapshot.comparisonCompanies).toContainEqual({ code: '7201', name: '日産自動車' });
    expect(result.snapshot).toMatchObject({
      code: '7203',
      companyName: 'トヨタ自動車',
      editionLabel: '2026年3集',
      pageUpdatedAt: '2026-07-09T00:00:00+09:00',
      status: 'captured',
      missingFields: [],
      industries: ['自動車', '輸送用機器'],
      marketThemes: ['EV', '自動運転'],
      profile: [
        { label: '本社', value: '愛知県豊田市' },
        { label: '上場', value: '1949年5月' },
      ],
    });
    expect(JSON.stringify(result.snapshot)).not.toContain('<');
  });

  test('distinguishes login and page-shape failures', () => {
    expect(extractFixture('login-required.html').kind).toBe('login_required');
    expect(extractFixture('page-changed.html').kind).toBe('page_changed');
  });

  test('returns a partial snapshot with stable missing field keys', () => {
    const document = parseFixture('7203-authenticated.html');
    document.querySelectorAll('section').forEach((section) => {
      if (section.querySelector('h2')?.textContent === '市場テーマ') section.remove();
    });

    const result = extractShikihoPage(document, FIXTURE_URL, NOW, '1.0.0');

    expect(result.kind).toBe('success');
    if (result.kind !== 'success') throw new Error('expected success');
    expect(result.snapshot.status).toBe('partial');
    expect(result.snapshot.missingFields).toEqual(['marketThemes']);
  });

  test('rejects a rendered identity that disagrees with the source URL', () => {
    const document = parseFixture('7203-authenticated.html');
    const code = document.querySelector('header span');
    if (code !== null) code.textContent = '6758';

    expect(extractShikihoPage(document, FIXTURE_URL, NOW, '1.0.0')).toEqual({
      kind: 'page_changed',
      code: '7203',
    });
  });

  test('content hash ignores capture time but changes with visible content', () => {
    const first = extractShikihoPage(parseFixture('7203-authenticated.html'), FIXTURE_URL, NOW, '1.0.0');
    const later = extractShikihoPage(
      parseFixture('7203-authenticated.html'),
      FIXTURE_URL,
      new Date('2026-07-11T01:02:03.000Z'),
      '1.0.0'
    );
    const changedDocument = parseFixture('7203-authenticated.html');
    const features = Array.from(changedDocument.querySelectorAll('h2')).find(
      (heading) => heading.textContent === '特色'
    )?.nextElementSibling;
    if (features !== null && features !== undefined) features.textContent = '変更後の特色';
    const changed = extractShikihoPage(changedDocument, FIXTURE_URL, NOW, '1.0.0');

    expect(first.kind).toBe('success');
    expect(later.kind).toBe('success');
    expect(changed.kind).toBe('success');
    if (first.kind !== 'success' || later.kind !== 'success' || changed.kind !== 'success') return;
    expect(first.snapshot.contentHash).toBe(later.snapshot.contentHash);
    expect(changed.snapshot.contentHash).not.toBe(first.snapshot.contentHash);
  });

  test('prefers semantic section labels over unrelated navigation text', () => {
    const document = parseFixture('7203-authenticated.html');
    document.body.insertAdjacentHTML(
      'afterbegin',
      '<nav><button>特色</button><span>ナビゲーション</span><a href="/stocks/9999">9999 対象外</a></nav>'
    );

    const result = extractShikihoPage(document, FIXTURE_URL, NOW, '1.0.0');

    expect(result.kind).toBe('success');
    if (result.kind !== 'success') throw new Error('expected success');
    expect(result.snapshot.features).toContain('4輪世界首位');
    expect(result.snapshot.comparisonCompanies).not.toContainEqual({ code: '9999', name: '対象外' });
  });
});
