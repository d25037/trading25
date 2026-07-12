import { describe, expect, test } from 'bun:test';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { Window } from 'happy-dom';
import { parseShikihoSnapshot } from './contract';
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
  test('classifies the current paid-plan prompt as login required', () => {
    const document = parseFixture('7203-login-plan-required.html');
    expect(extractShikihoPage(document, FIXTURE_URL, NOW, '1.0.0')).toEqual({
      kind: 'login_required',
      code: '7203',
    });
  });

  test('does not treat a navigation login control as sufficient on a valid page', () => {
    const document = parseFixture('7203-authenticated.html');
    document.querySelector('body')?.insertAdjacentHTML('afterbegin', '<nav><button>ログイン</button></nav>');
    expect(extractShikihoPage(document, FIXTURE_URL, NOW, '1.0.0').kind).toBe('success');
  });

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
    expect(result.snapshot.features).toContain('販売は 増加、利益率は 10% > 8% & 条件は x < y');
    expect(result.snapshot.features).not.toContain('<strong>');
    expect(parseShikihoSnapshot(result.snapshot)).toEqual(result.snapshot);
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

  test('requires bracketed commentary inside the visible Shikiho commentary region', () => {
    const document = parseFixture('7203-authenticated.html');
    document.querySelector('section[aria-label="会社四季報コメント"]')?.remove();
    document.body.insertAdjacentHTML('beforeend', '<aside><p>【お知らせ】サイト更新情報です。</p></aside>');

    expect(extractShikihoPage(document, FIXTURE_URL, NOW, '1.0.0')).toEqual({
      kind: 'page_changed',
      code: '7203',
    });
  });

  test('ignores hidden duplicate labels and hidden commentary', () => {
    const document = parseFixture('7203-authenticated.html');
    document.body.insertAdjacentHTML(
      'afterbegin',
      [
        '<section hidden><h2>特色</h2><p>hidden attribute</p></section>',
        '<section aria-hidden="true"><h2>特色</h2><p>aria hidden</p></section>',
        '<section style="display: none"><h2>特色</h2><p>display none</p></section>',
        '<section style="visibility: hidden"><h2>特色</h2><p>visibility hidden</p></section>',
        '<template><section><h2>特色</h2><p>template content</p></section></template>',
      ].join('')
    );

    const visibleResult = extractShikihoPage(document, FIXTURE_URL, NOW, '1.0.0');
    expect(visibleResult.kind).toBe('success');
    if (visibleResult.kind !== 'success') throw new Error('expected success');
    expect(visibleResult.snapshot.features).toContain('4輪世界首位');

    document.querySelector('section[aria-label="会社四季報コメント"]')?.setAttribute('hidden', '');
    expect(extractShikihoPage(document, FIXTURE_URL, NOW, '1.0.0')).toEqual({
      kind: 'page_changed',
      code: '7203',
    });
  });

  test('ignores hidden login text and controls', () => {
    const document = parseFixture('7203-authenticated.html');
    document.body.insertAdjacentHTML(
      'afterbegin',
      '<div aria-hidden="true">ログインして四季報を閲覧<input type="password"></div><input type="password" hidden>'
    );

    expect(extractShikihoPage(document, FIXTURE_URL, NOW, '1.0.0').kind).toBe('success');
  });

  test('requires the exact Shikiho stock URL before producing a snapshot', () => {
    const invalidUrls = [
      'http://shikiho.toyokeizai.net/stocks/7203',
      'https://evil.example/stocks/7203',
      'https://shikiho.toyokeizai.net:444/stocks/7203',
      'https://user:password@shikiho.toyokeizai.net/stocks/7203',
      'https://shikiho.toyokeizai.net/stocks/7203/extra',
      'https://shikiho.toyokeizai.net/stocks/7203?tab=company',
      'https://shikiho.toyokeizai.net/stocks/7203#company',
    ];

    for (const sourceUrl of invalidUrls) {
      expect(extractShikihoPage(parseFixture('7203-authenticated.html'), new URL(sourceUrl), NOW, '1.0.0').kind).toBe(
        'page_changed'
      );
    }
  });

  test('marks an empty or unparseable score region as partial', () => {
    const document = parseFixture('7203-authenticated.html');
    const scoreHeading = Array.from(document.querySelectorAll('h2')).find(
      (heading) => heading.textContent === '四季報スコア'
    );
    scoreHeading
      ?.closest('section')
      ?.querySelectorAll('dd')
      .forEach((value) => {
        value.textContent = '算出対象外';
      });

    const result = extractShikihoPage(document, FIXTURE_URL, NOW, '1.0.0');

    expect(result.kind).toBe('success');
    if (result.kind !== 'success') throw new Error('expected success');
    expect(result.snapshot.status).toBe('partial');
    expect(result.snapshot.missingFields).toContain('score');
  });
});
