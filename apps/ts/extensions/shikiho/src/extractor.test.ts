import { describe, expect, test } from 'bun:test';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { Window } from 'happy-dom';
import { parseShikihoSnapshot } from './contract';
import { extractShikihoPage, inspectShikihoPage, parseScore, probeShikihoFields } from './extractor';

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

function replaceAdjacentScoreValue(section: Element | null | undefined, label: string, value: string | null): void {
  const term = Array.from(section?.querySelectorAll('dt') ?? []).find(
    (candidate) => candidate.textContent === label && candidate.closest('[hidden]') === null
  );
  const adjacentValue = term?.nextElementSibling;
  if (adjacentValue === null || adjacentValue === undefined) return;
  if (value === null) adjacentValue.remove();
  else adjacentValue.textContent = value;
}

describe('Shikiho page extractor', () => {
  test('accepts only a normalized single integer score from zero through five', () => {
    expect(parseScore('  \n 4 \t ')).toBe(4);
    expect(parseScore('0')).toBe(0);
    expect(parseScore('5')).toBe(5);
    expect(parseScore('3.0')).toBeNull();
    expect(parseScore('4 extra')).toBeNull();
    expect(parseScore('2 / 5')).toBeNull();
    expect(parseScore('-1')).toBeNull();
    expect(parseScore('6')).toBeNull();
  });

  test('extracts the current authenticated table commentary and edition paragraph', () => {
    const result = extractFixture('7203-current-authenticated.html');

    expect(result.kind).toBe('success');
    if (result.kind !== 'success') throw new Error('expected success');
    expect(result.snapshot.commentary).toEqual([
      { heading: '連続減益', body: '架空の短い業績コメントです。' },
      { heading: '新製品', body: '架空の短い新製品コメントです。' },
    ]);
    expect(result.snapshot.editionLabel).toBe('2026年3集夏号（2026年6月17日発売）');
    expect(result.snapshot.commentary.map((item) => item.heading)).not.toContain('非表示');
    expect(result.snapshot.commentary.map((item) => item.heading)).not.toContain('本文なし');
    expect(result.snapshot.commentary.map((item) => item.heading)).not.toContain('隠し見出し');
    expect(result.snapshot.features).toBe('架空の短い企業特色です。');
    expect(result.snapshot.consolidatedBusinesses).toBe('車両70%、部品30%');
    expect(result.snapshot.score).toEqual({
      overall: 4,
      growth: 1,
      profitability: 2,
      safety: 3,
      scale: 4,
      value: 5,
      priceMomentum: 0,
    });
    expect(result.snapshot.status).toBe('captured');
    expect(result.snapshot.missingFields).toEqual([
      'comparisonCompanies',
      'industries',
      'marketThemes',
      'profile',
      'pageUpdatedAt',
    ]);
  });

  test('extracts a strict delayed quote from the visible fictional quote region', () => {
    const result = extractFixture('7203-current-quote.html');

    expect(result.kind).toBe('success');
    if (result.kind !== 'success') throw new Error('expected success');
    expect(result.snapshot.quote).toEqual({
      tradingDate: '2026-07-10',
      observedAt: '2026-07-10T14:45:00+09:00',
      delayMinutes: 15,
      currentPrice: 102,
      open: 100,
      high: 105,
      low: 98,
      previousClose: 99,
      volume: 12_300,
      openTime: '09:00',
      highTime: '13:20',
      lowTime: null,
      sourceLabel: '会社四季報オンライン',
    });
    expect(result.snapshot.status).toBe('captured');
    expect(result.snapshot.missingFields).not.toContain('quote');
    expect(parseShikihoSnapshot(result.snapshot)).toEqual(result.snapshot);
  });

  test('probes identity and quote before article content is canonically recognizable', () => {
    const document = parseFixture('7203-current-quote.html');
    for (const label of ['特色', '連結事業', '会社四季報']) {
      const element = Array.from(document.querySelectorAll('*')).find(
        (candidate) => candidate.textContent?.trim() === label
      );
      element?.closest('section, article, table, dl')?.remove();
    }

    expect(extractShikihoPage(document, FIXTURE_URL, NOW, '1.0.0')).toEqual({ kind: 'page_changed', code: '7203' });
    expect(probeShikihoFields(document, FIXTURE_URL)).toEqual(expect.arrayContaining(['identity', 'quote']));
  });

  test.each([
    ['features', '<dl><dt>特色</dt><dd>架空の特色</dd></dl>', 'features'],
    ['businesses', '<dl><dt>連結事業</dt><dd>架空の事業</dd></dl>', 'consolidatedBusinesses'],
    ['secondary score', '<section class="score"><dl><dt>四季報スコア</dt><dd>4</dd></dl></section>', 'score'],
  ] as const)('returns a noncanonical provisional candidate for %s-only content', (_name, body, field) => {
    const window = new Window({ url: FIXTURE_URL.href });
    window.document.write(`<main><h1>7203 トヨタ自動車</h1>${body}</main>`);

    const inspection = inspectShikihoPage(window.document as unknown as Document, FIXTURE_URL, NOW, '1.0.0');

    expect(inspection.result).toEqual({ kind: 'page_changed', code: '7203' });
    expect(inspection.fields).toEqual(['identity', field]);
    expect(inspection.candidate).toMatchObject({ code: '7203', status: 'partial' });
  });

  test('extracts score and delayed quote from the current live Shikiho DOM shape', () => {
    const result = extractFixture('7203-current-live-shape.html');

    expect(result.kind).toBe('success');
    if (result.kind !== 'success') throw new Error('expected success');
    expect(result.snapshot.score).toEqual({
      overall: 4,
      growth: 5,
      profitability: 5,
      safety: 2,
      scale: 5,
      value: 4,
      priceMomentum: 3,
    });
    expect(result.snapshot.quote).toEqual({
      tradingDate: '2026-07-13',
      observedAt: '2026-07-13T13:00:00+09:00',
      delayMinutes: 15,
      currentPrice: 2808.5,
      open: 2844,
      high: 2847,
      low: 2802,
      previousClose: 2823,
      volume: null,
      openTime: '09:00',
      highTime: '09:00',
      lowTime: '10:02',
      sourceLabel: '会社四季報オンライン',
    });
    expect(parseShikihoSnapshot(result.snapshot)).toEqual(result.snapshot);
  });

  test('includes the quote in the snapshot content hash', () => {
    const initial = extractFixture('7203-current-quote.html');
    const changedDocument = parseFixture('7203-current-quote.html');
    const currentPrice = Array.from(changedDocument.querySelectorAll('dt')).find(
      (term) => term.textContent === '現在値'
    )?.nextElementSibling;
    if (currentPrice !== null && currentPrice !== undefined) currentPrice.textContent = '103';
    const changed = extractShikihoPage(changedDocument, FIXTURE_URL, NOW, '1.0.0');

    expect(initial.kind).toBe('success');
    expect(changed.kind).toBe('success');
    if (initial.kind !== 'success' || changed.kind !== 'success') throw new Error('expected success');
    expect(changed.snapshot.quote?.currentPrice).toBe(103);
    expect(changed.snapshot.contentHash).not.toBe(initial.snapshot.contentHash);
  });

  test('rejects hidden, malformed, zero, inconsistent, and missing quotes without downgrading article capture', () => {
    const mutations: Array<(document: Document) => void> = [
      (document) =>
        Array.from(document.querySelectorAll('h2'))
          .find((heading) => heading.textContent === '株価')
          ?.closest('section')
          ?.setAttribute('hidden', ''),
      (document) => replaceAdjacentScoreValue(document.querySelector('section:last-of-type'), '現在値', '102円'),
      (document) => replaceAdjacentScoreValue(document.querySelector('section:last-of-type'), '始値', '0'),
      (document) => replaceAdjacentScoreValue(document.querySelector('section:last-of-type'), '高値', '101'),
      (document) => replaceAdjacentScoreValue(document.querySelector('section:last-of-type'), '現在値', null),
      (document) => {
        const updateTime = document.querySelector('section:last-of-type time[datetime*="T"]');
        updateTime?.setAttribute('datetime', '2026-02-30T14:45:00+09:00');
      },
      (document) => {
        const updateTime = document.querySelector('section:last-of-type time[datetime*="T"]');
        updateTime?.setAttribute('datetime', '2026-07-10T14:45+09:00');
      },
    ];

    for (const mutate of mutations) {
      const document = parseFixture('7203-current-quote.html');
      mutate(document);
      const result = extractShikihoPage(document, FIXTURE_URL, NOW, '1.0.0');
      expect(result.kind).toBe('success');
      if (result.kind !== 'success') throw new Error('expected success');
      expect(result.snapshot.quote).toBeUndefined();
      expect(result.snapshot.status).toBe('captured');
      expect(result.snapshot.missingFields).not.toContain('quote');
    }
  });

  test('keeps a core-missing current fixture partial', () => {
    const document = parseFixture('7203-current-authenticated.html');
    const consolidatedLabel = Array.from(document.querySelectorAll('dt')).find((label) =>
      label.textContent?.includes('連結事業')
    );
    consolidatedLabel?.nextElementSibling?.remove();
    consolidatedLabel?.remove();

    const result = extractShikihoPage(document, FIXTURE_URL, NOW, '1.0.0');

    expect(result.kind).toBe('success');
    if (result.kind !== 'success') throw new Error('expected success');
    expect(result.snapshot.consolidatedBusinesses).toBeNull();
    expect(result.snapshot.status).toBe('partial');
    expect(result.snapshot.missingFields).toContain('consolidatedBusinesses');
  });

  test('returns a partial success when valid core-labelled content has no commentary', () => {
    const document = parseFixture('7203-current-authenticated.html');
    document.querySelector('table')?.remove();

    const result = extractShikihoPage(document, FIXTURE_URL, NOW, '1.0.0');

    expect(result.kind).toBe('success');
    if (result.kind !== 'success') throw new Error('expected success');
    expect(result.snapshot.features).toBe('架空の短い企業特色です。');
    expect(result.snapshot.consolidatedBusinesses).toBe('車両70%、部品30%');
    expect(result.snapshot.commentary).toEqual([]);
    expect(result.snapshot.status).toBe('partial');
    expect(result.snapshot.missingFields).toContain('commentary');
  });

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

  test('keeps captured status with stable optional missing field keys', () => {
    const document = parseFixture('7203-authenticated.html');
    document.querySelectorAll('section').forEach((section) => {
      if (section.querySelector('h2')?.textContent === '市場テーマ') section.remove();
    });

    const result = extractShikihoPage(document, FIXTURE_URL, NOW, '1.0.0');

    expect(result.kind).toBe('success');
    if (result.kind !== 'success') throw new Error('expected success');
    expect(result.snapshot.status).toBe('captured');
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

  test('ignores unrelated bracketed text when commentary is missing', () => {
    const document = parseFixture('7203-authenticated.html');
    document.querySelector('section[aria-label="会社四季報コメント"]')?.remove();
    document.body.insertAdjacentHTML('beforeend', '<aside><p>【お知らせ】サイト更新情報です。</p></aside>');

    const result = extractShikihoPage(document, FIXTURE_URL, NOW, '1.0.0');
    expect(result.kind).toBe('success');
    if (result.kind !== 'success') throw new Error('expected success');
    expect(result.snapshot.commentary).toEqual([]);
    expect(result.snapshot.status).toBe('partial');
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
    const hiddenCommentaryResult = extractShikihoPage(document, FIXTURE_URL, NOW, '1.0.0');
    expect(hiddenCommentaryResult.kind).toBe('success');
    if (hiddenCommentaryResult.kind !== 'success') throw new Error('expected success');
    expect(hiddenCommentaryResult.snapshot.commentary).toEqual([]);
    expect(hiddenCommentaryResult.snapshot.status).toBe('partial');
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

  test('keeps captured status when the optional score region is unparseable', () => {
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
    expect(result.snapshot.status).toBe('captured');
    expect(result.snapshot.missingFields).toContain('score');
  });

  test('ignores hidden, missing, malformed, and out-of-range score values', () => {
    const document = parseFixture('7203-current-authenticated.html');
    const scoreHeading = Array.from(document.querySelectorAll('h2')).find(
      (heading) => heading.textContent === '四季報スコア'
    );
    const section = scoreHeading?.closest('section');
    section?.insertAdjacentHTML(
      'afterbegin',
      '<div hidden><h2>四季報スコア</h2><strong>5</strong><dl><dt>成長性</dt><dd>5</dd></dl></div>'
    );
    if (scoreHeading?.nextElementSibling !== null && scoreHeading?.nextElementSibling !== undefined) {
      scoreHeading.nextElementSibling.textContent = '6';
    }
    replaceAdjacentScoreValue(section, '成長性', '3.5');
    replaceAdjacentScoreValue(section, '収益性', '-1');
    replaceAdjacentScoreValue(section, '安全性', '対象外');
    replaceAdjacentScoreValue(section, '規模', '5点');
    replaceAdjacentScoreValue(section, '割安度', '2');
    replaceAdjacentScoreValue(section, '値上がり', null);

    const result = extractShikihoPage(document, FIXTURE_URL, NOW, '1.0.0');

    expect(result.kind).toBe('success');
    if (result.kind !== 'success') throw new Error('expected success');
    expect(result.snapshot.score).toEqual({
      overall: null,
      growth: null,
      profitability: null,
      safety: null,
      scale: null,
      value: 2,
      priceMomentum: null,
    });
    expect(result.snapshot.status).toBe('captured');
    expect(result.snapshot.missingFields).not.toContain('score');
  });
});
