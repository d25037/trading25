import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import type {
  ShikihoCaptureDiagnosticV1,
  ShikihoCaptureTraceV1,
  ShikihoSnapshotV1,
} from '@trading25/shikiho-extension/contract';
import { describe, expect, test, vi } from 'vitest';
import type { ShikihoCaptureState } from '@/hooks/useShikihoSnapshot';
import { ShikihoPanel } from './ShikihoPanel';

const snapshot7203: ShikihoSnapshotV1 = {
  schemaVersion: 1,
  extractorVersion: '1.0.0',
  code: '7203',
  companyName: 'トヨタ自動車',
  sourceUrl: 'https://shikiho.toyokeizai.net/stocks/7203',
  capturedAt: '2026-07-10T01:02:03.000Z',
  pageUpdatedAt: '2026-07-09T00:00:00+09:00',
  editionLabel: '2026年3集',
  earningsAnnouncementDate: null,
  contentHash: 'sha256:example',
  status: 'captured',
  features: '4輪世界首位。世界販売を拡大。',
  consolidatedBusinesses: '自動車事業、金融事業',
  commentary: [{ heading: '連続増益', body: '原価低減を進める。' }],
  score: {
    overall: 4,
    growth: 5,
    profitability: 5,
    safety: 2,
    scale: 5,
    value: 3,
    priceMomentum: null,
  },
  comparisonCompanies: [
    { code: '7201', name: '日産自動車' },
    { code: null, name: '海外メーカー' },
  ],
  industries: ['自動車'],
  marketThemes: ['EV'],
  profile: [{ label: '本社', value: '愛知県豊田市' }],
  missingFields: [],
};

const noDiagnostic = null;
const noop = () => undefined;
const provisionalProvenance = {
  provisional: true as const,
  tradingDate: '2026-07-13',
  observedAt: '2026-07-13T01:35:00.000Z',
  delayMinutes: 15 as const,
  sourceLabel: '会社四季報オンライン' as const,
};

const emptySnapshot: ShikihoSnapshotV1 = {
  ...snapshot7203,
  features: null,
  consolidatedBusinesses: null,
  commentary: [],
  score: {
    overall: null,
    growth: null,
    profitability: null,
    safety: null,
    scale: null,
    value: null,
    priceMomentum: null,
  },
  comparisonCompanies: [],
  industries: [],
  marketThemes: [],
  profile: [],
};

const activeTrace: ShikihoCaptureTraceV1 = {
  schemaVersion: 1,
  attemptId: 'attempt-progressive',
  code: '7203',
  mode: 'new_owned_tab',
  phase: 'observing_dom',
  startedAt: '2026-07-14T00:00:00.000Z',
  updatedAt: '2026-07-14T00:00:06.200Z',
  outcome: null,
  waitEndReason: null,
  receiverAttempts: 1,
  receiverReadyMs: 100,
  documentReadyState: 'interactive',
  navigation: { responseStartMs: 10, domInteractiveMs: 90, domContentLoadedMs: null, loadEndMs: null },
  dom: {
    firstSampleMs: 120,
    mutationBatches: 2,
    meaningfulChanges: 2,
    samples: 2,
    presentFields: ['identity', 'features', 'commentary'],
    missingFields: ['quote', 'consolidatedBusinesses', 'score'],
    firstSeenMs: {
      identity: 120,
      quote: null,
      features: 300,
      consolidatedBusinesses: null,
      commentary: 500,
      score: null,
      comparisonCompanies: null,
      industries: null,
      marketThemes: null,
      profile: null,
      editionLabel: null,
      earningsAnnouncementDate: null,
      pageUpdatedAt: null,
      coreReady: null,
    },
  },
  extraction: { samples: 2, lastMs: 4, maxMs: 4, totalMs: 8 },
  timings: { probeMs: 5, acquisitionMs: 10, receiverMs: 100, domObservationMs: 6_085, storageMs: 0, totalMs: 6_200 },
};

function renderPanel(
  snapshot: ShikihoSnapshotV1 | null,
  captureState: ShikihoCaptureState = snapshot?.status ?? 'not_captured',
  diagnostic: ShikihoCaptureDiagnosticV1 | null = noDiagnostic
) {
  return render(
    <ShikihoPanel
      symbol="7203"
      snapshot={snapshot}
      diagnostic={diagnostic}
      captureState={captureState}
      isRefreshing={false}
      onRefresh={noop}
      onSelectSymbol={noop}
    />
  );
}

describe('ShikihoPanel', () => {
  test('renders candidate-only fields progressively while keeping stable fallback content', () => {
    const candidate = {
      ...emptySnapshot,
      status: 'partial' as const,
      features: '先に取得できた特色',
      commentary: [{ heading: '進捗', body: '先に取得できたコメント' }],
      missingFields: ['consolidatedBusinesses'],
    };
    const displaySnapshot = {
      ...candidate,
      consolidatedBusinesses: snapshot7203.consolidatedBusinesses,
    };

    render(
      <ShikihoPanel
        symbol="7203"
        snapshot={displaySnapshot}
        candidate={candidate}
        trace={activeTrace}
        diagnostic={null}
        captureState="captured"
        isRefreshing
        onRefresh={noop}
        onSelectSymbol={noop}
      />
    );

    expect(screen.getByText('先に取得できた特色')).toBeInTheDocument();
    expect(screen.getByText('自動車事業、金融事業')).toBeInTheDocument();
    expect(screen.getByText('更新中（新規 2項目）')).toBeInTheDocument();
    expect(screen.getByText('DOM確認 6.2秒')).toBeInTheDocument();
    expect(screen.queryByText('取得済み')).not.toBeInTheDocument();
  });

  test('does not present a candidate quote as chart provenance', () => {
    const candidateQuote = {
      tradingDate: '2026-07-14',
      observedAt: '2026-07-14T01:00:00.000Z',
      delayMinutes: 15 as const,
      currentPrice: 999,
      open: 990,
      high: 1_000,
      low: 980,
      previousClose: 970,
      volume: 100,
      openTime: null,
      highTime: null,
      lowTime: null,
      sourceLabel: '会社四季報オンライン' as const,
    };
    render(
      <ShikihoPanel
        symbol="7203"
        snapshot={{ ...emptySnapshot, quote: candidateQuote }}
        candidate={{ ...emptySnapshot, quote: candidateQuote }}
        trace={activeTrace}
        diagnostic={null}
        captureState="not_captured"
        isRefreshing
        onRefresh={noop}
        onSelectSymbol={noop}
        provisionalProvenance={null}
      />
    );

    expect(screen.queryByText('四季報 15分遅延・当日暫定')).not.toBeInTheDocument();
    expect(screen.queryByTestId('shikiho-quote')).not.toBeInTheDocument();
  });

  test('keeps canonical edition, timestamp, and quote separate from progressive body content', () => {
    const canonicalQuote = {
      tradingDate: '2026-07-14',
      observedAt: '2026-07-14T01:00:00.000Z',
      delayMinutes: 15 as const,
      currentPrice: 120,
      open: 112,
      high: 125,
      low: 110,
      previousClose: 108,
      volume: 123_000,
      openTime: null,
      highTime: null,
      lowTime: null,
      sourceLabel: '会社四季報オンライン' as const,
    };
    const canonical = { ...snapshot7203, quote: canonicalQuote };
    const candidate = {
      ...emptySnapshot,
      capturedAt: '2026-07-14T00:00:06.200Z',
      editionLabel: '2026年4集（候補）',
      pageUpdatedAt: '2026-07-14T00:00:00+09:00',
      features: '候補から先に表示する特色',
      quote: { ...canonicalQuote, currentPrice: 999 },
    };

    render(
      <ShikihoPanel
        symbol="7203"
        snapshot={candidate}
        canonicalSnapshot={canonical}
        candidate={candidate}
        trace={activeTrace}
        diagnostic={null}
        captureState="captured"
        isRefreshing
        onRefresh={noop}
        onSelectSymbol={noop}
        provisionalProvenance={provisionalProvenance}
      />
    );

    expect(screen.getByText('候補から先に表示する特色')).toBeInTheDocument();
    expect(screen.getByText('2026年3集')).toBeInTheDocument();
    expect(screen.queryByText('2026年4集（候補）')).not.toBeInTheDocument();
    expect(screen.getByText(/取得 .*2026/)).toHaveTextContent('2026/07/10');
    expect(screen.getByTestId('shikiho-quote')).toHaveTextContent('現在値￥120');
    expect(screen.getByTestId('shikiho-quote')).not.toHaveTextContent('￥999');
  });

  test('renders a compact captured snapshot and comparison navigation', async () => {
    const onSelectSymbol = vi.fn();
    render(
      <ShikihoPanel
        symbol="7203"
        snapshot={snapshot7203}
        diagnostic={null}
        captureState="captured"
        isRefreshing={false}
        onRefresh={noop}
        onSelectSymbol={onSelectSymbol}
      />
    );

    expect(screen.getByText('特色')).toBeInTheDocument();
    expect(screen.getByText(/4輪世界首位/)).toBeInTheDocument();
    expect(screen.getByText('連結事業')).toBeInTheDocument();
    expect(screen.getByRole('region', { name: '会社四季報' })).toBeInTheDocument();
    expect(screen.getByRole('heading', { name: '会社四季報' })).toBeInTheDocument();
    expect(screen.getByRole('status')).toHaveTextContent('取得済み');
    expect(screen.getAllByText('会社四季報')).toHaveLength(1);
    expect(screen.getByTestId('shikiho-body')).toHaveClass('lg:grid-cols-[minmax(0,2fr)_minmax(16rem,1fr)]');
    expect(screen.getByTestId('shikiho-primary')).toHaveClass('lg:border-r');
    expect(screen.getByTestId('shikiho-secondary')).toBeInTheDocument();
    expect(screen.getByTestId('shikiho-score-card')).toBeInTheDocument();
    expect(screen.getByTestId('shikiho-body').firstElementChild).toBe(screen.getByTestId('shikiho-score-card'));
    await userEvent.click(screen.getByRole('button', { name: /7201 日産自動車/ }));
    expect(onSelectSymbol).toHaveBeenCalledWith('7201');
    expect(screen.getByText('海外メーカー')).toBeInTheDocument();
  });

  test('renders the earnings announcement badge with date and urgency copy', () => {
    vi.setSystemTime(new Date('2026-07-15T03:00:00.000Z'));
    renderPanel({ ...snapshot7203, earningsAnnouncementDate: '2026-07-18' });

    const badge = screen.getByLabelText('決算発表予定日 2026年7月18日 あと3日');
    expect(badge).toHaveTextContent('決算発表予定日');
    expect(badge).toHaveTextContent('2026/07/18');
    expect(badge).toHaveTextContent('あと3日');

    vi.useRealTimers();
  });

  test('renders source text literally and supports collapse', async () => {
    renderPanel({ ...snapshot7203, features: '<img src=x onerror=alert(1)>' });

    expect(screen.getByText('<img src=x onerror=alert(1)>')).toBeInTheDocument();
    expect(document.querySelector('img')).toBeNull();
    const collapseButton = screen.getByRole('button', { name: /会社四季報を折りたたむ/ });
    expect(collapseButton).toHaveAttribute('aria-controls');
    const controlledBodyId = collapseButton.getAttribute('aria-controls');
    if (!controlledBodyId) throw new Error('Collapse button must reference the disclosure body');
    expect(document.getElementById(controlledBodyId)).toBeInTheDocument();
    await userEvent.click(collapseButton);
    const collapsedBody = screen.getByTestId('shikiho-body');
    expect(collapsedBody).toHaveAttribute('hidden');
    expect(collapsedBody).toHaveAttribute('id', controlledBodyId);
    expect(screen.getByText('特色').closest('[hidden]')).toBe(collapsedBody);
    expect(screen.getByRole('link', { name: /四季報で開く/ })).toBeInTheDocument();
  });

  test('renders compact quote provenance and OHLC details only for an active provisional overlay', () => {
    const quotedSnapshot: ShikihoSnapshotV1 = {
      ...snapshot7203,
      quote: {
        tradingDate: '2026-07-13',
        observedAt: '2026-07-13T01:35:00.000Z',
        delayMinutes: 15,
        currentPrice: 120,
        open: 112,
        high: 125,
        low: 110,
        previousClose: 108,
        volume: 123_000,
        openTime: '09:00',
        highTime: '10:30',
        lowTime: null,
        sourceLabel: '会社四季報オンライン',
      },
    };
    const { rerender } = render(
      <ShikihoPanel
        symbol="7203"
        snapshot={quotedSnapshot}
        diagnostic={null}
        captureState="captured"
        isRefreshing={false}
        onRefresh={noop}
        onSelectSymbol={noop}
        provisionalProvenance={provisionalProvenance}
      />
    );

    expect(screen.getByText('四季報 15分遅延・当日暫定')).toBeInTheDocument();
    expect(screen.getByText('10:35')).toBeInTheDocument();
    const quote = screen.getByTestId('shikiho-quote');
    expect(quote).toHaveTextContent('現在値￥120');
    expect(quote).toHaveTextContent('始値￥112');
    expect(quote).toHaveTextContent('高値￥125');
    expect(quote).toHaveTextContent('安値￥110');
    expect(quote).toHaveTextContent('前日終値￥108');
    expect(quote).toHaveTextContent('出来高123,000');

    rerender(
      <ShikihoPanel
        symbol="7203"
        snapshot={quotedSnapshot}
        diagnostic={null}
        captureState="captured"
        isRefreshing={false}
        onRefresh={noop}
        onSelectSymbol={noop}
        provisionalProvenance={null}
      />
    );
    expect(screen.queryByText('四季報 15分遅延・当日暫定')).not.toBeInTheDocument();
    expect(screen.queryByTestId('shikiho-quote')).not.toBeInTheDocument();
  });

  test('wraps long captured tokens without overflowing the workbench', () => {
    renderPanel({ ...snapshot7203, features: 'x'.repeat(4096) });

    const body = screen.getByTestId('shikiho-body');
    expect(body).toHaveClass('min-w-0', '[overflow-wrap:anywhere]');
    expect(body).not.toHaveClass('overflow-hidden');
  });

  test('normalizes fallback source codes and omits the link for invalid symbols', () => {
    const { rerender } = render(
      <ShikihoPanel
        symbol="72030"
        snapshot={null}
        diagnostic={null}
        captureState="not_captured"
        isRefreshing={false}
        onRefresh={noop}
        onSelectSymbol={noop}
      />
    );

    expect(screen.getByRole('link', { name: /四季報で開く/ })).toHaveAttribute(
      'href',
      'https://shikiho.toyokeizai.net/stocks/7203'
    );

    rerender(
      <ShikihoPanel
        symbol="72A3"
        snapshot={null}
        diagnostic={null}
        captureState="not_captured"
        isRefreshing={false}
        onRefresh={noop}
        onSelectSymbol={noop}
      />
    );
    expect(screen.queryByRole('link', { name: /四季報で開く/ })).not.toBeInTheDocument();
  });

  test('resets expansion on symbol change and keeps later no-snapshot guidance visible', async () => {
    const { rerender } = render(
      <ShikihoPanel
        symbol="7203"
        snapshot={snapshot7203}
        diagnostic={null}
        captureState="captured"
        isRefreshing={false}
        onRefresh={noop}
        onSelectSymbol={noop}
      />
    );
    await userEvent.click(screen.getByRole('button', { name: /会社四季報を折りたたむ/ }));

    rerender(
      <ShikihoPanel
        symbol="6758"
        snapshot={{
          ...emptySnapshot,
          code: '6758',
          sourceUrl: 'https://shikiho.toyokeizai.net/stocks/6758',
          features: '新しい銘柄の特色',
        }}
        diagnostic={null}
        captureState="captured"
        isRefreshing={false}
        onRefresh={noop}
        onSelectSymbol={noop}
      />
    );

    expect(screen.getByText('新しい銘柄の特色')).toBeInTheDocument();
    const newSymbolCollapse = screen.getByRole('button', { name: /会社四季報を折りたたむ/ });
    expect(newSymbolCollapse).toHaveAttribute('aria-expanded', 'true');
    await userEvent.click(newSymbolCollapse);

    rerender(
      <ShikihoPanel
        symbol="6758"
        snapshot={null}
        diagnostic={null}
        captureState="checking_extension"
        isRefreshing={false}
        onRefresh={noop}
        onSelectSymbol={noop}
      />
    );

    expect(screen.getByText('Company Shikiho bridge の応答を待っています。')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /会社四季報を(折りたたむ|展開する)/ })).not.toBeInTheDocument();
  });

  test('does not offer collapse when a snapshot has no displayable content', () => {
    renderPanel(emptySnapshot);

    expect(screen.queryByRole('button', { name: /会社四季報を(折りたたむ|展開する)/ })).not.toBeInTheDocument();
  });

  test('refreshes on demand with an accessible compact action', async () => {
    const onRefresh = vi.fn();
    render(
      <ShikihoPanel
        symbol="7203"
        snapshot={snapshot7203}
        diagnostic={null}
        captureState="captured"
        isRefreshing={false}
        onRefresh={onRefresh}
        onSelectSymbol={noop}
      />
    );

    const refreshButton = screen.getByRole('button', { name: '会社四季報を更新' });
    expect(refreshButton).toHaveTextContent('更新');
    await userEvent.click(refreshButton);
    expect(onRefresh).toHaveBeenCalledOnce();
  });

  test('keeps snapshot content and metadata visible while refreshing', () => {
    render(
      <ShikihoPanel
        symbol="7203"
        snapshot={snapshot7203}
        diagnostic={null}
        captureState="captured"
        isRefreshing
        onRefresh={noop}
        onSelectSymbol={noop}
      />
    );

    expect(screen.getByRole('button', { name: '会社四季報を更新' })).toBeDisabled();
    expect(screen.getByRole('status')).toHaveTextContent('取得中');
    expect(screen.getByText(/4輪世界首位/)).toBeInTheDocument();
    expect(screen.getByText('2026年3集')).toBeInTheDocument();
    expect(screen.getByText(/取得 .*2026/)).toBeInTheDocument();
    expect(screen.getByRole('link', { name: /四季報で開く/ })).toBeInTheDocument();
  });

  test('uses a full-width primary column when secondary content is absent', () => {
    renderPanel({ ...emptySnapshot, features: '主情報のみ' });

    expect(screen.getByTestId('shikiho-body')).not.toHaveClass('lg:grid-cols-[minmax(0,2fr)_minmax(16rem,1fr)]');
    expect(screen.getByTestId('shikiho-primary')).not.toHaveClass('lg:border-r');
    expect(screen.queryByTestId('shikiho-secondary')).not.toBeInTheDocument();
  });

  test('uses a full-width secondary column when primary content is absent', () => {
    renderPanel({ ...emptySnapshot, industries: ['自動車'] });

    expect(screen.getByTestId('shikiho-body')).not.toHaveClass('lg:grid-cols-[minmax(0,2fr)_minmax(16rem,1fr)]');
    expect(screen.queryByTestId('shikiho-primary')).not.toBeInTheDocument();
    expect(screen.getByTestId('shikiho-secondary')).toBeInTheDocument();
  });

  test.each([
    ['checking_extension', '拡張機能を確認中'],
    ['extension_unavailable', '拡張機能が見つかりません'],
    ['not_captured', '未取得'],
  ] as const)('renders %s without snapshot content', (captureState, statusText) => {
    renderPanel(null, captureState);

    expect(screen.getByRole('status')).toHaveTextContent(statusText);
    expect(screen.queryByText('特色')).not.toBeInTheDocument();
    expect(screen.getByRole('link', { name: /四季報で開く/ })).toHaveAttribute(
      'href',
      'https://shikiho.toyokeizai.net/stocks/7203'
    );
  });

  test.each([
    ['partial', '一部取得'],
    ['stale', '古いスナップショット'],
  ] as const)('renders %s snapshot metadata and a mobile-safe source link', (captureState, statusText) => {
    renderPanel({ ...snapshot7203, status: 'partial' }, captureState);

    expect(screen.getByText(statusText)).toBeInTheDocument();
    expect(screen.getByText('2026年3集')).toBeInTheDocument();
    expect(screen.getByText(/取得 .*2026/)).toBeInTheDocument();
    expect(screen.getByRole('link', { name: /四季報で開く/ })).toHaveAttribute('target', '_blank');
  });

  test('prefers the edition label over the page update timestamp', () => {
    renderPanel(snapshot7203);

    expect(screen.getByTestId('shikiho-edition-meta')).toHaveTextContent('2026年3集');
    expect(screen.getByTestId('shikiho-edition-meta')).not.toHaveTextContent('更新');
  });

  test('falls back to the page update timestamp when edition is missing', () => {
    renderPanel({ ...snapshot7203, editionLabel: null });

    expect(screen.getByTestId('shikiho-edition-meta')).toHaveTextContent(/更新 .*2026/);
  });

  test('omits edition metadata when edition and page update are missing', () => {
    renderPanel({ ...snapshot7203, editionLabel: null, pageUpdatedAt: null });

    expect(screen.queryByTestId('shikiho-edition-meta')).not.toBeInTheDocument();
  });

  test.each([
    ['login_required', 'ログインが必要です'],
    ['page_changed', 'ページ構造の変更を検知しました'],
    ['storage_error', '保存エラー'],
  ] as const)('renders the %s capture diagnostic', (captureState, message) => {
    const diagnostic: ShikihoCaptureDiagnosticV1 = {
      schemaVersion: 1,
      code: '7203',
      observedAt: '2026-07-10T02:02:03.000Z',
      status: captureState,
    };

    renderPanel(null, captureState, diagnostic);

    expect(screen.getByText(message)).toBeInTheDocument();
  });

  test('omits empty optional sections', () => {
    renderPanel(emptySnapshot);

    expect(screen.queryByText('特色')).not.toBeInTheDocument();
    expect(screen.queryByText('四季報スコア')).not.toBeInTheDocument();
    expect(screen.queryByText('比較会社')).not.toBeInTheDocument();
    expect(screen.getByRole('link', { name: /四季報で開く/ })).toBeInTheDocument();
  });
});
