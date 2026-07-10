import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import type { ShikihoCaptureDiagnosticV1, ShikihoSnapshotV1 } from '@trading25/shikiho-extension/contract';
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
      onSelectSymbol={noop}
    />
  );
}

describe('ShikihoPanel', () => {
  test('renders a compact captured snapshot and comparison navigation', async () => {
    const onSelectSymbol = vi.fn();
    render(
      <ShikihoPanel
        symbol="7203"
        snapshot={snapshot7203}
        diagnostic={null}
        captureState="captured"
        onSelectSymbol={onSelectSymbol}
      />
    );

    expect(screen.getByText('特色')).toBeInTheDocument();
    expect(screen.getByText(/4輪世界首位/)).toBeInTheDocument();
    expect(screen.getByText('連結事業')).toBeInTheDocument();
    expect(screen.getByText('会社四季報')).toBeInTheDocument();
    expect(screen.getByTestId('shikiho-body')).toHaveClass('lg:grid-cols-[minmax(0,2fr)_minmax(16rem,1fr)]');
    expect(screen.getByTestId('shikiho-primary')).toHaveClass('lg:border-r');
    expect(screen.getByTestId('shikiho-secondary')).toBeInTheDocument();
    await userEvent.click(screen.getByRole('button', { name: /7201 日産自動車/ }));
    expect(onSelectSymbol).toHaveBeenCalledWith('7201');
    expect(screen.getByText('海外メーカー')).toBeInTheDocument();
  });

  test('renders source text literally and supports collapse', async () => {
    renderPanel({ ...snapshot7203, features: '<img src=x onerror=alert(1)>' });

    expect(screen.getByText('<img src=x onerror=alert(1)>')).toBeInTheDocument();
    expect(document.querySelector('img')).toBeNull();
    const collapseButton = screen.getByRole('button', { name: /会社四季報を折りたたむ/ });
    expect(collapseButton).toHaveAttribute('aria-controls');
    expect(document.getElementById(collapseButton.getAttribute('aria-controls') ?? '')).toBeInTheDocument();
    await userEvent.click(collapseButton);
    expect(screen.queryByText('特色')).not.toBeInTheDocument();
    expect(screen.getByRole('link', { name: /四季報で開く/ })).toBeInTheDocument();
  });

  test('normalizes fallback source codes and omits the link for invalid symbols', () => {
    const { rerender } = render(
      <ShikihoPanel
        symbol="72030"
        snapshot={null}
        diagnostic={null}
        captureState="not_captured"
        onSelectSymbol={noop}
      />
    );

    expect(screen.getByRole('link', { name: /四季報で開く/ })).toHaveAttribute(
      'href',
      'https://shikiho.toyokeizai.net/stocks/7203'
    );

    rerender(
      <ShikihoPanel symbol="720A" snapshot={null} diagnostic={null} captureState="not_captured" onSelectSymbol={noop} />
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
        onSelectSymbol={noop}
      />
    );

    expect(screen.getByText('Company Shikiho bridge の応答を待っています。')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /会社四季報を/ })).not.toBeInTheDocument();
  });

  test('does not offer collapse when a snapshot has no displayable content', () => {
    renderPanel(emptySnapshot);

    expect(screen.queryByRole('button', { name: /会社四季報を/ })).not.toBeInTheDocument();
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
