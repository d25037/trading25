import { render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import type { MarketRankingSymbolResponse } from '@trading25/contracts/types/api-response-types';
import { describe, expect, it, vi } from 'vitest';
import type { StockInfoResponse } from '@/hooks/useStockInfo';
import { DailyRankingSnapshot } from './DailyRankingSnapshot';
import { DAILY_RANKING_VALUE_METRICS } from './dailyRankingPresentation';

const stockInfo: StockInfoResponse = {
  code: '7203',
  companyName: 'Toyota Motor',
  companyNameEnglish: 'Toyota Motor Corp.',
  listedDate: '1949-05-16',
  marketCode: '0111',
  marketName: 'Prime',
  scaleCategory: 'TOPIX Core30',
  sector17Code: '6',
  sector17Name: 'Automobiles & Transportation Equipment',
  sector33Code: '3700',
  sector33Name: 'Stock Info Sector',
};

const completeResponse: MarketRankingSymbolResponse = {
  date: '2026-07-09',
  item: {
    rank: 3,
    code: '72030',
    companyName: 'Toyota Motor',
    marketCode: 'standard',
    sector33Name: 'Ranking Sector',
    sectorStrengthScore: 0.9,
    currentPrice: 3000,
    volume: 1_000_000,
    changePercentage: 2.345,
    sma5AboveCount5d: 4,
    per: 8,
    perPercentile: 0.15,
    forwardPer: 6,
    forwardPerPercentile: 0.15,
    forecastOperatingProfitGrowthRatio: 1.25,
    psr: 2,
    psrPercentile: 0.95,
    forwardPsr: 1.5,
    forwardPsrPercentile: 0.85,
    pbr: 0.5,
    pbrPercentile: 0.05,
    valueCompositeScore: 0.92,
    marketCap: 2_000_000_000,
    liquidityResidualZ: -1.4,
    liquidityRegime: 'neutral_rerating',
    tradingValue: 1_500_000_000,
    riskFlags: ['overheat'],
    technicalFlags: ['momentum_20_60_top20'],
  },
  lastUpdated: '2026-07-10T00:00:00Z',
};

const defaultProps = {
  response: completeResponse,
  isLoading: false,
  error: null,
  onRetry: vi.fn(),
  stockInfo,
  latestMarketCaps: { freeFloat: 800_000_000, issuedShares: 1_000_000_000 },
};

describe('DailyRankingSnapshot', () => {
  it('renders the complete ranking snapshot with ranking-owned basic values', () => {
    render(<DailyRankingSnapshot {...defaultProps} />);

    expect(screen.getByRole('region', { name: 'Daily Ranking Snapshot' })).toBeInTheDocument();
    expect(screen.queryByRole('heading', { name: 'Daily Ranking Snapshot' })).not.toBeInTheDocument();
    expect(screen.getByText('2026-07-09')).toBeInTheDocument();

    const basicInfo = screen.getByTestId('daily-ranking-basic-info');
    expect(within(basicInfo).getByText('Mkt').parentElement).toHaveTextContent('Standard');
    expect(within(basicInfo).getByText('Index').parentElement).toHaveTextContent('Core30');
    expect(within(basicInfo).getByText('S17').parentElement).toHaveTextContent(
      'Automobiles & Transportation Equipment'
    );
    expect(within(basicInfo).getByText('S33').parentElement).toHaveTextContent('Ranking Sector');
    expect(within(basicInfo).getByText('MCap').parentElement).toHaveTextContent('20.0億円');
    expect(within(basicInfo).getByText('FF MCap').parentElement).toHaveTextContent('8.0億円');
    expect(basicInfo).not.toHaveTextContent('Stock Info Sector');
    expect(basicInfo).not.toHaveTextContent('10.0億円');

    for (const { label } of DAILY_RANKING_VALUE_METRICS) {
      expect(screen.getByText(label)).toBeInTheDocument();
    }
    expect(screen.getByText('0.90')).toHaveClass('text-green-700');
    expect(screen.getByText('¥3,000')).toBeInTheDocument();
    expect(screen.getByText('+2.35%')).toHaveClass('text-green-600');
    const psrPair = screen.getByTestId('daily-ranking-psr-pair');
    expect(within(psrPair).getByText('PSR')).toBeInTheDocument();
    expect(within(psrPair).getByText('Fwd PSR')).toBeInTheDocument();
    expect(psrPair).toHaveTextContent('2.00x');
    expect(psrPair).toHaveTextContent('1.50x');
    expect(screen.getByText('1.50B')).toBeInTheDocument();
    expect(screen.getByText('Neutral Rerating')).toHaveClass('text-green-700');
    expect(screen.getByText('Deep Value')).toHaveClass('text-green-700');
    expect(screen.getByText('Overheat')).toHaveClass('text-purple-700');
    expect(screen.getByText('20/60D Mom')).toHaveClass('text-sky-700');
  });

  it('keeps supplemental basic information visible when ranking is unavailable', () => {
    render(
      <DailyRankingSnapshot
        {...defaultProps}
        response={{ date: '2026-07-09', item: null, lastUpdated: '2026-07-10T00:00:00Z' }}
      />
    );

    expect(screen.getByText('2026-07-09')).toBeInTheDocument();
    const unavailableStatus = screen.getByRole('status');
    expect(unavailableStatus).toHaveAttribute('aria-live', 'polite');
    expect(unavailableStatus).toHaveTextContent('Daily Ranking data unavailable');
    const basicInfo = screen.getByTestId('daily-ranking-basic-info');
    expect(within(basicInfo).getByText('Mkt').parentElement).toHaveTextContent('Prime');
    expect(within(basicInfo).getByText('S33').parentElement).toHaveTextContent('Stock Info Sector');
    expect(within(basicInfo).getByText('MCap').parentElement).toHaveTextContent('10.0億円');
  });

  it('keeps partially missing ranking metrics visible without fundamentals substitution', () => {
    const response: MarketRankingSymbolResponse = {
      ...completeResponse,
      item: completeResponse.item
        ? {
            ...completeResponse.item,
            per: null,
            forwardPer: null,
            marketCap: null,
          }
        : null,
    };
    render(<DailyRankingSnapshot {...defaultProps} response={response} />);

    const metrics = screen.getByTestId('daily-ranking-metrics');
    expect(within(metrics).getByText('PER').parentElement).toHaveTextContent('-');
    expect(within(metrics).getByText('Fwd PER').parentElement).toHaveTextContent('-');
    expect(screen.getByTestId('daily-ranking-basic-info')).toHaveTextContent('MCap-');
    expect(screen.getByTestId('daily-ranking-basic-info')).not.toHaveTextContent('10.0億円');
  });

  it('renders a compact loading state while keeping basic information', () => {
    render(<DailyRankingSnapshot {...defaultProps} response={undefined} isLoading />);

    const loadingStatus = screen.getByRole('status');
    expect(loadingStatus).toHaveAttribute('aria-live', 'polite');
    expect(loadingStatus).toHaveTextContent('Loading Daily Ranking data…');
    expect(screen.queryByText('Loading Daily Ranking data...')).not.toBeInTheDocument();
    expect(screen.getByTestId('daily-ranking-basic-info')).toHaveTextContent('Core30');
  });

  it('renders an error with a retry action', async () => {
    const user = userEvent.setup();
    const onRetry = vi.fn();
    render(<DailyRankingSnapshot {...defaultProps} response={undefined} error={new Error('boom')} onRetry={onRetry} />);

    const alert = screen.getByRole('alert');
    expect(alert).toHaveTextContent('Unable to load Daily Ranking data');
    expect(alert).toHaveTextContent('boom');
    expect(screen.getByText('boom')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Retry' }));
    expect(onRetry).toHaveBeenCalledTimes(1);
  });

  it('uses a wrapping metadata line and two-row desktop metric strip', () => {
    render(<DailyRankingSnapshot {...defaultProps} />);

    expect(screen.getByTestId('daily-ranking-basic-info')).toHaveClass('flex', 'flex-wrap');
    expect(screen.getByTestId('daily-ranking-metrics')).toHaveClass('grid-cols-2', 'lg:grid-cols-7');
    expect(screen.getByTestId('daily-ranking-regime')).not.toHaveClass('col-span-full');
    expect(screen.getByTestId('daily-ranking-signals')).not.toHaveClass('col-span-full');
  });

  it('labels provisional ranking metrics only when Shikiho provenance is active', () => {
    const provenance = {
      provisional: true as const,
      tradingDate: '2026-07-13',
      observedAt: '2026-07-13T01:35:00.000Z',
      delayMinutes: 15 as const,
      sourceLabel: '会社四季報オンライン' as const,
    };
    const { rerender } = render(<DailyRankingSnapshot {...defaultProps} provisionalProvenance={provenance} />);

    expect(screen.getByText('四季報 15分遅延・当日暫定')).toBeInTheDocument();
    expect(screen.getByText('四季報 15分遅延・当日暫定').getAttribute('aria-label')).toContain('暫定');

    rerender(<DailyRankingSnapshot {...defaultProps} provisionalProvenance={null} />);
    expect(screen.queryByText('四季報 15分遅延・当日暫定')).not.toBeInTheDocument();
  });
});
