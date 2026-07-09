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

    expect(screen.getByText('Daily Ranking Snapshot')).toBeInTheDocument();
    expect(screen.getByText('As of 2026-07-09')).toBeInTheDocument();

    const basicInfo = screen.getByTestId('daily-ranking-basic-info');
    expect(within(basicInfo).getByText('Market').parentElement).toHaveTextContent('Standard');
    expect(within(basicInfo).getByText('Index Membership').parentElement).toHaveTextContent('Core30');
    expect(within(basicInfo).getByText('Sector 17').parentElement).toHaveTextContent(
      'Automobiles & Transportation Equipment'
    );
    expect(within(basicInfo).getByText('Sector 33').parentElement).toHaveTextContent('Ranking Sector');
    expect(within(basicInfo).getByText('Market Cap').parentElement).toHaveTextContent('20.0億円');
    expect(within(basicInfo).getByText('Free-Float Market Cap').parentElement).toHaveTextContent('8.0億円');
    expect(basicInfo).not.toHaveTextContent('Stock Info Sector');
    expect(basicInfo).not.toHaveTextContent('10.0億円');

    for (const { label } of DAILY_RANKING_VALUE_METRICS) {
      expect(screen.getByText(label)).toBeInTheDocument();
    }
    expect(screen.getByText('0.90')).toHaveClass('text-green-700');
    expect(screen.getByText('￥3,000')).toBeInTheDocument();
    expect(screen.getByText('+2.35%')).toHaveClass('text-green-600');
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

    expect(screen.getByText('As of 2026-07-09')).toBeInTheDocument();
    expect(screen.getByText('Daily Ranking data unavailable')).toBeInTheDocument();
    const basicInfo = screen.getByTestId('daily-ranking-basic-info');
    expect(within(basicInfo).getByText('Market').parentElement).toHaveTextContent('Prime');
    expect(within(basicInfo).getByText('Sector 33').parentElement).toHaveTextContent('Stock Info Sector');
    expect(within(basicInfo).getByText('Market Cap').parentElement).toHaveTextContent('10.0億円');
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
    expect(screen.getByTestId('daily-ranking-basic-info')).toHaveTextContent('Market Cap-');
    expect(screen.getByTestId('daily-ranking-basic-info')).not.toHaveTextContent('10.0億円');
  });

  it('renders a compact loading state while keeping basic information', () => {
    render(<DailyRankingSnapshot {...defaultProps} response={undefined} isLoading />);

    expect(screen.getByText('Loading Daily Ranking data...')).toBeInTheDocument();
    expect(screen.getByTestId('daily-ranking-basic-info')).toHaveTextContent('Core30');
  });

  it('renders an error with a retry action', async () => {
    const user = userEvent.setup();
    const onRetry = vi.fn();
    render(<DailyRankingSnapshot {...defaultProps} response={undefined} error={new Error('boom')} onRetry={onRetry} />);

    expect(screen.getByText('Unable to load Daily Ranking data')).toBeInTheDocument();
    expect(screen.getByText('boom')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Retry' }));
    expect(onRetry).toHaveBeenCalledTimes(1);
  });

  it('uses a two-column mobile grid and full-row Regime and Signals groups', () => {
    render(<DailyRankingSnapshot {...defaultProps} />);

    expect(screen.getByTestId('daily-ranking-basic-info')).toHaveClass('grid-cols-2');
    expect(screen.getByTestId('daily-ranking-metrics')).toHaveClass('grid-cols-2');
    expect(screen.getByTestId('daily-ranking-regime')).toHaveClass('col-span-2');
    expect(screen.getByTestId('daily-ranking-signals')).toHaveClass('col-span-2');
  });
});
