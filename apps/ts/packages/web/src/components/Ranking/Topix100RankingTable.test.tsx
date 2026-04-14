import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';
import type { Topix100RankingResponse } from '@/types/ranking';
import { Topix100RankingTable } from './Topix100RankingTable';

function createIntradayResponse(): Topix100RankingResponse {
  return {
    date: '2026-03-30',
    studyMode: 'intraday',
    rankingMetric: 'price_vs_sma_gap',
    smaWindow: 50,
    shortWindowStreaks: 3,
    longWindowStreaks: 53,
    longScoreHorizonDays: 5,
    shortScoreHorizonDays: 1,
    scoreTarget: 'next_session_open_close',
    intradayScoreTarget: 'next_session_open_close',
    scoreModelType: 'daily_refit',
    scoreTrainWindowDays: 756,
    scoreTestWindowDays: 1,
    scoreStepDays: 1,
    scoreSplitTrainStart: '2023-01-04',
    scoreSplitTrainEnd: '2025-12-30',
    scoreSplitTestStart: null,
    scoreSplitTestEnd: null,
    scoreSplitPartialTail: false,
    scoreSourceRunId: '20260406_180623_c0eb7f87',
    primaryBenchmark: null,
    secondaryBenchmark: null,
    primaryBenchmarkReturn: null,
    secondaryBenchmarkReturn: null,
    benchmarkEntryDate: null,
    benchmarkExitDate: null,
    itemCount: 4,
    lastUpdated: '2026-03-30T00:00:00Z',
    items: [
      {
        rank: 1,
        code: '7203',
        companyName: 'Toyota',
        marketCode: 'prime',
        sector33Name: 'Transport',
        scaleCategory: 'TOPIX Core30',
        currentPrice: 3000,
        volume: 1_000_000,
        priceVsSmaGap: 0.12,
        priceSma20_80: 1.23,
        volumeSma5_20: 1.11,
        priceDecile: 1,
        priceBucket: 'q1',
        intradayScore: -0.0032,
        intradayLongRank: 3,
        intradayShortRank: 2,
        nextSessionDate: '2026-03-31',
        nextSessionIntradayReturn: 0.011,
      },
      {
        rank: 2,
        code: '6758',
        companyName: 'Sony',
        marketCode: 'prime',
        sector33Name: 'Electronics',
        scaleCategory: 'TOPIX Large70',
        currentPrice: 12000,
        volume: 800_000,
        priceVsSmaGap: -0.08,
        priceSma20_80: 0.95,
        volumeSma5_20: 0.88,
        priceDecile: 10,
        priceBucket: 'q10',
        intradayScore: 0.0125,
        intradayLongRank: 1,
        intradayShortRank: 4,
        nextSessionDate: '2026-03-31',
        nextSessionIntradayReturn: -0.004,
      },
      {
        rank: 3,
        code: '9432',
        companyName: 'NTT',
        marketCode: 'prime',
        sector33Name: 'Telecom',
        scaleCategory: 'TOPIX Mid400',
        currentPrice: 150,
        volume: 2_000_000,
        priceVsSmaGap: 0.01,
        priceSma20_80: 1.01,
        volumeSma5_20: 1.05,
        priceDecile: 3,
        priceBucket: 'q234',
        intradayScore: 0.0041,
        intradayLongRank: 2,
        intradayShortRank: 3,
        nextSessionDate: '2026-03-31',
        nextSessionIntradayReturn: 0.006,
      },
      {
        rank: 4,
        code: '8306',
        companyName: 'MUFG',
        marketCode: 'prime',
        sector33Name: 'Banks',
        scaleCategory: 'TOPIX Mid400',
        currentPrice: 1800,
        volume: 1_500_000,
        priceVsSmaGap: -0.01,
        priceSma20_80: 0.99,
        volumeSma5_20: 1.02,
        priceDecile: 7,
        priceBucket: 'other',
        intradayScore: null,
        intradayLongRank: null,
        intradayShortRank: null,
        nextSessionDate: null,
        nextSessionIntradayReturn: null,
      },
    ],
  };
}

function createSwingResponse(): Topix100RankingResponse {
  return {
    ...createIntradayResponse(),
    studyMode: 'swing_5d',
    scoreTarget: 'next_session_open_to_open_5d',
    intradayScoreTarget: 'next_session_open_to_open_5d',
    primaryBenchmark: 'topix',
    secondaryBenchmark: 'topix100_universe',
    primaryBenchmarkReturn: 0.012,
    secondaryBenchmarkReturn: 0.01,
    benchmarkEntryDate: '2026-03-31',
    benchmarkExitDate: '2026-04-07',
    items: [
      {
        rank: 1,
        code: '7203',
        companyName: 'Toyota',
        marketCode: 'prime',
        sector33Name: 'Transport',
        scaleCategory: 'TOPIX Core30',
        currentPrice: 3000,
        volume: 1_000_000,
        priceVsSmaGap: 0.12,
        priceSma20_80: 1.23,
        volumeSma5_20: 1.11,
        priceDecile: 1,
        priceBucket: 'q1',
        longScore5d: 0.015,
        longScore5dRank: 1,
        swingEntryDate: '2026-03-31',
        swingExitDate: '2026-04-07',
        openToOpen5dReturn: 0.025,
      },
      {
        rank: 2,
        code: '6758',
        companyName: 'Sony',
        marketCode: 'prime',
        sector33Name: 'Electronics',
        scaleCategory: 'TOPIX Large70',
        currentPrice: 12000,
        volume: 800_000,
        priceVsSmaGap: -0.08,
        priceSma20_80: 0.95,
        volumeSma5_20: 0.88,
        priceDecile: 10,
        priceBucket: 'q10',
        longScore5d: 0.011,
        longScore5dRank: 2,
        swingEntryDate: '2026-03-31',
        swingExitDate: '2026-04-07',
        openToOpen5dReturn: 0.018,
      },
      {
        rank: 3,
        code: '9432',
        companyName: 'NTT',
        marketCode: 'prime',
        sector33Name: 'Telecom',
        scaleCategory: 'TOPIX Mid400',
        currentPrice: 150,
        volume: 2_000_000,
        priceVsSmaGap: 0.01,
        priceSma20_80: 1.01,
        volumeSma5_20: 1.05,
        priceDecile: 3,
        priceBucket: 'q234',
        longScore5d: 0.009,
        longScore5dRank: 3,
        swingEntryDate: '2026-03-31',
        swingExitDate: '2026-04-07',
        openToOpen5dReturn: 0.014,
      },
    ],
  };
}

describe('Topix100RankingTable', () => {
  it('renders current decile-only columns and omits removed state columns', () => {
    render(
      <Topix100RankingTable
        data={createIntradayResponse()}
        isLoading={false}
        error={null}
        onStockClick={vi.fn()}
        studyMode="intraday"
        rankingMetric="price_vs_sma_gap"
        rankingSmaWindow={50}
        priceBucketFilter="all"
        sortBy="rank"
        sortOrder="asc"
        onSortChange={vi.fn()}
      />
    );

    expect(screen.getByText('Bucket')).toBeInTheDocument();
    expect(screen.queryByText('Vol Split')).not.toBeInTheDocument();
    expect(screen.queryByText('Short')).not.toBeInTheDocument();
    expect(screen.queryByText('Long')).not.toBeInTheDocument();
    expect(screen.getByText('q1')).toBeInTheDocument();
  });

  it('filters by price bucket only', () => {
    render(
      <Topix100RankingTable
        data={createIntradayResponse()}
        isLoading={false}
        error={null}
        onStockClick={vi.fn()}
        studyMode="intraday"
        rankingMetric="price_vs_sma_gap"
        rankingSmaWindow={50}
        priceBucketFilter="q10"
        sortBy="rank"
        sortOrder="asc"
        onSortChange={vi.fn()}
      />
    );

    expect(screen.getByText('Sony')).toBeInTheDocument();
    expect(screen.queryByText('Toyota')).not.toBeInTheDocument();
    expect(screen.queryByText('NTT')).not.toBeInTheDocument();
  });

  it('sorts by intraday score descending', async () => {
    const user = userEvent.setup();
    const onSortChange = vi.fn();
    render(
      <Topix100RankingTable
        data={createIntradayResponse()}
        isLoading={false}
        error={null}
        onStockClick={vi.fn()}
        studyMode="intraday"
        rankingMetric="price_vs_sma_gap"
        rankingSmaWindow={50}
        priceBucketFilter="all"
        sortBy="rank"
        sortOrder="asc"
        onSortChange={onSortChange}
      />
    );

    await user.click(screen.getByRole('button', { name: /ID Score/i }));
    expect(onSortChange).toHaveBeenCalledWith('intradayScore', 'desc');
  });

  it('renders swing benchmark summary copy', () => {
    render(
      <Topix100RankingTable
        data={createSwingResponse()}
        isLoading={false}
        error={null}
        onStockClick={vi.fn()}
        studyMode="swing_5d"
        rankingMetric="price_vs_sma_gap"
        rankingSmaWindow={50}
        priceBucketFilter="all"
        sortBy="longScore5d"
        sortOrder="desc"
        onSortChange={vi.fn()}
      />
    );

    expect(screen.getByText('KPI = vs TOPIX')).toBeInTheDocument();
    expect(screen.getAllByText(/vs TOPIX100 EW/).length).toBeGreaterThan(0);
    expect(screen.getAllByText(/2026-03-31/).length).toBeGreaterThan(0);
    expect(screen.getAllByText(/2026-04-07/).length).toBeGreaterThan(0);
  });
});
