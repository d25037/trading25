/* @vitest-environment jsdom */

import type { ApiFundamentalDataPoint } from '@trading25/shared/types/api-types';
import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import { FundamentalsSummaryCard } from './FundamentalsSummaryCard';

const baseMetrics: ApiFundamentalDataPoint = {
  date: '2024-03-31',
  disclosedDate: '2024-05-15',
  periodType: 'FY',
  isConsolidated: true,
  accountingStandard: 'JGAAP',
  roe: 13.3,
  eps: 300,
  dilutedEps: 290,
  bps: 2250,
  per: 20,
  pbr: 2.7,
  roa: 4.4,
  operatingMargin: 11.1,
  netMargin: 8.9,
  stockPrice: 6100,
  netProfit: 4_000_000,
  equity: 30_000_000,
  totalAssets: 90_000_000,
  netSales: 45_000_000,
  operatingProfit: 5_000_000,
  cashFlowOperating: 6_000_000,
  cashFlowInvesting: -2_000_000,
  cashFlowFinancing: -1_000_000,
  cashAndEquivalents: 8_000_000,
  fcf: 4_000_000,
  fcfYield: 0.5,
  fcfMargin: 8.9,
  cfoToNetProfitRatio: 1.5,
  tradingValueToMarketCapRatio: 0.12,
  forecastEps: 350,
  forecastEpsChangeRate: 16.7,
  revisedForecastEps: null,
  revisedForecastSource: null,
  prevCashFlowOperating: null,
  prevCashFlowInvesting: null,
  prevCashFlowFinancing: null,
  prevCashAndEquivalents: null,
};

describe('FundamentalsSummaryCard', () => {
  it('renders empty state when metrics are missing', () => {
    render(<FundamentalsSummaryCard metrics={undefined} />);
    expect(screen.getByText('No data available')).toBeInTheDocument();
  });

  it('renders new ratio row with custom trading value period', () => {
    render(<FundamentalsSummaryCard metrics={baseMetrics} tradingValuePeriod={20} />);

    expect(screen.getByText('営業CF/純利益')).toBeInTheDocument();
    expect(screen.getByText('20日売買代金/時価総額')).toBeInTheDocument();
    expect(screen.getByText('1.50x')).toBeInTheDocument();
    expect(screen.getByText('0.12x')).toBeInTheDocument();
  });

  it('uses 15-day label by default', () => {
    render(<FundamentalsSummaryCard metrics={baseMetrics} />);
    expect(screen.getByText('15日売買代金/時価総額')).toBeInTheDocument();
  });

  it('renders adjusted EPS context and previous cash flow value', () => {
    const metrics: ApiFundamentalDataPoint = {
      ...baseMetrics,
      isConsolidated: false,
      accountingStandard: null,
      stockPrice: null,
      adjustedEps: 320,
      adjustedForecastEps: 280,
      adjustedBps: 2300,
      forecastEpsChangeRate: -12.5,
      prevCashFlowOperating: 200,
    };

    render(<FundamentalsSummaryCard metrics={metrics} />);

    expect(screen.getByText('320')).toBeInTheDocument();
    expect(screen.getByText('予: 280')).toBeInTheDocument();
    expect(screen.getByText('(-12.5%)')).toBeInTheDocument();
    expect(screen.getByText('(2億)')).toBeInTheDocument();
    expect(screen.getByText(/単体 \/ JGAAP/)).toBeInTheDocument();
    expect(screen.queryByText(/株価 @ 開示日/)).not.toBeInTheDocument();
  });

  it('does not render EPS forecast block when forecast values are missing', () => {
    const metrics: ApiFundamentalDataPoint = {
      ...baseMetrics,
      forecastEps: null,
      adjustedForecastEps: null,
      forecastEpsChangeRate: 0,
    };

    render(<FundamentalsSummaryCard metrics={metrics} />);
    expect(screen.queryByText(/予:/)).not.toBeInTheDocument();
  });
});
