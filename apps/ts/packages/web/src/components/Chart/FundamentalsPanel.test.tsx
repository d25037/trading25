/* @vitest-environment jsdom */

import { render, screen } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { FundamentalsPanel } from './FundamentalsPanel';

const mockUseFundamentals = vi.fn();
const mockSummaryCard = vi.fn();

vi.mock('@/hooks/useFundamentals', () => ({
  useFundamentals: (...args: unknown[]) => mockUseFundamentals(...args),
}));

vi.mock('./FundamentalsSummaryCard', () => ({
  FundamentalsSummaryCard: (props: unknown) => {
    mockSummaryCard(props);
    return <div>Summary Card</div>;
  },
}));

describe('FundamentalsPanel', () => {
  beforeEach(() => {
    mockUseFundamentals.mockReset();
    mockSummaryCard.mockReset();
  });

  it('renders placeholder when symbol is null', () => {
    mockUseFundamentals.mockReturnValue({ data: null, isLoading: false, error: null });

    render(<FundamentalsPanel symbol={null} />);

    expect(screen.getByText('銘柄を選択してください')).toBeInTheDocument();
  });

  it('forwards enabled option to useFundamentals', () => {
    mockUseFundamentals.mockReturnValue({ data: null, isLoading: false, error: null });

    render(<FundamentalsPanel symbol="7203" enabled={false} />);

    expect(mockUseFundamentals).toHaveBeenCalledWith('7203', { enabled: false, tradingValuePeriod: 15 });
  });

  it('forwards custom tradingValuePeriod to useFundamentals and summary card', () => {
    mockUseFundamentals.mockReturnValue({
      data: {
        data: [
          {
            date: '2024-03-31',
            periodType: 'FY',
            adjustedEps: 100,
            eps: 95,
            adjustedForecastEps: 120,
            forecastEps: 110,
            cashFlowOperating: 100,
            cashFlowInvesting: -50,
            cashFlowFinancing: -20,
            cashAndEquivalents: 500,
            netProfit: 200,
            equity: 1000,
          },
        ],
        latestMetrics: {},
        dailyValuation: [],
        tradingValuePeriod: 30,
      },
      isLoading: false,
      error: null,
    });

    render(<FundamentalsPanel symbol="7203" tradingValuePeriod={30} />);

    expect(mockUseFundamentals).toHaveBeenCalledWith('7203', { enabled: true, tradingValuePeriod: 30 });
    expect(mockSummaryCard.mock.calls.at(-1)?.[0]).toMatchObject({ tradingValuePeriod: 30 });
  });

  it('renders loading and normalized error states', () => {
    mockUseFundamentals.mockReturnValue({ data: null, isLoading: true, error: null });
    const { rerender } = render(<FundamentalsPanel symbol="7203" />);
    expect(screen.getByText('Loading fundamentals data...')).toBeInTheDocument();

    mockUseFundamentals.mockReturnValue({ data: null, isLoading: false, error: 'string error' });
    rerender(<FundamentalsPanel symbol="7203" />);
    expect(screen.getByText('Failed to load fundamentals data')).toBeInTheDocument();
  });

  it('renders Error instance message as-is', () => {
    mockUseFundamentals.mockReturnValue({
      data: null,
      isLoading: false,
      error: new Error('API unavailable'),
    });

    render(<FundamentalsPanel symbol="7203" />);
    expect(screen.getByText('API unavailable')).toBeInTheDocument();
  });

  it('renders empty state when there is no fundamentals data', () => {
    mockUseFundamentals.mockReturnValue({
      data: { data: [] },
      isLoading: false,
      error: null,
    });

    render(<FundamentalsPanel symbol="7203" />);
    expect(screen.getByText('No fundamentals data available')).toBeInTheDocument();
  });

  it('builds latest FY metrics from latestMetrics and daily valuation', () => {
    mockUseFundamentals.mockReturnValue({
      data: {
        data: [
          {
            date: '2024-03-31',
            periodType: 'FY',
            adjustedEps: 100,
            eps: 95,
            adjustedForecastEps: 120,
            forecastEps: 110,
            adjustedBps: 2200,
            bps: 2100,
            cashFlowOperating: 100,
            cashFlowInvesting: -50,
            cashFlowFinancing: -20,
            cashAndEquivalents: 500,
            netProfit: 200,
            equity: 1000,
          },
        ],
        latestMetrics: {
          forecastEps: 130,
          adjustedForecastEps: 150,
          forecastEpsChangeRate: 999,
          prevCashFlowOperating: 90,
          prevCashFlowInvesting: -40,
          prevCashFlowFinancing: -10,
          prevCashAndEquivalents: 450,
          cfoToNetProfitRatio: 0.45,
          tradingValueToMarketCapRatio: 33.333333,
        },
        dailyValuation: [{ per: 18, pbr: 1.4, close: 2500, marketCap: 1000000000 }],
        tradingValuePeriod: 20,
      },
      isLoading: false,
      error: null,
    });

    render(<FundamentalsPanel symbol="7203" />);

    expect(screen.getByText('Summary Card')).toBeInTheDocument();
    expect(mockSummaryCard).toHaveBeenCalled();

    const metrics = (mockSummaryCard.mock.calls.at(-1)?.[0] as { metrics?: Record<string, unknown> }).metrics;
    expect(metrics).toBeDefined();
    expect(metrics?.forecastEps).toBe(130);
    expect(metrics?.adjustedForecastEps).toBe(150);
    expect(metrics?.forecastEpsChangeRate).toBe(50);
    expect(metrics?.per).toBe(18);
    expect(metrics?.pbr).toBe(1.4);
    expect(metrics?.stockPrice).toBe(2500);
    expect(metrics?.cfoToNetProfitRatio).toBe(0.45);
    expect(metrics?.tradingValueToMarketCapRatio).toBeCloseTo(33.333333, 5);
    expect(mockSummaryCard.mock.calls.at(-1)?.[0]).toMatchObject({ tradingValuePeriod: 20 });
  });

  it('falls back when latestMetrics is missing and adjusted EPS cannot compute change rate', () => {
    mockUseFundamentals.mockReturnValue({
      data: {
        data: [
          {
            date: '2024-03-31',
            periodType: 'FY',
            adjustedEps: null,
            eps: 0,
            adjustedForecastEps: null,
            forecastEps: 120,
            forecastEpsChangeRate: 10,
            cashFlowOperating: 100,
            cashFlowInvesting: -50,
            cashFlowFinancing: -20,
            cashAndEquivalents: 500,
            netProfit: 200,
            equity: 1000,
          },
        ],
        dailyValuation: [undefined],
      },
      isLoading: false,
      error: null,
    });

    render(<FundamentalsPanel symbol="7203" />);

    const metrics = (mockSummaryCard.mock.calls.at(-1)?.[0] as { metrics?: Record<string, unknown> }).metrics;
    expect(metrics).toBeDefined();
    expect(metrics?.forecastEpsChangeRate).toBeNull();
    expect(metrics?.forecastEps).toBeNull();
    expect(metrics?.adjustedForecastEps).toBeNull();
    expect(metrics?.per).toBeUndefined();
    expect(metrics?.pbr).toBeUndefined();
    expect(metrics?.stockPrice).toBeUndefined();
  });
});
