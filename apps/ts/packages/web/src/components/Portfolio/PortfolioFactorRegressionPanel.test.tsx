/* @vitest-environment jsdom */

import { render, screen } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { PortfolioFactorRegressionPanel } from './PortfolioFactorRegressionPanel';

const mockUsePortfolioFactorRegression = vi.fn();

vi.mock('@/hooks/usePortfolioFactorRegression', () => ({
  usePortfolioFactorRegression: (...args: unknown[]) => mockUsePortfolioFactorRegression(...args),
}));

function makePortfolioRegressionData() {
  return {
    portfolioId: 1,
    portfolioName: 'My Portfolio',
    weights: [
      {
        code: '7203',
        companyName: 'Toyota',
        weight: 0.45,
        latestPrice: 3000,
        marketValue: 1350000,
        quantity: 450,
      },
    ],
    totalValue: 1350000,
    stockCount: 3,
    includedStockCount: 2,
    marketBeta: 1.12,
    marketRSquared: 0.31,
    // Portfolio endpoint now returns `code`/`name` and may omit `beta`.
    sector17Matches: [{ code: '8100', name: 'TOPIX-17 Auto', rSquared: 0.42 }],
    sector33Matches: [],
    topixStyleMatches: [{ code: '0070', name: 'TOPIX Growth', rSquared: 0.12, beta: 0.84 }],
    analysisDate: '2026-02-10',
    dataPoints: 252,
    dateRange: {
      from: '2025-01-01',
      to: '2026-01-01',
    },
    excludedStocks: [],
  };
}

describe('PortfolioFactorRegressionPanel', () => {
  beforeEach(() => {
    mockUsePortfolioFactorRegression.mockReset();
  });

  it('renders placeholder when portfolioId is null', () => {
    mockUsePortfolioFactorRegression.mockReturnValue({ data: null, isLoading: false, error: null });

    render(<PortfolioFactorRegressionPanel portfolioId={null} />);
    expect(screen.getByText('ポートフォリオを選択してください')).toBeInTheDocument();
  });

  it('renders loading and normalized error states', () => {
    mockUsePortfolioFactorRegression.mockReturnValue({ data: null, isLoading: true, error: null });
    const { rerender } = render(<PortfolioFactorRegressionPanel portfolioId={1} />);
    expect(screen.getByText('Analyzing portfolio factors...')).toBeInTheDocument();

    mockUsePortfolioFactorRegression.mockReturnValue({ data: null, isLoading: false, error: 'invalid payload' });
    rerender(<PortfolioFactorRegressionPanel portfolioId={1} />);
    expect(screen.getByText('Failed to load portfolio factor regression data')).toBeInTheDocument();
  });

  it('renders matches when API returns code/name format and missing beta', () => {
    mockUsePortfolioFactorRegression.mockReturnValue({
      data: makePortfolioRegressionData(),
      isLoading: false,
      error: null,
    });

    render(<PortfolioFactorRegressionPanel portfolioId={1} />);

    expect(screen.getAllByText('1. TOPIX-17 Auto').length).toBeGreaterThan(0);
    expect(screen.getByText('R²=42.0%')).toBeInTheDocument();
    expect(screen.getByText('β=N/A')).toBeInTheDocument();
    expect(screen.getByText('β=0.84')).toBeInTheDocument();
  });
});
