import { render, screen } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { FactorRegressionPanel } from './FactorRegressionPanel';

const mockUseFactorRegression = vi.fn();

vi.mock('@/hooks/useFactorRegression', () => ({
  useFactorRegression: (...args: unknown[]) => mockUseFactorRegression(...args),
}));

function makeRegressionData(marketBeta: number, marketRSquared: number) {
  return {
    stockCode: '7203',
    marketBeta,
    marketRSquared,
    sector17Matches: [
      {
        indexCode: '1001',
        indexName: 'TOPIX-17 Auto',
        category: 'sector17',
        rSquared: 0.35,
        beta: 1.25,
      },
      {
        indexCode: '1002',
        indexName: 'TOPIX-17 Retail',
        category: 'sector17',
        rSquared: 0.15,
        beta: 0.95,
      },
      {
        indexCode: '1003',
        indexName: 'TOPIX-17 Other',
        category: 'sector17',
        rSquared: 0.05,
        beta: 0.6,
      },
    ],
    sector33Matches: [
      {
        indexCode: '0059',
        indexName: '東証業種別 輸送用機器',
        category: 'sector33',
        rSquared: 0.42,
        beta: 1.1,
      },
      {
        indexCode: '0058',
        indexName: '東証業種別 機械',
        category: 'sector33',
        rSquared: 0.21,
        beta: 0.85,
      },
    ],
    topixStyleMatches: [
      {
        indexCode: '8100',
        indexName: 'TOPIX Large Growth',
        category: 'topix_style',
        rSquared: 0.28,
        beta: 1.05,
      },
    ],
    analysisDate: '2025-01-01',
    dataPoints: 240,
    dateRange: {
      from: '2024-01-01',
      to: '2025-01-01',
    },
  };
}

describe('FactorRegressionPanel', () => {
  beforeEach(() => {
    mockUseFactorRegression.mockReset();
  });

  it('renders placeholder when symbol is null', () => {
    mockUseFactorRegression.mockReturnValue({ data: null, isLoading: false, error: null });

    render(<FactorRegressionPanel symbol={null} />);
    expect(screen.getByText('銘柄を選択してください')).toBeInTheDocument();
  });

  it('forwards enabled option to useFactorRegression', () => {
    mockUseFactorRegression.mockReturnValue({ data: null, isLoading: false, error: null });

    render(<FactorRegressionPanel symbol="7203" enabled={false} />);
    expect(mockUseFactorRegression).toHaveBeenCalledWith('7203', { enabled: false });
  });

  it('renders loading and normalized error states', () => {
    mockUseFactorRegression.mockReturnValue({ data: null, isLoading: true, error: null });
    const { rerender } = render(<FactorRegressionPanel symbol="7203" />);
    expect(screen.getByText('Analyzing factor regression...')).toBeInTheDocument();

    mockUseFactorRegression.mockReturnValue({ data: null, isLoading: false, error: 'invalid payload' });
    rerender(<FactorRegressionPanel symbol="7203" />);
    expect(screen.getByText('Failed to load factor regression data')).toBeInTheDocument();
  });

  it('renders empty state when no regression data exists', () => {
    mockUseFactorRegression.mockReturnValue({ data: null, isLoading: false, error: null });

    render(<FactorRegressionPanel symbol="7203" />);
    expect(screen.getByText('No factor regression data available')).toBeInTheDocument();
  });

  it('renders regression data with high/low match states', () => {
    mockUseFactorRegression.mockReturnValue({
      data: makeRegressionData(1.3, 0.32),
      isLoading: false,
      error: null,
    });

    render(<FactorRegressionPanel symbol="7203" />);

    expect(screen.getByText('High sensitivity')).toBeInTheDocument();
    expect(screen.queryByText('TOPIX-17 Sectors')).not.toBeInTheDocument();
    expect(screen.queryByText('TOPIX-17 Auto')).not.toBeInTheDocument();
    expect(screen.getByText('33 Sectors')).toBeInTheDocument();
    expect(screen.getByText('Size + Style')).toBeInTheDocument();
    expect(screen.getByText(/東証業種別 輸送用機器/)).toBeInTheDocument();
    expect(screen.getByText(/TOPIX Large Growth/)).toBeInTheDocument();
    expect(screen.getByText('R²=42.0%')).toBeInTheDocument();
    expect(screen.getByText('R²=28.0%')).toBeInTheDocument();
    expect(screen.queryByText('No significant matches')).not.toBeInTheDocument();
    expect(screen.getByText(/Data Points: 240/)).toBeInTheDocument();
  });

  it('renders moderate and low beta interpretations', () => {
    mockUseFactorRegression.mockReturnValue({
      data: makeRegressionData(1.0, 0.15),
      isLoading: false,
      error: null,
    });
    const { rerender } = render(<FactorRegressionPanel symbol="7203" />);
    expect(screen.getByText('Moderate sensitivity')).toBeInTheDocument();

    mockUseFactorRegression.mockReturnValue({
      data: makeRegressionData(0.6, 0.05),
      isLoading: false,
      error: null,
    });
    rerender(<FactorRegressionPanel symbol="7203" />);
    expect(screen.getByText('Low sensitivity')).toBeInTheDocument();
  });
});
