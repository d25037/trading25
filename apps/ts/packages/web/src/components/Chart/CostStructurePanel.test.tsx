import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { CostStructurePanel } from './CostStructurePanel';

const mockUseCostStructureAnalysis = vi.fn();

vi.mock('@/hooks/useCostStructureAnalysis', () => ({
  useCostStructureAnalysis: (...args: unknown[]) => mockUseCostStructureAnalysis(...args),
}));

function makeCostStructureData() {
  return {
    symbol: '7203',
    companyName: 'Toyota Motor',
    lastUpdated: '2026-04-02T00:00:00Z',
    dateRange: {
      from: '2024-03-31',
      to: '2025-12-31',
    },
    provenance: {
      source_kind: 'market' as const,
      warnings: ['Market snapshot warning'],
    },
    diagnostics: {
      warnings: ['Derived 4Q point'],
    },
    latestPoint: {
      periodEnd: '2025-12-31',
      disclosedDate: '2026-02-05',
      fiscalYear: 'FY2025',
      analysisPeriodType: '4Q' as const,
      sales: 78_900,
      operatingProfit: 3_400,
      operatingMargin: 4.3,
      isDerived: true,
    },
    points: [
      {
        periodEnd: '2024-03-31',
        disclosedDate: '2024-05-08',
        fiscalYear: 'FY2024',
        analysisPeriodType: '1Q' as const,
        sales: 54_000,
        operatingProfit: -2_300,
        operatingMargin: -4.3,
        isDerived: false,
      },
      {
        periodEnd: '2025-12-31',
        disclosedDate: '2026-02-05',
        fiscalYear: 'FY2025',
        analysisPeriodType: '4Q' as const,
        sales: 78_900,
        operatingProfit: 3_400,
        operatingMargin: 4.3,
        isDerived: true,
      },
    ],
    regression: {
      sampleCount: 6,
      slope: 0.43,
      intercept: -5_459,
      rSquared: 0.939,
      contributionMarginRatio: 0.43,
      variableCostRatio: 0.57,
      fixedCost: 5_459,
      breakEvenSales: 12_688,
    },
  };
}

function makeSinglePointCostStructureData() {
  const data = makeCostStructureData();
  return {
    ...data,
    points: [
      {
        ...data.latestPoint,
        sales: 50_000,
        operatingProfit: 5_000,
      },
    ],
    latestPoint: {
      ...data.latestPoint,
      sales: 50_000,
      operatingProfit: 5_000,
    },
    regression: {
      ...data.regression,
      sampleCount: 1,
      intercept: 5_000,
      slope: 0,
    },
  };
}

describe('CostStructurePanel', () => {
  beforeEach(() => {
    mockUseCostStructureAnalysis.mockReset();
  });

  it('renders placeholder when symbol is null', () => {
    mockUseCostStructureAnalysis.mockReturnValue({ data: null, isLoading: false, error: null });

    render(<CostStructurePanel symbol={null} />);
    expect(screen.getByText('銘柄を選択してください')).toBeInTheDocument();
  });

  it('forwards enabled option to hook', () => {
    mockUseCostStructureAnalysis.mockReturnValue({ data: null, isLoading: false, error: null });

    render(<CostStructurePanel symbol="7203" enabled={false} />);
    expect(mockUseCostStructureAnalysis).toHaveBeenCalledWith('7203', {
      enabled: false,
      view: 'recent',
      windowQuarters: 12,
    });
  });

  it('renders loading and normalized error states', () => {
    mockUseCostStructureAnalysis.mockReturnValue({ data: null, isLoading: true, error: null });
    const { rerender } = render(<CostStructurePanel symbol="7203" />);
    expect(screen.getByText('Analyzing cost structure...')).toBeInTheDocument();

    mockUseCostStructureAnalysis.mockReturnValue({ data: null, isLoading: false, error: 'invalid payload' });
    rerender(<CostStructurePanel symbol="7203" />);
    expect(screen.getByText('Failed to load cost structure data')).toBeInTheDocument();
  });

  it('renders empty state when no data exists', () => {
    mockUseCostStructureAnalysis.mockReturnValue({ data: null, isLoading: false, error: null });

    render(<CostStructurePanel symbol="7203" />);
    expect(screen.getByText('No cost structure data available')).toBeInTheDocument();
  });

  it('renders scatter summary cards and warnings', () => {
    mockUseCostStructureAnalysis.mockReturnValue({
      data: makeCostStructureData(),
      isLoading: false,
      error: null,
    });

    render(<CostStructurePanel symbol="7203" />);

    expect(screen.getByRole('img', { name: 'Cost structure scatter plot' })).toBeInTheDocument();
    expect(screen.getByText('Latest Sales')).toBeInTheDocument();
    expect(screen.getByText('Operating Margin')).toBeInTheDocument();
    expect(screen.getByText('Variable Cost Ratio')).toBeInTheDocument();
    expect(screen.getByText('R²')).toBeInTheDocument();
    expect(screen.getByText('Recommended Analysis')).toBeInTheDocument();
    expect(screen.getByText(/Analysis Window:/)).toBeInTheDocument();
    expect(screen.getByText(/Derived 4Q point/)).toBeInTheDocument();
    expect(screen.getByText(/Market snapshot warning/)).toBeInTheDocument();
    expect(screen.getByText('57.0%')).toBeInTheDocument();
    expect(screen.getByText('93.9%')).toBeInTheDocument();
  });

  it('switches analysis modes through the hook', async () => {
    mockUseCostStructureAnalysis.mockReturnValue({
      data: makeCostStructureData(),
      isLoading: false,
      error: null,
    });
    const user = userEvent.setup();

    render(<CostStructurePanel symbol="7203" />);

    expect(mockUseCostStructureAnalysis).toHaveBeenLastCalledWith('7203', {
      enabled: true,
      view: 'recent',
      windowQuarters: 12,
    });

    await user.click(screen.getByRole('button', { name: 'Same Q' }));

    expect(mockUseCostStructureAnalysis).toHaveBeenLastCalledWith('7203', {
      enabled: true,
      view: 'same_quarter',
      windowQuarters: 12,
    });

    await user.click(screen.getByRole('button', { name: '20Q' }));

    expect(mockUseCostStructureAnalysis).toHaveBeenLastCalledWith('7203', {
      enabled: true,
      view: 'recent',
      windowQuarters: 20,
    });

    await user.click(screen.getByRole('button', { name: 'FY only' }));

    expect(mockUseCostStructureAnalysis).toHaveBeenLastCalledWith('7203', {
      enabled: true,
      view: 'fiscal_year_only',
      windowQuarters: 12,
    });
  });

  it('renders a single-point scatter without crashing', () => {
    mockUseCostStructureAnalysis.mockReturnValue({
      data: makeSinglePointCostStructureData(),
      isLoading: false,
      error: null,
    });

    render(<CostStructurePanel symbol="7203" />);

    expect(screen.getByRole('img', { name: 'Cost structure scatter plot' })).toBeInTheDocument();
    expect(screen.getByText('1 samples')).toBeInTheDocument();
  });
});
