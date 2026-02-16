import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { PortfolioWithItems } from '@/types/portfolio';
import { PortfolioDetail } from './PortfolioDetail';

const mockNavigate = vi.fn();
const mockSetSelectedSymbol = vi.fn();
const mockUsePortfolioPerformance = vi.fn();

vi.mock('@tanstack/react-router', () => ({
  useNavigate: () => mockNavigate,
}));

vi.mock('@/stores/chartStore', () => ({
  useChartStore: () => ({
    setSelectedSymbol: mockSetSelectedSymbol,
  }),
}));

vi.mock('@/hooks/usePortfolioPerformance', () => ({
  usePortfolioPerformance: (...args: unknown[]) => mockUsePortfolioPerformance(...args),
}));

vi.mock('./AddStockDialog', () => ({
  AddStockDialog: () => <div>AddStockDialog</div>,
}));

vi.mock('./EditPortfolioDialog', () => ({
  EditPortfolioDialog: () => <div>EditPortfolioDialog</div>,
}));

vi.mock('./DeletePortfolioDialog', () => ({
  DeletePortfolioDialog: () => <div>DeletePortfolioDialog</div>,
}));

vi.mock('./EditStockDialog', () => ({
  EditStockDialog: () => <div>EditStockDialog</div>,
}));

vi.mock('./DeleteStockDialog', () => ({
  DeleteStockDialog: () => <div>DeleteStockDialog</div>,
}));

vi.mock('./PerformanceSummary', () => ({
  PerformanceSummary: () => <div>PerformanceSummary</div>,
}));

vi.mock('./BenchmarkChart', () => ({
  BenchmarkChart: () => <div>BenchmarkChart</div>,
}));

vi.mock('./PortfolioFactorRegressionPanel', () => ({
  PortfolioFactorRegressionPanel: () => <div>PortfolioFactorRegressionPanel</div>,
}));

const samplePortfolio: PortfolioWithItems = {
  id: 1,
  name: 'Main Portfolio',
  description: 'Core holdings',
  createdAt: '2026-02-16T00:00:00Z',
  updatedAt: '2026-02-16T00:00:00Z',
  items: [
    {
      id: 10,
      portfolioId: 1,
      code: '7203',
      companyName: 'Toyota Motor',
      quantity: 100,
      purchasePrice: 2500,
      purchaseDate: '2026-01-10',
      account: undefined,
      notes: undefined,
      createdAt: '2026-01-10T00:00:00Z',
      updatedAt: '2026-01-10T00:00:00Z',
    },
  ],
};

const samplePortfolioWithTwoItems: PortfolioWithItems = {
  id: 2,
  name: 'Growth Portfolio',
  description: 'Aggressive growth',
  createdAt: '2026-02-16T00:00:00Z',
  updatedAt: '2026-02-16T00:00:00Z',
  items: [
    {
      id: 20,
      portfolioId: 2,
      code: '7203',
      companyName: 'Toyota Motor',
      quantity: 100,
      purchasePrice: 2500,
      purchaseDate: '2026-01-10',
      account: undefined,
      notes: undefined,
      createdAt: '2026-01-10T00:00:00Z',
      updatedAt: '2026-01-10T00:00:00Z',
    },
    {
      id: 21,
      portfolioId: 2,
      code: '6758',
      companyName: 'Sony Group',
      quantity: 50,
      purchasePrice: 12000,
      purchaseDate: '2026-01-11',
      account: undefined,
      notes: undefined,
      createdAt: '2026-01-11T00:00:00Z',
      updatedAt: '2026-01-11T00:00:00Z',
    },
  ],
};

describe('PortfolioDetail', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockUsePortfolioPerformance.mockReturnValue({
      data: null,
      isLoading: false,
    });
  });

  it('shows empty selection state when portfolio is not selected', () => {
    render(<PortfolioDetail portfolio={undefined} isLoading={false} error={null} />);

    expect(screen.getByText('Select a portfolio to view details')).toBeInTheDocument();
  });

  it('navigates to chart page when stock code is clicked', async () => {
    const user = userEvent.setup();

    render(<PortfolioDetail portfolio={samplePortfolio} isLoading={false} error={null} />);

    await user.click(screen.getByRole('button', { name: 'View chart for 7203 Toyota Motor' }));

    expect(mockSetSelectedSymbol).toHaveBeenCalledWith('7203');
    expect(mockNavigate).toHaveBeenCalledWith({ to: '/charts' });
  });

  it('renders performance sections when performance data is available', async () => {
    const user = userEvent.setup();

    mockUsePortfolioPerformance.mockReturnValue({
      data: {
        portfolioId: 2,
        portfolioName: 'Growth Portfolio',
        summary: {
          totalCost: 1000000,
          currentValue: 1200000,
          totalPnL: 200000,
          returnRate: 20,
        },
        holdings: [
          {
            code: '7203',
            companyName: 'Toyota Motor',
            quantity: 100,
            purchasePrice: 2500,
            currentPrice: 2600,
            cost: 250000,
            marketValue: 260000,
            pnl: 10000,
            returnRate: 4,
            weight: 0.2,
            purchaseDate: '2026-01-10',
            account: undefined,
          },
          {
            code: '6758',
            companyName: 'Sony Group',
            quantity: 50,
            purchasePrice: 12000,
            currentPrice: 13000,
            cost: 600000,
            marketValue: 650000,
            pnl: 50000,
            returnRate: 8.33,
            weight: 0.5,
            purchaseDate: '2026-01-11',
            account: undefined,
          },
        ],
        timeSeries: [],
        benchmark: {
          code: '0000',
          name: 'TOPIX',
          beta: 1,
          alpha: 0,
          correlation: 0.9,
          rSquared: 0.8,
          benchmarkReturn: 10,
          relativeReturn: 10,
        },
        benchmarkTimeSeries: [
          {
            date: '2026-02-16',
            portfolioReturn: 0.1,
            benchmarkReturn: 0.08,
          },
        ],
        analysisDate: '2026-02-16',
        dateRange: null,
        dataPoints: 1,
        warnings: [],
      },
      isLoading: false,
    });

    render(<PortfolioDetail portfolio={samplePortfolioWithTwoItems} isLoading={false} error={null} />);

    expect(screen.getByText('PerformanceSummary')).toBeInTheDocument();
    expect(screen.getByText('BenchmarkChart')).toBeInTheDocument();
    expect(screen.getByText('PortfolioFactorRegressionPanel')).toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'Code' }));
    await user.click(screen.getByRole('button', { name: 'Code' }));
    await user.click(screen.getByRole('button', { name: 'Qty' }));
  });

  it('renders empty holdings table when selected portfolio has no items', () => {
    const emptyPortfolio: PortfolioWithItems = {
      ...samplePortfolio,
      id: 3,
      name: 'Empty Portfolio',
      items: [],
    };

    render(<PortfolioDetail portfolio={emptyPortfolio} isLoading={false} error={null} />);

    expect(screen.getByText('No stocks in this portfolio')).toBeInTheDocument();
  });
});
