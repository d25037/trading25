import { render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import { FundamentalsHistoryPanel } from './FundamentalsHistoryPanel';

const mockUseFundamentals = vi.fn();

vi.mock('@/hooks/useFundamentals', () => ({
  useFundamentals: (...args: unknown[]) => mockUseFundamentals(...args),
}));

describe('FundamentalsHistoryPanel', () => {
  it('renders placeholder when no symbol is provided', () => {
    mockUseFundamentals.mockReturnValue({ data: null, isLoading: false, error: null });

    render(<FundamentalsHistoryPanel symbol={null} />);
    expect(screen.getByText('銘柄を選択してください')).toBeInTheDocument();
  });

  it('renders loading state', () => {
    mockUseFundamentals.mockReturnValue({ data: null, isLoading: true, error: null });

    render(<FundamentalsHistoryPanel symbol="7203" />);
    expect(screen.getByText('Loading fundamentals history...')).toBeInTheDocument();
  });

  it('renders error state', () => {
    mockUseFundamentals.mockReturnValue({
      data: null,
      isLoading: false,
      error: new Error('API Error'),
    });

    render(<FundamentalsHistoryPanel symbol="7203" />);
    expect(screen.getByText('API Error')).toBeInTheDocument();
  });

  it('renders error state for non-Error objects', () => {
    mockUseFundamentals.mockReturnValue({
      data: null,
      isLoading: false,
      error: 'string error',
    });

    render(<FundamentalsHistoryPanel symbol="7203" />);
    expect(screen.getByText('Failed to load fundamentals data')).toBeInTheDocument();
  });

  it('renders empty state when no FY data exists', () => {
    mockUseFundamentals.mockReturnValue({
      data: { data: [] },
      isLoading: false,
      error: null,
    });

    render(<FundamentalsHistoryPanel symbol="7203" />);
    expect(screen.getByText('過去のFYデータがありません')).toBeInTheDocument();
  });

  it('renders empty state when data has only quarterly statements', () => {
    mockUseFundamentals.mockReturnValue({
      data: {
        data: [
          {
            date: '2024-06-30',
            periodType: 'Q1',
            eps: 100,
            bps: 500,
            roe: 10,
            cashFlowOperating: 50,
            cashFlowInvesting: -20,
            cashFlowFinancing: -10,
            fcf: 30,
            forecastEps: 120,
          },
        ],
      },
      isLoading: false,
      error: null,
    });

    render(<FundamentalsHistoryPanel symbol="7203" />);
    expect(screen.getByText('過去のFYデータがありません')).toBeInTheDocument();
  });

  it('renders FY history table with data', () => {
    mockUseFundamentals.mockReturnValue({
      data: {
        data: [
          {
            date: '2024-03-31',
            periodType: 'FY',
            eps: 250.5,
            bps: 3200,
            roe: 12.5,
            cashFlowOperating: 500,
            cashFlowInvesting: -200,
            cashFlowFinancing: -100,
            fcf: 300,
            forecastEps: 280,
            netProfit: 1000,
            equity: 8000,
          },
          {
            date: '2023-03-31',
            periodType: 'FY',
            eps: 200,
            bps: 3000,
            roe: 11.0,
            cashFlowOperating: 400,
            cashFlowInvesting: -180,
            cashFlowFinancing: -80,
            fcf: 220,
            forecastEps: 250,
            netProfit: 900,
            equity: 7500,
          },
        ],
      },
      isLoading: false,
      error: null,
    });

    render(<FundamentalsHistoryPanel symbol="7203" />);

    // Table headers
    expect(screen.getByText('FY期')).toBeInTheDocument();
    expect(screen.getByText('EPS')).toBeInTheDocument();
    expect(screen.getByText('来期予想EPS')).toBeInTheDocument();
    expect(screen.getByText('BPS')).toBeInTheDocument();
    expect(screen.getByText('営業CF')).toBeInTheDocument();
    expect(screen.getByText('投資CF')).toBeInTheDocument();
    expect(screen.getByText('財務CF')).toBeInTheDocument();
    expect(screen.getByText('ROE')).toBeInTheDocument();

    // FY labels (2024/3期, 2023/3期)
    expect(screen.getByText('2024/3期')).toBeInTheDocument();
    expect(screen.getByText('2023/3期')).toBeInTheDocument();
  });

  it('sorts FY data in descending order by date', () => {
    mockUseFundamentals.mockReturnValue({
      data: {
        data: [
          {
            date: '2022-03-31',
            periodType: 'FY',
            eps: 150,
            bps: 2800,
            roe: 9,
            cashFlowOperating: 300,
            cashFlowInvesting: -100,
            cashFlowFinancing: -50,
            fcf: 200,
            forecastEps: null,
            netProfit: 800,
            equity: 7000,
          },
          {
            date: '2024-03-31',
            periodType: 'FY',
            eps: 250,
            bps: 3200,
            roe: 12,
            cashFlowOperating: 500,
            cashFlowInvesting: -200,
            cashFlowFinancing: -100,
            fcf: 300,
            forecastEps: 280,
            netProfit: 1000,
            equity: 8000,
          },
        ],
      },
      isLoading: false,
      error: null,
    });

    render(<FundamentalsHistoryPanel symbol="7203" />);

    const rows = screen.getAllByRole('row');
    // First row is header, second should be 2024 (newest), third should be 2022
    expect(rows[1]?.textContent).toContain('2024/3期');
    expect(rows[2]?.textContent).toContain('2022/3期');
  });

  it('limits display to 5 most recent FY periods', () => {
    const fyData = Array.from({ length: 7 }, (_, i) => ({
      date: `${2024 - i}-03-31`,
      periodType: 'FY',
      eps: 100 + i * 10,
      bps: 2000 + i * 100,
      roe: 10 + i,
      cashFlowOperating: 400 + i * 50,
      cashFlowInvesting: -(100 + i * 20),
      cashFlowFinancing: -(50 + i * 10),
      fcf: 300 + i * 30,
      forecastEps: null,
      netProfit: 500 + i * 50,
      equity: 5000 + i * 500,
    }));

    mockUseFundamentals.mockReturnValue({
      data: { data: fyData },
      isLoading: false,
      error: null,
    });

    render(<FundamentalsHistoryPanel symbol="7203" />);

    // header + 5 data rows = 6 total rows
    const rows = screen.getAllByRole('row');
    expect(rows).toHaveLength(6);

    // Oldest 2 FY periods should not be visible
    expect(screen.queryByText('2018/3期')).not.toBeInTheDocument();
    expect(screen.queryByText('2019/3期')).not.toBeInTheDocument();
  });

  it('displays dash for null forecast EPS', () => {
    mockUseFundamentals.mockReturnValue({
      data: {
        data: [
          {
            date: '2024-03-31',
            periodType: 'FY',
            eps: 250,
            bps: 3200,
            roe: 12,
            cashFlowOperating: 500,
            cashFlowInvesting: -200,
            cashFlowFinancing: -100,
            fcf: 300,
            forecastEps: null,
            netProfit: 1000,
            equity: 8000,
          },
        ],
      },
      isLoading: false,
      error: null,
    });

    render(<FundamentalsHistoryPanel symbol="7203" />);

    // The forecast EPS column should show '-' for null
    const rows = screen.getAllByRole('row');
    const dataRow = rows[1];
    const cells = dataRow?.querySelectorAll('td');
    // forecastEps is the 3rd column (index 2)
    expect(cells?.[2]?.textContent).toBe('-');
  });
});
