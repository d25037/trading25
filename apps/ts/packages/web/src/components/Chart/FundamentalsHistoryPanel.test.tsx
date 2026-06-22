import { fireEvent, render, screen, within } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import {
  DEFAULT_FUNDAMENTALS_HISTORY_METRIC_ORDER,
  DEFAULT_FUNDAMENTALS_HISTORY_METRIC_VISIBILITY,
  type FundamentalsHistoryMetricId,
} from '@/constants/fundamentalsHistoryMetrics';
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

  it('defaults to FY-only mode and shows empty state when no FY data exists', () => {
    mockUseFundamentals.mockReturnValue({
      data: { data: [] },
      isLoading: false,
      error: null,
    });

    render(<FundamentalsHistoryPanel symbol="7203" />);
    expect(screen.getByRole('button', { name: 'FYのみ5期' })).toHaveAttribute('aria-pressed', 'true');
    expect(screen.getByText('過去のFYデータがありません')).toBeInTheDocument();
  });

  it('renders empty state in FY-only mode when data has only quarterly statements', () => {
    mockUseFundamentals.mockReturnValue({
      data: {
        data: [
          {
            date: '2024-06-30',
            disclosedDate: '2024-08-08',
            periodType: '1Q',
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

  it('renders fundamentals table headers and period labels', () => {
    mockUseFundamentals.mockReturnValue({
      data: {
        data: [
          {
            date: '2024-03-31',
            disclosedDate: '2024-05-10',
            periodType: 'FY',
            eps: 250.5,
            bps: 3200,
            roe: 12.5,
            netSales: 12_000,
            forecastSales: 13_000,
            operatingProfit: 1_500,
            forecastOperatingProfit: 1_800,
            operatingMargin: 12.5,
            cashFlowOperating: 500,
            cashFlowInvesting: -200,
            cashFlowFinancing: -100,
            fcf: 300,
            forecastEps: 280,
            forecastDividendFy: 120,
            payoutRatio: 35,
            forecastPayoutRatio: 38,
            netProfit: 1000,
            equity: 8000,
          },
          {
            date: '2023-03-31',
            disclosedDate: '2023-05-12',
            periodType: 'FY',
            eps: 200,
            bps: 3000,
            roe: 11.0,
            netSales: 11_000,
            forecastSales: 12_000,
            operatingProfit: 1_300,
            forecastOperatingProfit: 1_500,
            operatingMargin: 11.8,
            cashFlowOperating: 400,
            cashFlowInvesting: -180,
            cashFlowFinancing: -80,
            fcf: 220,
            forecastEps: 250,
            forecastDividendFy: 110,
            payoutRatio: 33,
            forecastPayoutRatio: 36,
            netProfit: 900,
            equity: 7500,
          },
        ],
      },
      isLoading: false,
      error: null,
    });

    render(<FundamentalsHistoryPanel symbol="7203" />);

    expect(screen.getByText('期別')).toBeInTheDocument();
    expect(screen.queryByText('発表日')).not.toBeInTheDocument();
    expect(screen.getByText('EPS')).toBeInTheDocument();
    expect(screen.getByText('来期予想EPS')).toBeInTheDocument();
    expect(screen.getByText('売上高')).toBeInTheDocument();
    expect(screen.getByText('予想売上高')).toBeInTheDocument();
    expect(screen.getByText('営業利益')).toBeInTheDocument();
    expect(screen.getByText('予想営業利益')).toBeInTheDocument();
    expect(screen.getByText('営業利益率')).toBeInTheDocument();
    expect(screen.getByText('ROE')).toBeInTheDocument();
    expect(screen.getByText('1株配当')).toBeInTheDocument();
    expect(screen.queryByText('BPS')).not.toBeInTheDocument();
    expect(screen.queryByText('予想1株配当')).not.toBeInTheDocument();
    expect(screen.queryByText('配当性向')).not.toBeInTheDocument();
    expect(screen.queryByText('予想配当性向')).not.toBeInTheDocument();
    expect(screen.queryByText('営業CF')).not.toBeInTheDocument();
    expect(screen.queryByText('投資CF')).not.toBeInTheDocument();
    expect(screen.queryByText('財務CF')).not.toBeInTheDocument();
    expect(screen.getByText('2024/3期')).toBeInTheDocument();
    expect(screen.getByText('2023/3期')).toBeInTheDocument();
    expect(screen.getByText('2024-05-10')).toBeInTheDocument();
  });

  it('applies FY metric visibility settings to table columns', () => {
    mockUseFundamentals.mockReturnValue({
      data: {
        data: [
          {
            date: '2024-03-31',
            disclosedDate: '2024-05-10',
            periodType: 'FY',
            eps: 250,
            bps: 3200,
            roe: 12.5,
            cashFlowOperating: 500,
            cashFlowInvesting: -200,
            cashFlowFinancing: -100,
            dividendFy: 120,
            forecastDividendFy: 125,
            payoutRatio: 35,
            forecastPayoutRatio: 38,
            netProfit: 1000,
            equity: 8000,
          },
        ],
      },
      isLoading: false,
      error: null,
    });

    render(
      <FundamentalsHistoryPanel
        symbol="7203"
        metricVisibility={{
          ...DEFAULT_FUNDAMENTALS_HISTORY_METRIC_VISIBILITY,
          roe: false,
          payoutRatio: false,
          forecastPayoutRatio: true,
        }}
      />
    );

    expect(screen.queryByText('ROE')).not.toBeInTheDocument();
    expect(screen.queryByText('配当性向')).not.toBeInTheDocument();
    expect(screen.getByText('EPS')).toBeInTheDocument();
    expect(screen.getByText('予想配当性向')).toBeInTheDocument();
  });

  it('renders explicitly visible optional metrics at render time', () => {
    mockUseFundamentals.mockReturnValue({
      data: {
        data: [
          {
            date: '2024-03-31',
            disclosedDate: '2024-05-10',
            periodType: 'FY',
            eps: 250,
            bps: 3200,
            roe: 12.5,
            netSales: 12_000,
            operatingProfit: 1_500,
            operatingMargin: 12.5,
            cashFlowOperating: 500,
            cashFlowInvesting: -200,
            cashFlowFinancing: -100,
            dividendFy: 120,
            forecastDividendFy: 125,
            payoutRatio: 35,
            forecastPayoutRatio: 38,
            netProfit: 1000,
            equity: 8000,
          },
        ],
      },
      isLoading: false,
      error: null,
    });

    render(
      <FundamentalsHistoryPanel
        symbol="7203"
        metricVisibility={{
          eps: true,
          forecastEps: true,
          netSales: true,
          forecastSales: true,
          operatingProfit: true,
          forecastOperatingProfit: true,
          operatingMargin: true,
          roe: true,
          dividendPerShare: true,
          bps: true,
          cashFlowOperating: true,
          forecastDividendPerShare: true,
          payoutRatio: true,
          forecastPayoutRatio: true,
          cashFlowInvesting: true,
          cashFlowFinancing: true,
        }}
      />
    );

    expect(screen.getByText('売上高')).toBeInTheDocument();
    expect(screen.getByText('予想売上高')).toBeInTheDocument();
    expect(screen.getByText('営業利益')).toBeInTheDocument();
    expect(screen.getByText('営業利益率')).toBeInTheDocument();
    expect(screen.getByText('BPS')).toBeInTheDocument();
    expect(screen.getByText('営業CF')).toBeInTheDocument();
    expect(screen.getByText('予想1株配当')).toBeInTheDocument();
    expect(screen.getByText('配当性向')).toBeInTheDocument();
    expect(screen.getByText('予想配当性向')).toBeInTheDocument();
  });

  it('respects explicit operating metric order at render time', () => {
    mockUseFundamentals.mockReturnValue({
      data: {
        data: [
          {
            date: '2024-03-31',
            disclosedDate: '2024-05-10',
            periodType: 'FY',
            eps: 250,
            bps: 3200,
            roe: 12.5,
            netSales: 12_000,
            operatingProfit: 1_500,
            operatingMargin: 12.5,
            dividendFy: 120,
            netProfit: 1000,
            equity: 8000,
          },
        ],
      },
      isLoading: false,
      error: null,
    });

    const metricOrder = ['eps', 'operatingProfit', 'operatingMargin', 'roe'] as FundamentalsHistoryMetricId[];

    render(<FundamentalsHistoryPanel symbol="7203" metricOrder={metricOrder} />);

    const headers = screen
      .getAllByRole('columnheader')
      .map((header) => header.textContent?.trim())
      .filter((text): text is string => typeof text === 'string' && text.length > 0);
    expect(headers.slice(0, 5)).toEqual(['期別', 'EPS', '営業利益', '営業利益率', 'ROE']);
  });

  it('shows operating profit and margin YoY deltas in separate columns', () => {
    mockUseFundamentals.mockReturnValue({
      data: {
        data: [
          {
            date: '2024-03-31',
            disclosedDate: '2024-05-10',
            periodType: 'FY',
            eps: 250,
            bps: 3200,
            roe: 12.5,
            netSales: 12_000,
            operatingProfit: 1_500,
            operatingMargin: 12.5,
            dividendFy: 120,
            netProfit: 1000,
            equity: 8000,
          },
          {
            date: '2023-03-31',
            disclosedDate: '2023-05-10',
            periodType: 'FY',
            eps: 200,
            bps: 3000,
            roe: 11,
            netSales: 11_000,
            operatingProfit: 1_200,
            operatingMargin: 10.0,
            dividendFy: 100,
            netProfit: 900,
            equity: 7500,
          },
        ],
      },
      isLoading: false,
      error: null,
    });

    render(<FundamentalsHistoryPanel symbol="7203" />);

    const operatingProfitValue = screen.getByText('15億');
    expect(operatingProfitValue).toBeInTheDocument();
    expect(operatingProfitValue.closest('td')).toHaveClass('text-foreground');
    expect(operatingProfitValue.closest('td')).not.toHaveClass('text-green-500');
    expect(screen.getAllByText('12.5%').length).toBeGreaterThan(0);
    expect(screen.getByText('（＋25.0%）')).toHaveClass('text-green-500');
    expect(screen.getByText('（＋2.5pt）')).toHaveClass('text-green-500');
  });

  it('applies FY metric order settings to table columns', () => {
    mockUseFundamentals.mockReturnValue({
      data: {
        data: [
          {
            date: '2024-03-31',
            disclosedDate: '2024-05-10',
            periodType: 'FY',
            eps: 250,
            bps: 3200,
            roe: 12.5,
            cashFlowOperating: 500,
            cashFlowInvesting: -200,
            cashFlowFinancing: -100,
            dividendFy: 120,
            forecastDividendFy: 125,
            payoutRatio: 35,
            forecastPayoutRatio: 38,
            netProfit: 1000,
            equity: 8000,
          },
        ],
      },
      isLoading: false,
      error: null,
    });

    render(
      <FundamentalsHistoryPanel
        symbol="7203"
        metricOrder={['payoutRatio', ...DEFAULT_FUNDAMENTALS_HISTORY_METRIC_ORDER.filter((id) => id !== 'payoutRatio')]}
        metricVisibility={{ ...DEFAULT_FUNDAMENTALS_HISTORY_METRIC_VISIBILITY, payoutRatio: true }}
      />
    );

    const headers = screen
      .getAllByRole('columnheader')
      .map((header) => header.textContent?.trim())
      .filter((text): text is string => typeof text === 'string' && text.length > 0);
    expect(headers[0]).toBe('期別');
    expect(headers[1]).toBe('配当性向');
    expect(headers[2]).toBe('EPS');
  });

  it('sorts FY rows by date descending, then disclosedDate descending', () => {
    mockUseFundamentals.mockReturnValue({
      data: {
        data: [
          {
            date: '2024-03-31',
            disclosedDate: '2024-05-08',
            periodType: 'FY',
            eps: 250,
            bps: 3200,
            roe: 12,
            cashFlowOperating: 500,
            cashFlowInvesting: -200,
            cashFlowFinancing: -100,
            netProfit: 1000,
            equity: 8000,
          },
          {
            date: '2024-03-31',
            disclosedDate: '2024-05-10',
            periodType: 'FY',
            eps: 260,
            bps: 3220,
            roe: 12.2,
            cashFlowOperating: 510,
            cashFlowInvesting: -210,
            cashFlowFinancing: -95,
            netProfit: 1005,
            equity: 8010,
          },
          {
            date: '2023-03-31',
            disclosedDate: '2023-05-10',
            periodType: 'FY',
            eps: 200,
            bps: 3000,
            roe: 11.0,
            cashFlowOperating: 400,
            cashFlowInvesting: -180,
            cashFlowFinancing: -80,
            netProfit: 900,
            equity: 7500,
          },
        ],
      },
      isLoading: false,
      error: null,
    });

    render(<FundamentalsHistoryPanel symbol="7203" />);

    const rows = screen.getAllByRole('row');
    expect(rows[1]?.textContent).toContain('2024-05-10');
    expect(rows[2]?.textContent).toContain('2024-05-08');
    expect(rows[3]?.textContent).toContain('2023-05-10');
  });

  it('limits FY-only mode to 5 most recent periods', () => {
    const fyData = Array.from({ length: 7 }, (_, i) => ({
      date: `${2024 - i}-03-31`,
      disclosedDate: `${2024 - i}-05-10`,
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

    const rows = screen.getAllByRole('row');
    expect(rows).toHaveLength(6);
    expect(screen.queryByText('2018/3期')).not.toBeInTheDocument();
    expect(screen.queryByText('2019/3期')).not.toBeInTheDocument();
  });

  it('switches to FY+xQ mode and shows latest 10 records with quarter labels', () => {
    mockUseFundamentals.mockReturnValue({
      data: {
        data: [
          {
            date: '2024-12-31',
            disclosedDate: '2025-02-10',
            periodType: '3Q',
            eps: 140,
            bps: 3200,
            roe: 10,
            cashFlowOperating: 320,
            cashFlowInvesting: -140,
            cashFlowFinancing: -90,
            netProfit: 600,
            equity: 6000,
          },
          {
            date: '2024-09-30',
            disclosedDate: '2024-11-10',
            periodType: '2Q',
            eps: 135,
            bps: 3180,
            roe: 9.8,
            cashFlowOperating: 300,
            cashFlowInvesting: -130,
            cashFlowFinancing: -85,
            netProfit: 580,
            equity: 5900,
          },
          {
            date: '2024-06-30',
            disclosedDate: '2024-08-10',
            periodType: '1Q',
            eps: 130,
            bps: 3160,
            roe: 9.6,
            cashFlowOperating: 290,
            cashFlowInvesting: -120,
            cashFlowFinancing: -82,
            netProfit: 570,
            equity: 5800,
          },
          {
            date: '2024-03-31',
            disclosedDate: '2024-05-10',
            periodType: 'FY',
            eps: 120,
            bps: 3140,
            roe: 9.4,
            cashFlowOperating: 280,
            cashFlowInvesting: -110,
            cashFlowFinancing: -80,
            netProfit: 560,
            equity: 5700,
          },
          {
            date: '2023-12-31',
            disclosedDate: '2024-02-10',
            periodType: '3Q',
            eps: 115,
            bps: 3100,
            roe: 9.1,
            cashFlowOperating: 260,
            cashFlowInvesting: -100,
            cashFlowFinancing: -78,
            netProfit: 540,
            equity: 5600,
          },
          {
            date: '2023-09-30',
            disclosedDate: '2023-11-10',
            periodType: '2Q',
            eps: 110,
            bps: 3080,
            roe: 8.9,
            cashFlowOperating: 250,
            cashFlowInvesting: -95,
            cashFlowFinancing: -75,
            netProfit: 530,
            equity: 5550,
          },
          {
            date: '2023-06-30',
            disclosedDate: '2023-08-10',
            periodType: '1Q',
            eps: 108,
            bps: 3060,
            roe: 8.7,
            cashFlowOperating: 240,
            cashFlowInvesting: -90,
            cashFlowFinancing: -72,
            netProfit: 520,
            equity: 5500,
          },
          {
            date: '2023-03-31',
            disclosedDate: '2023-05-10',
            periodType: 'FY',
            eps: 105,
            bps: 3040,
            roe: 8.5,
            cashFlowOperating: 230,
            cashFlowInvesting: -85,
            cashFlowFinancing: -70,
            netProfit: 510,
            equity: 5450,
          },
          {
            date: '2022-12-31',
            disclosedDate: '2023-02-10',
            periodType: '3Q',
            eps: 100,
            bps: 3020,
            roe: 8.3,
            cashFlowOperating: 220,
            cashFlowInvesting: -80,
            cashFlowFinancing: -68,
            netProfit: 500,
            equity: 5400,
          },
          {
            date: '2022-09-30',
            disclosedDate: '2022-11-10',
            periodType: '2Q',
            eps: 95,
            bps: 3000,
            roe: 8.1,
            cashFlowOperating: 210,
            cashFlowInvesting: -75,
            cashFlowFinancing: -65,
            netProfit: 490,
            equity: 5300,
          },
          {
            date: '2022-06-30',
            disclosedDate: '2022-08-10',
            periodType: '1Q',
            eps: 90,
            bps: 2980,
            roe: 7.8,
            cashFlowOperating: 200,
            cashFlowInvesting: -70,
            cashFlowFinancing: -60,
            netProfit: 470,
            equity: 5200,
          },
          {
            date: '2022-03-31',
            disclosedDate: '2022-05-10',
            periodType: 'FY',
            eps: 85,
            bps: 2960,
            roe: 7.5,
            cashFlowOperating: 190,
            cashFlowInvesting: -65,
            cashFlowFinancing: -55,
            netProfit: 460,
            equity: 5100,
          },
        ],
      },
      isLoading: false,
      error: null,
    });

    render(<FundamentalsHistoryPanel symbol="7203" />);

    expect(screen.getAllByRole('row')).toHaveLength(4);

    fireEvent.click(screen.getByRole('button', { name: 'FY+xQ 10回分' }));

    expect(screen.getByRole('button', { name: 'FY+xQ 10回分' })).toHaveAttribute('aria-pressed', 'true');
    expect(screen.getAllByRole('row')).toHaveLength(11);
    expect(screen.getByText('2024/12期 (3Q)')).toBeInTheDocument();
    expect(screen.getByText('2024/9期 (2Q)')).toBeInTheDocument();
    expect(screen.queryByText('2022/6期 (1Q)')).not.toBeInTheDocument();
    expect(screen.queryByText('2022/3期')).not.toBeInTheDocument();
  });

  it('shows quarter current-year forecast operating profit in FY+xQ mode', () => {
    mockUseFundamentals.mockReturnValue({
      data: {
        data: [
          {
            date: '2024-06-30',
            disclosedDate: '2024-08-10',
            periodType: '1Q',
            eps: 130,
            bps: 3160,
            roe: 9.6,
            operatingProfit: 600,
            forecastSales: 2_400,
            forecastOperatingProfit: 2_300,
            netProfit: 570,
            equity: 5800,
          },
          {
            date: '2024-03-31',
            disclosedDate: '2024-05-10',
            periodType: 'FY',
            eps: 120,
            bps: 3140,
            roe: 9.4,
            operatingProfit: 560,
            forecastSales: 1_600,
            revisedForecastSales: 2_400,
            forecastOperatingProfit: 1_800,
            revisedForecastOperatingProfit: 2_300,
            revisedForecastSource: '1Q',
            netProfit: 560,
            equity: 5700,
          },
        ],
      },
      isLoading: false,
      error: null,
    });

    render(<FundamentalsHistoryPanel symbol="7203" />);

    fireEvent.click(screen.getByRole('button', { name: 'FY+xQ 10回分' }));

    const quarterRow = screen.getByText('2024/6期 (1Q)').closest('tr');
    expect(quarterRow).not.toBeNull();
    expect(within(quarterRow as HTMLTableRowElement).getByText('24億')).toBeInTheDocument();
    expect(within(quarterRow as HTMLTableRowElement).getByText('23億')).toBeInTheDocument();

    const fyRow = screen.getByText('2024/3期').closest('tr');
    expect(fyRow).not.toBeNull();
    expect(within(fyRow as HTMLTableRowElement).getByText('16億')).toBeInTheDocument();
    expect(within(fyRow as HTMLTableRowElement).getByText('(1Q: 24億)')).toBeInTheDocument();
    expect(within(fyRow as HTMLTableRowElement).getByText('18億')).toBeInTheDocument();
    expect(within(fyRow as HTMLTableRowElement).getByText('(1Q: 23億)')).toBeInTheDocument();
  });

  it('shows EPS year-over-year delta in FY+xQ mode against the same fiscal period', () => {
    mockUseFundamentals.mockReturnValue({
      data: {
        data: [
          {
            date: '2025-06-30',
            disclosedDate: '2025-08-10',
            periodType: '1Q',
            eps: 84,
            bps: 3200,
            roe: 10,
            cashFlowOperating: 320,
            cashFlowInvesting: -140,
            cashFlowFinancing: -90,
            netProfit: 600,
            equity: 6000,
          },
          {
            date: '2024-06-28',
            disclosedDate: '2024-08-10',
            periodType: '1Q',
            eps: 64,
            bps: 3160,
            roe: 9.6,
            cashFlowOperating: 290,
            cashFlowInvesting: -120,
            cashFlowFinancing: -82,
            netProfit: 570,
            equity: 5800,
          },
          {
            date: '2025-03-31',
            disclosedDate: '2025-05-10',
            periodType: 'FY',
            eps: 140,
            bps: 3140,
            roe: 9.4,
            cashFlowOperating: 280,
            cashFlowInvesting: -110,
            cashFlowFinancing: -80,
            netProfit: 560,
            equity: 5700,
          },
          {
            date: '2024-03-31',
            disclosedDate: '2024-05-10',
            periodType: 'FY',
            eps: 150,
            bps: 3040,
            roe: 8.5,
            cashFlowOperating: 230,
            cashFlowInvesting: -85,
            cashFlowFinancing: -70,
            netProfit: 510,
            equity: 5450,
          },
        ],
      },
      isLoading: false,
      error: null,
    });

    render(<FundamentalsHistoryPanel symbol="7203" metricOrder={['eps']} />);

    fireEvent.click(screen.getByRole('button', { name: 'FY+xQ 10回分' }));

    const rows = screen.getAllByRole('row');
    expect(rows[1]?.textContent).toContain('84（＋20）');
    expect(rows[2]?.textContent).toContain('140（-10）');
  });

  it('can show quarterly-only data after switching to FY+xQ mode', () => {
    mockUseFundamentals.mockReturnValue({
      data: {
        data: [
          {
            date: '2024-06-30',
            disclosedDate: '2024-08-10',
            periodType: '1Q',
            eps: 100,
            bps: 500,
            roe: 10,
            cashFlowOperating: 50,
            cashFlowInvesting: -20,
            cashFlowFinancing: -10,
            netProfit: 100,
            equity: 1000,
          },
        ],
      },
      isLoading: false,
      error: null,
    });

    render(<FundamentalsHistoryPanel symbol="7203" />);

    expect(screen.getByText('過去のFYデータがありません')).toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: 'FY+xQ 10回分' }));

    expect(screen.queryByText('過去のFYデータがありません')).not.toBeInTheDocument();
    expect(screen.getByText('2024/6期 (1Q)')).toBeInTheDocument();
  });

  it('displays dash for null forecast EPS', () => {
    mockUseFundamentals.mockReturnValue({
      data: {
        data: [
          {
            date: '2024-03-31',
            disclosedDate: '2024-05-10',
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

    const rows = screen.getAllByRole('row');
    const dataRow = rows[1];
    const cells = dataRow?.querySelectorAll('td');
    expect(cells?.[3]?.textContent).toBe('-');
  });
});
