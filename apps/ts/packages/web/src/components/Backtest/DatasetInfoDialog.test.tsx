import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import type { ReactNode } from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { DatasetInfoDialog } from './DatasetInfoDialog';

const mockUseDatasetInfo = vi.fn();

const mockState = {
  data: null as Record<string, unknown> | null,
  isLoading: false,
  isError: false,
  error: null as Error | null,
};

vi.mock('@/hooks/useDataset', () => ({
  useDatasetInfo: (name: string | null) => {
    mockUseDatasetInfo(name);
    return {
      data: mockState.data,
      isLoading: mockState.isLoading,
      isError: mockState.isError,
      error: mockState.error,
    };
  },
}));

vi.mock('@/components/ui/dialog', () => ({
  Dialog: ({ open, children }: { open: boolean; children: ReactNode }) => (open ? <div>{children}</div> : null),
  DialogContent: ({ children }: { children: ReactNode }) => <div>{children}</div>,
  DialogHeader: ({ children }: { children: ReactNode }) => <div>{children}</div>,
  DialogTitle: ({ children }: { children: ReactNode }) => <h2>{children}</h2>,
  DialogDescription: ({ children }: { children: ReactNode }) => <p>{children}</p>,
  DialogFooter: ({ children }: { children: ReactNode }) => <div>{children}</div>,
}));

vi.mock('@/components/ui/button', () => ({
  Button: ({ children, onClick, disabled }: { children: ReactNode; onClick?: () => void; disabled?: boolean }) => (
    <button type="button" onClick={onClick} disabled={disabled}>
      {children}
    </button>
  ),
}));

describe('DatasetInfoDialog', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockState.data = null;
    mockState.isLoading = false;
    mockState.isError = false;
    mockState.error = null;
  });

  it('renders loading state', () => {
    mockState.isLoading = true;

    render(<DatasetInfoDialog open={true} onOpenChange={vi.fn()} datasetName="quickTesting.db" />);

    expect(document.querySelector('.animate-spin')).toBeInTheDocument();
    expect(mockUseDatasetInfo).toHaveBeenCalledWith('quickTesting.db');
  });

  it('renders error state', () => {
    mockState.isError = true;
    mockState.error = new Error('failed to load');

    render(<DatasetInfoDialog open={true} onOpenChange={vi.fn()} datasetName="quickTesting.db" />);

    expect(screen.getByText('Error: failed to load')).toBeInTheDocument();
  });

  it('renders detailed dataset info and closes dialog', async () => {
    const user = userEvent.setup();
    const onOpenChange = vi.fn();
    mockState.data = {
      snapshot: {
        preset: 'quickTesting',
        createdAt: '2026-01-01T00:00:00Z',
      },
      fileSize: 1024 * 1024,
      lastModified: '2026-01-02T00:00:00Z',
      stats: {
        totalStocks: 100,
        totalQuotes: 2000,
        dateRange: { from: '2025-01-01', to: '2025-12-31' },
        hasMarginData: true,
        hasTOPIXData: true,
        hasSectorData: false,
        hasStatementsData: true,
        statementsFieldCoverage: {
          hasExtendedFields: true,
          hasCashFlowFields: false,
        },
      },
      validation: {
        isValid: true,
        errors: [],
        warnings: [],
        details: {
          dataCoverage: {
            stocksWithQuotes: 90,
            stocksWithStatements: 80,
            stocksWithMargin: 70,
            totalStocks: 100,
          },
        },
      },
    };

    render(<DatasetInfoDialog open={true} onOpenChange={onOpenChange} datasetName="quickTesting.db" />);

    expect(screen.getByText('quickTesting')).toBeInTheDocument();
    expect(screen.getByText('Quotes')).toBeInTheDocument();
    expect(screen.getByText('90 / 100')).toBeInTheDocument();
    expect(screen.getAllByText('Statements').length).toBeGreaterThanOrEqual(1);
    expect(screen.getByText('80 / 100')).toBeInTheDocument();
    expect(screen.getAllByText('Margin').length).toBeGreaterThanOrEqual(1);
    expect(screen.getByText('70 / 100')).toBeInTheDocument();
    expect(screen.getByText('CF: N/A (v1)')).toBeInTheDocument();
    expect(screen.getByText('問題なし')).toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: '閉じる' }));
    expect(onOpenChange).toHaveBeenCalledWith(false);
  });

  it('renders validation errors and warnings', () => {
    mockState.data = {
      snapshot: {
        preset: null,
        createdAt: null,
      },
      fileSize: 1234,
      lastModified: '2026-01-02T00:00:00Z',
      stats: {
        totalStocks: 10,
        totalQuotes: 20,
        dateRange: { from: '2025-01-01', to: '2025-01-02' },
        hasMarginData: false,
        hasTOPIXData: false,
        hasSectorData: false,
        hasStatementsData: false,
        statementsFieldCoverage: null,
      },
      validation: {
        isValid: false,
        errors: ['missing quotes'],
        warnings: ['partial update'],
        details: {
          dateGapsCount: 3,
          orphanStocksCount: 2,
          fkIntegrity: {
            stockDataOrphans: 1,
            marginDataOrphans: 0,
            statementsOrphans: 4,
          },
          stockCountValidation: {
            preset: 'quickTesting',
            expected: { min: 20, max: 20 },
            actual: 10,
            isWithinRange: false,
          },
        },
      },
    };

    render(<DatasetInfoDialog open={true} onOpenChange={vi.fn()} datasetName="quickTesting.db" />);

    expect(screen.getByText('missing quotes')).toBeInTheDocument();
    expect(screen.getByText('partial update')).toBeInTheDocument();
    expect(screen.getByText('Date gaps')).toBeInTheDocument();
    expect(screen.getByText('Stocks without quotes')).toBeInTheDocument();
    expect(screen.getByText('FK integrity')).toBeInTheDocument();
  });
});
