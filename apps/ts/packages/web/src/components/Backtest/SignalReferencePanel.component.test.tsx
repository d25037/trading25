import { act, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { SignalReferenceResponse } from '@/types/backtest';
import { SignalReferencePanel } from './SignalReferencePanel';

const mockUseSignalReference = vi.fn();
const mockLoggerWarn = vi.fn();

vi.mock('@/hooks/useBacktest', () => ({
  useSignalReference: () => mockUseSignalReference(),
}));

vi.mock('@/utils/logger', () => ({
  logger: {
    warn: (...args: unknown[]) => mockLoggerWarn(...args),
    debug: vi.fn(),
    error: vi.fn(),
    info: vi.fn(),
  },
}));

const baseSignalData = {
  categories: [
    { key: 'breakout', label: 'Breakout' },
    { key: 'trend', label: 'Trend' },
  ],
  signals: [
    {
      key: 'breakout.close_gt_sma',
      category: 'breakout',
      name: 'Close > SMA',
      description: 'Close price is above moving average',
      usage_hint: 'Use this for trend confirmation',
      yaml_snippet: 'entry_filter:\n  breakout.close_gt_sma:\n    enabled: true',
      fields: [
        {
          name: 'period',
          description: 'SMA period',
          options: ['5', '10'],
          constraints: { gt: 0, le: 50 },
        },
      ],
    },
    {
      key: 'trend.unknown',
      category: 'not-defined',
      name: 'Unknown category signal',
      description: 'should trigger warn log',
      usage_hint: 'N/A',
      yaml_snippet: 'exit_trigger: {}',
      fields: [],
    },
  ],
} as SignalReferenceResponse;

describe('SignalReferencePanel', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.useRealTimers();
    mockUseSignalReference.mockReturnValue({
      data: baseSignalData,
      isLoading: false,
      error: null,
    });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('renders loading state', () => {
    mockUseSignalReference.mockReturnValue({
      data: null,
      isLoading: true,
      error: null,
    });

    render(<SignalReferencePanel onCopySnippet={vi.fn()} />);

    expect(screen.getByText('Loading signals...')).toBeInTheDocument();
  });

  it('renders error states for Error and unknown errors', () => {
    mockUseSignalReference.mockReturnValue({
      data: null,
      isLoading: false,
      error: new Error('request failed'),
    });

    const { rerender } = render(<SignalReferencePanel onCopySnippet={vi.fn()} />);
    expect(screen.getByText('Failed to load signal reference')).toBeInTheDocument();
    expect(screen.getByText('request failed')).toBeInTheDocument();

    mockUseSignalReference.mockReturnValue({
      data: null,
      isLoading: false,
      error: { message: 'plain object error' },
    });
    rerender(<SignalReferencePanel onCopySnippet={vi.fn()} />);
    expect(screen.getByText('Unknown error occurred')).toBeInTheDocument();
  });

  it('renders categories, filters by search, expands detail, and copies snippet', () => {
    vi.useFakeTimers();
    const onCopySnippet = vi.fn();
    render(<SignalReferencePanel onCopySnippet={onCopySnippet} />);

    expect(mockLoggerWarn).toHaveBeenCalledWith(
      'Unknown signal category: not-defined (signal: trend.unknown)'
    );
    expect(screen.getByText('Signal Reference')).toBeInTheDocument();
    expect(screen.getByText('Breakout')).toBeInTheDocument();
    expect(screen.getByText('1')).toBeInTheDocument();

    const signalHeaderButton = screen.getByText('Close > SMA').closest('button');
    expect(signalHeaderButton).not.toBeNull();
    fireEvent.click(signalHeaderButton as HTMLButtonElement);
    expect(screen.getByText('Use this for trend confirmation')).toBeInTheDocument();
    expect(screen.getByText('[>0, <=50]')).toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: 'Copy' }));
    expect(onCopySnippet).toHaveBeenCalledWith('entry_filter:\n  breakout.close_gt_sma:\n    enabled: true');
    expect(screen.getByText('Copied!')).toBeInTheDocument();

    act(() => {
      vi.advanceTimersByTime(2000);
    });
    expect(screen.queryByText('Copied!')).not.toBeInTheDocument();

    fireEvent.change(screen.getByPlaceholderText('Search signals...'), { target: { value: 'non-existent' } });
    expect(screen.getByText('No signals found')).toBeInTheDocument();
  });
});
