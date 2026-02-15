import { render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';
import type { BacktestJobResponse } from '@/types/backtest';
import { JobProgressCard } from './JobProgressCard';

const baseResult = {
  total_return: 0.12,
  sharpe_ratio: 1.24,
  calmar_ratio: 0.8,
  max_drawdown: -0.1,
  win_rate: 55,
  trade_count: 18,
  html_path: null,
};

const baseJob: BacktestJobResponse = {
  job_id: 'job-1',
  status: 'completed',
  progress: 100,
  message: null,
  created_at: '2026-02-15T09:00:00Z',
  started_at: '2026-02-15T09:00:01Z',
  completed_at: '2026-02-15T09:01:00Z',
  error: null,
  result: baseResult,
};

function createJob(overrides: Partial<BacktestJobResponse>): BacktestJobResponse {
  return {
    ...baseJob,
    ...overrides,
  };
}

function expectSortinoValue(value: string) {
  const sortinoRow = screen.getByText('Sortino:').parentElement;
  expect(sortinoRow).not.toBeNull();
  if (!sortinoRow) {
    return;
  }
  expect(within(sortinoRow).getByText(value)).toBeInTheDocument();
}

describe('JobProgressCard', () => {
  it('renders nothing when no job and not loading', () => {
    const { container } = render(<JobProgressCard job={null} />);
    expect(container.firstChild).toBeNull();
  });

  it('renders submitting state when loading without job', () => {
    render(<JobProgressCard job={null} isLoading={true} />);
    expect(screen.getByText('Submitting...')).toBeInTheDocument();
  });

  it('renders running state with progress message and cancel action', async () => {
    const user = userEvent.setup();
    const onCancel = vi.fn();
    const job = createJob({
      status: 'running',
      result: null,
      message: 'executing strategy',
      completed_at: null,
    });

    render(<JobProgressCard job={job} onCancel={onCancel} />);

    expect(screen.getByText('Running')).toBeInTheDocument();
    expect(screen.getByText('executing strategy')).toBeInTheDocument();
    expect(screen.getByText(/⏱/)).toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'Cancel' }));
    expect(onCancel).toHaveBeenCalledTimes(1);
  });

  it('renders failed state with error message', () => {
    const job = createJob({
      status: 'failed',
      result: null,
      error: 'execution failed',
      message: null,
    });

    render(<JobProgressCard job={job} />);
    expect(screen.getByText('Failed')).toBeInTheDocument();
    expect(screen.getByText('execution failed')).toBeInTheDocument();
  });

  it('renders cancelled state with fallback message', () => {
    const job = createJob({
      status: 'cancelled',
      result: null,
      message: null,
    });

    render(<JobProgressCard job={job} />);
    expect(screen.getByText('Cancelled')).toBeInTheDocument();
    expect(screen.getByText('Backtest was cancelled')).toBeInTheDocument();
  });

  it('renders cancelled state with server message', () => {
    const job = createJob({
      status: 'cancelled',
      result: null,
      message: 'cancelled by user',
    });

    render(<JobProgressCard job={job} />);
    expect(screen.getByText('cancelled by user')).toBeInTheDocument();
  });

  it('renders pending state without cancel button when callback is absent', () => {
    const job = createJob({
      status: 'pending',
      result: null,
      message: null,
    });

    render(<JobProgressCard job={job} />);
    expect(screen.getByText('Pending')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Cancel' })).not.toBeInTheDocument();
  });

  it('shows sortino ratio when available on result payload', () => {
    const job = {
      ...baseJob,
      result: {
        ...baseResult,
        sortino_ratio: 1.86,
      },
    } as BacktestJobResponse;

    render(<JobProgressCard job={job} />);
    expectSortinoValue('1.86');
  });

  it('shows fallback when sortino ratio is absent', () => {
    render(<JobProgressCard job={baseJob} />);
    expectSortinoValue('-');
  });

  it('shows fallback when sortino ratio is NaN', () => {
    const job = {
      ...baseJob,
      result: {
        ...baseResult,
        sortino_ratio: Number.NaN,
      },
    } as BacktestJobResponse;

    render(<JobProgressCard job={job} />);
    expectSortinoValue('-');
  });

  it('shows fallback when sortino ratio is null', () => {
    const job = {
      ...baseJob,
      result: {
        ...baseResult,
        sortino_ratio: null,
      },
    } as BacktestJobResponse;

    render(<JobProgressCard job={job} />);
    expectSortinoValue('-');
  });
});
