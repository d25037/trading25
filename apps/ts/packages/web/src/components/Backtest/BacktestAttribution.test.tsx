import { render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { BacktestAttribution } from './BacktestAttribution';

const mockRunMutateAsync = vi.fn();
const mockCancelMutate = vi.fn();
const mockSetSelectedStrategy = vi.fn();
const mockSetActiveAttributionJobId = vi.fn();

const mockStore = {
  selectedStrategy: 'strategy.yml' as string | null,
  setSelectedStrategy: mockSetSelectedStrategy,
  activeAttributionJobId: null as string | null,
  setActiveAttributionJobId: mockSetActiveAttributionJobId,
};

const mockHookState = {
  strategies: {
    data: {
      strategies: [
        {
          name: 'strategy.yml',
          display_name: 'Strategy',
          category: 'production',
          description: 'test',
          last_modified: '2025-01-01T00:00:00Z',
        },
      ],
    },
    isLoading: false,
  },
  run: {
    isPending: false,
    isError: false,
    error: null as Error | null,
  },
  cancel: {
    isPending: false,
    isError: false,
    error: null as Error | null,
  },
  jobStatus: {
    data: null as Record<string, unknown> | null,
  },
  result: {
    data: null as Record<string, unknown> | null,
    isError: false,
    error: null as Error | null,
  },
};

vi.mock('@/stores/backtestStore', () => ({
  useBacktestStore: () => mockStore,
}));

vi.mock('@/hooks/useBacktest', () => ({
  useStrategies: () => mockHookState.strategies,
  useRunSignalAttribution: () => ({
    mutateAsync: mockRunMutateAsync,
    isPending: mockHookState.run.isPending,
    isError: mockHookState.run.isError,
    error: mockHookState.run.error,
  }),
  useCancelSignalAttribution: () => ({
    mutate: mockCancelMutate,
    isPending: mockHookState.cancel.isPending,
    isError: mockHookState.cancel.isError,
    error: mockHookState.cancel.error,
  }),
  useSignalAttributionJobStatus: () => mockHookState.jobStatus,
  useSignalAttributionResult: () => mockHookState.result,
}));

vi.mock('./AttributionArtifactBrowser', () => ({
  AttributionArtifactBrowser: () => <div>Attribution History Panel</div>,
}));

describe('BacktestAttribution', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockStore.selectedStrategy = 'strategy.yml';
    mockStore.activeAttributionJobId = null;
    mockHookState.run.isPending = false;
    mockHookState.run.isError = false;
    mockHookState.run.error = null;
    mockHookState.cancel.isPending = false;
    mockHookState.cancel.isError = false;
    mockHookState.cancel.error = null;
    mockHookState.jobStatus.data = null;
    mockHookState.result.data = null;
    mockHookState.result.isError = false;
    mockHookState.result.error = null;
    mockRunMutateAsync.mockResolvedValue({ job_id: 'attr-1', status: 'pending' });
  });

  it('sends advanced parameters when running attribution', async () => {
    const user = userEvent.setup();

    render(<BacktestAttribution />);

    await user.click(screen.getByRole('button', { name: /Advanced Parameters/i }));
    await user.clear(screen.getByLabelText('Shapley Top N'));
    await user.type(screen.getByLabelText('Shapley Top N'), '8');
    await user.clear(screen.getByLabelText('Shapley Permutations'));
    await user.type(screen.getByLabelText('Shapley Permutations'), '256');
    await user.type(screen.getByLabelText('Random Seed (optional)'), '42');

    await user.click(screen.getByRole('button', { name: /Run Signal Attribution/i }));

    expect(mockRunMutateAsync).toHaveBeenCalledWith({
      strategy_name: 'strategy.yml',
      shapley_top_n: 8,
      shapley_permutations: 256,
      random_seed: 42,
    });
    expect(mockSetActiveAttributionJobId).toHaveBeenCalledWith('attr-1');
  });

  it('renders running job and triggers cancel', async () => {
    const user = userEvent.setup();
    mockStore.activeAttributionJobId = 'attr-running';
    mockHookState.jobStatus.data = {
      job_id: 'attr-running',
      status: 'running',
      progress: 0.5,
      message: 'running',
      created_at: '2025-01-01T00:00:00Z',
      started_at: '2025-01-01T00:00:01Z',
      completed_at: null,
      error: null,
      result_data: null,
    };

    render(<BacktestAttribution />);

    expect(screen.getByText('Job ID: attr-running')).toBeInTheDocument();
    expect(screen.getByText('50.0%')).toBeInTheDocument();
    expect(screen.getByRole('progressbar')).toHaveAttribute('aria-valuenow', '50');
    await user.click(screen.getByRole('button', { name: 'Cancel' }));
    expect(mockCancelMutate).toHaveBeenCalledWith('attr-running');
  });

  it('renders indeterminate progress when progress value is missing', () => {
    mockStore.activeAttributionJobId = 'attr-running-no-progress';
    mockHookState.jobStatus.data = {
      job_id: 'attr-running-no-progress',
      status: 'running',
      progress: null,
      message: 'running',
      created_at: '2025-01-01T00:00:00Z',
      started_at: '2025-01-01T00:00:01Z',
      completed_at: null,
      error: null,
      result_data: null,
    };

    render(<BacktestAttribution />);

    expect(screen.getByText('Tracking...')).toBeInTheDocument();
    expect(screen.getByRole('progressbar')).not.toHaveAttribute('aria-valuenow');
  });

  it('clamps out-of-range progress values into 0-100 for display', () => {
    mockStore.activeAttributionJobId = 'attr-out-of-range-progress';
    mockHookState.jobStatus.data = {
      job_id: 'attr-out-of-range-progress',
      status: 'running',
      progress: 1.4,
      message: 'running',
      created_at: '2025-01-01T00:00:00Z',
      started_at: '2025-01-01T00:00:01Z',
      completed_at: null,
      error: null,
      result_data: null,
    };

    render(<BacktestAttribution />);

    expect(screen.getByText('100.0%')).toBeInTheDocument();
    expect(screen.getByRole('progressbar')).toHaveAttribute('aria-valuenow', '100');
  });

  it('shows dash placeholders for non-topN shapley cells', () => {
    mockStore.activeAttributionJobId = 'attr-completed';
    mockHookState.jobStatus.data = {
      job_id: 'attr-completed',
      status: 'completed',
      progress: 1,
      message: null,
      created_at: '2025-01-01T00:00:00Z',
      started_at: '2025-01-01T00:00:01Z',
      completed_at: '2025-01-01T00:01:00Z',
      error: null,
      result_data: {
        baseline_metrics: { total_return: 0.2, sharpe_ratio: 1.1 },
        top_n_selection: {
          top_n_requested: 5,
          top_n_effective: 1,
          selected_signal_ids: ['entry.signal_a'],
          scores: [{ signal_id: 'entry.signal_a', score: 1 }],
        },
        timing: {
          total_seconds: 1,
          baseline_seconds: 0.2,
          loo_seconds: 0.4,
          shapley_seconds: 0.4,
        },
        shapley: { method: 'exact', sample_size: 2, error: null, evaluations: 2 },
        signals: [
          {
            signal_id: 'entry.signal_a',
            scope: 'entry',
            param_key: 'signal_a',
            signal_name: 'Signal A',
            loo: {
              status: 'ok',
              variant_metrics: { total_return: 0.1, sharpe_ratio: 1.0 },
              delta_total_return: 0.1,
              delta_sharpe_ratio: 0.1,
              error: null,
            },
            shapley: {
              status: 'ok',
              total_return: 0.1,
              sharpe_ratio: 0.05,
              method: 'exact',
              sample_size: 2,
              error: null,
            },
          },
          {
            signal_id: 'entry.signal_b',
            scope: 'entry',
            param_key: 'signal_b',
            signal_name: 'Signal B',
            loo: {
              status: 'ok',
              variant_metrics: { total_return: 0.18, sharpe_ratio: 1.05 },
              delta_total_return: 0.02,
              delta_sharpe_ratio: 0.05,
              error: null,
            },
            shapley: null,
          },
        ],
      },
    };

    render(<BacktestAttribution />);

    const row = screen.getByText('entry.signal_b').closest('tr');
    expect(row).not.toBeNull();
    expect(within(row as HTMLElement).getAllByText('-').length).toBeGreaterThanOrEqual(3);
  });

  it('shows validation error and skips submit for non-integer random seed', async () => {
    const user = userEvent.setup();
    render(<BacktestAttribution />);

    await user.click(screen.getByRole('button', { name: /Advanced Parameters/i }));
    await user.type(screen.getByLabelText('Random Seed (optional)'), '1.5');
    await user.click(screen.getByRole('button', { name: /Run Signal Attribution/i }));

    expect(screen.getByText('Random seed must be an integer.')).toBeInTheDocument();
    expect(mockRunMutateAsync).not.toHaveBeenCalled();
  });

  it('renders run/cancel/result error banners', () => {
    mockStore.activeAttributionJobId = 'attr-error';
    mockHookState.run.isError = true;
    mockHookState.run.error = new Error('run failed');
    mockHookState.cancel.isError = true;
    mockHookState.cancel.error = new Error('cancel failed');
    mockHookState.jobStatus.data = {
      job_id: 'attr-error',
      status: 'running',
      progress: 0.3,
      message: 'running',
      created_at: '2025-01-01T00:00:00Z',
      started_at: '2025-01-01T00:00:01Z',
      completed_at: null,
      error: null,
      result_data: null,
    };
    mockHookState.result.isError = true;
    mockHookState.result.error = new Error('result failed');

    render(<BacktestAttribution />);

    expect(screen.getByText('run failed')).toBeInTheDocument();
    expect(screen.getByText('cancel failed')).toBeInTheDocument();
    expect(screen.getByText('result failed')).toBeInTheDocument();
  });

  it('switches to history tab', async () => {
    const user = userEvent.setup();
    render(<BacktestAttribution />);

    await user.click(screen.getByRole('button', { name: 'History' }));
    expect(screen.getByText('Attribution History Panel')).toBeInTheDocument();
  });
});
