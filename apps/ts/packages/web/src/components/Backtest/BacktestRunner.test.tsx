import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { BacktestRunner } from './BacktestRunner';

const mockInvalidateQueries = vi.fn();
const mockSetSelectedStrategy = vi.fn();
const mockSetActiveJobId = vi.fn();
const mockSetActiveOptimizationJobId = vi.fn();
const mockRunBacktestMutateAsync = vi.fn();
const mockCancelBacktestMutate = vi.fn();
const mockRunOptimizationMutateAsync = vi.fn();

const mockStore = {
  selectedStrategy: 'production/alpha' as string | null,
  setSelectedStrategy: mockSetSelectedStrategy,
  activeJobId: null as string | null,
  setActiveJobId: mockSetActiveJobId,
  activeOptimizationJobId: null as string | null,
  setActiveOptimizationJobId: mockSetActiveOptimizationJobId,
};

const mockHookState = {
  strategiesData: {
    data: {
      strategies: [{ name: 'production/alpha', category: 'production', display_name: null }],
    },
    isLoading: false,
  },
  strategyDetail: null as
    | {
        name: string;
        display_name: string | null;
        category: string;
        description: string | null;
      }
    | null,
  jobStatus: {
    data: null as { status?: string } | null,
    isLoading: false,
  },
  runBacktest: {
    isPending: false,
    isError: false,
    error: null as Error | null,
  },
  cancelBacktest: {
    isPending: false,
  },
  optimizationJobStatus: {
    data: null as { status?: string } | null,
    isLoading: false,
  },
  runOptimization: {
    isPending: false,
    isError: false,
    error: null as Error | null,
  },
  gridConfig: null as { content: string; param_count: number; combinations: number } | null,
  gridEntries: [] as Array<{ path: string; values: unknown[] }>,
};

vi.mock('@tanstack/react-query', async () => {
  const actual = await vi.importActual<typeof import('@tanstack/react-query')>('@tanstack/react-query');
  return {
    ...actual,
    useQueryClient: () => ({
      invalidateQueries: mockInvalidateQueries,
    }),
  };
});

vi.mock('@/stores/backtestStore', () => ({
  useBacktestStore: () => mockStore,
}));

vi.mock('@/hooks/useBacktest', () => ({
  backtestKeys: {
    htmlFiles: () => ['backtest', 'html-files'],
  },
  useStrategies: () => ({
    data: mockHookState.strategiesData.data,
    isLoading: mockHookState.strategiesData.isLoading,
  }),
  useStrategy: () => ({
    data: mockHookState.strategyDetail,
  }),
  useJobStatus: () => ({
    data: mockHookState.jobStatus.data,
    isLoading: mockHookState.jobStatus.isLoading,
  }),
  useRunBacktest: () => ({
    mutateAsync: mockRunBacktestMutateAsync,
    isPending: mockHookState.runBacktest.isPending,
    isError: mockHookState.runBacktest.isError,
    error: mockHookState.runBacktest.error,
  }),
  useCancelBacktest: () => ({
    mutate: mockCancelBacktestMutate,
    isPending: mockHookState.cancelBacktest.isPending,
  }),
}));

vi.mock('@/hooks/useOptimization', () => ({
  optimizationKeys: {
    htmlFiles: () => ['optimization', 'html-files'],
  },
  useOptimizationJobStatus: () => ({
    data: mockHookState.optimizationJobStatus.data,
    isLoading: mockHookState.optimizationJobStatus.isLoading,
  }),
  useRunOptimization: () => ({
    mutateAsync: mockRunOptimizationMutateAsync,
    isPending: mockHookState.runOptimization.isPending,
    isError: mockHookState.runOptimization.isError,
    error: mockHookState.runOptimization.error,
  }),
  useOptimizationGridConfig: () => ({
    data: mockHookState.gridConfig,
  }),
}));

vi.mock('./optimizationGridParams', () => ({
  extractGridParameterEntries: () => mockHookState.gridEntries,
  formatGridParameterValue: (value: unknown) => String(value),
}));

vi.mock('./StrategySelector', () => ({
  StrategySelector: ({
    value,
    onChange,
    disabled,
  }: {
    value: string | null;
    onChange: (next: string | null) => void;
    disabled: boolean;
  }) => (
    <div>
      <div>strategy-value:{value ?? 'none'}</div>
      <div>strategy-disabled:{String(disabled)}</div>
      <button type="button" onClick={() => onChange('experimental/next')}>
        Select Strategy
      </button>
    </div>
  ),
}));

vi.mock('./DefaultConfigEditor', () => ({
  DefaultConfigEditor: ({
    open,
    onOpenChange,
  }: {
    open: boolean;
    onOpenChange: (next: boolean) => void;
  }) => (
    <div>
      <div>default-config-open:{String(open)}</div>
      <button type="button" onClick={() => onOpenChange(false)}>
        Close Default Config
      </button>
    </div>
  ),
}));

vi.mock('./JobProgressCard', () => ({
  JobProgressCard: ({
    isLoading,
    isCancelling,
    onCancel,
  }: {
    isLoading: boolean;
    isCancelling: boolean;
    onCancel?: () => void;
  }) => (
    <div>
      <div>job-loading:{String(isLoading)}</div>
      <div>job-cancelling:{String(isCancelling)}</div>
      {onCancel ? (
        <button type="button" onClick={onCancel}>
          Cancel Active Job
        </button>
      ) : null}
    </div>
  ),
}));

vi.mock('./OptimizationJobProgressCard', () => ({
  OptimizationJobProgressCard: ({ isLoading }: { isLoading: boolean }) => (
    <div>optimization-job-loading:{String(isLoading)}</div>
  ),
}));

describe('BacktestRunner', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockStore.selectedStrategy = 'production/alpha';
    mockStore.activeJobId = null;
    mockStore.activeOptimizationJobId = null;

    mockHookState.strategyDetail = null;
    mockHookState.jobStatus.data = null;
    mockHookState.jobStatus.isLoading = false;
    mockHookState.runBacktest.isPending = false;
    mockHookState.runBacktest.isError = false;
    mockHookState.runBacktest.error = null;
    mockHookState.cancelBacktest.isPending = false;
    mockHookState.optimizationJobStatus.data = null;
    mockHookState.optimizationJobStatus.isLoading = false;
    mockHookState.runOptimization.isPending = false;
    mockHookState.runOptimization.isError = false;
    mockHookState.runOptimization.error = null;
    mockHookState.gridConfig = null;
    mockHookState.gridEntries = [];

    mockRunBacktestMutateAsync.mockResolvedValue({ job_id: 'job-1' });
    mockRunOptimizationMutateAsync.mockResolvedValue({ job_id: 'opt-1' });
  });

  it('renders strategy detail and fallback text when grid config is missing', () => {
    mockHookState.strategyDetail = {
      name: 'production/alpha',
      display_name: 'Alpha Strategy',
      category: 'production',
      description: 'Alpha description',
    };

    render(<BacktestRunner />);

    expect(screen.getByText('strategy-value:production/alpha')).toBeInTheDocument();
    expect(screen.getByText('Alpha Strategy')).toBeInTheDocument();
    expect(screen.getByText('Category: production')).toBeInTheDocument();
    expect(screen.getByText('Alpha description')).toBeInTheDocument();
    expect(screen.getByText('No grid config found. Configure in Strategies > Optimize tab.')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Run Optimization' })).toBeDisabled();
  });

  it('opens default config editor when button is clicked', async () => {
    const user = userEvent.setup();
    render(<BacktestRunner />);

    expect(screen.getByText('default-config-open:false')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Default Config' }));

    expect(screen.getByText('default-config-open:true')).toBeInTheDocument();
  });

  it('runs backtest and stores active job id', async () => {
    const user = userEvent.setup();
    render(<BacktestRunner />);

    await user.click(screen.getByRole('button', { name: 'Run Backtest' }));

    expect(mockRunBacktestMutateAsync).toHaveBeenCalledWith({ strategy_name: 'production/alpha' });
    await waitFor(() => {
      expect(mockSetActiveJobId).toHaveBeenCalledWith('job-1');
    });
  });

  it('disables backtest run when strategy is not selected', () => {
    mockStore.selectedStrategy = null;
    render(<BacktestRunner />);

    expect(screen.getByRole('button', { name: 'Run Backtest' })).toBeDisabled();
  });

  it('shows running state, disables selector, and supports cancellation', async () => {
    const user = userEvent.setup();
    mockStore.activeJobId = 'job-running';
    mockHookState.jobStatus.data = { status: 'running' };

    render(<BacktestRunner />);

    expect(screen.getByRole('button', { name: 'Running...' })).toBeDisabled();
    expect(screen.getByText('strategy-disabled:true')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Cancel Active Job' }));
    expect(mockCancelBacktestMutate).toHaveBeenCalledWith('job-running');
  });

  it('shows backtest error and invalidates result html list on completion', () => {
    mockHookState.runBacktest.isError = true;
    mockHookState.runBacktest.error = new Error('backtest failed');
    mockHookState.jobStatus.data = { status: 'completed' };

    render(<BacktestRunner />);

    expect(screen.getByText('backtest failed')).toBeInTheDocument();
    expect(mockInvalidateQueries).toHaveBeenCalledWith({ queryKey: ['backtest', 'html-files'] });
  });

  it('runs optimization with grid config and displays grid entry details', async () => {
    const user = userEvent.setup();
    mockHookState.gridConfig = {
      content: 'dummy',
      param_count: 2,
      combinations: 12,
    };
    mockHookState.gridEntries = [
      { path: 'entry_filter_params.signal_a', values: [1, 2] },
      { path: 'exit_trigger_params.signal_b', values: ['x', 'y'] },
    ];

    render(<BacktestRunner />);

    expect(screen.getByText('Grid config: 2 params, 12 combinations')).toBeInTheDocument();
    expect(screen.getByText('entry_filter_params.signal_a: [1, 2]')).toBeInTheDocument();
    expect(screen.getByText('exit_trigger_params.signal_b: [x, y]')).toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'Run Optimization' }));
    expect(mockRunOptimizationMutateAsync).toHaveBeenCalledWith({ strategy_name: 'production/alpha' });
    await waitFor(() => {
      expect(mockSetActiveOptimizationJobId).toHaveBeenCalledWith('opt-1');
    });
  });

  it('shows optimization running and error states, then invalidates html list on completion', () => {
    mockHookState.gridConfig = {
      content: 'dummy',
      param_count: 1,
      combinations: 1,
    };
    mockHookState.runOptimization.isPending = true;
    mockHookState.runOptimization.isError = true;
    mockHookState.runOptimization.error = new Error('optimization failed');
    mockHookState.optimizationJobStatus.data = { status: 'failed' };

    render(<BacktestRunner />);

    expect(screen.getByRole('button', { name: 'Optimizing...' })).toBeDisabled();
    expect(screen.getByText('strategy-disabled:true')).toBeInTheDocument();
    expect(screen.getByText('optimization failed')).toBeInTheDocument();
    expect(mockInvalidateQueries).toHaveBeenCalledWith({ queryKey: ['optimization', 'html-files'] });
  });
});
