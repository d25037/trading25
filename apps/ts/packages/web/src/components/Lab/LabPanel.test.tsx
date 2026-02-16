import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { LabPanel } from './LabPanel';

const mockSetSelectedStrategy = vi.fn();
const mockSetActiveLabJobId = vi.fn();
const mockSetActiveLabType = vi.fn();
const mockGenerateMutateAsync = vi.fn();
const mockEvolveMutateAsync = vi.fn();
const mockOptimizeMutateAsync = vi.fn();
const mockImproveMutateAsync = vi.fn();
const mockCancelMutate = vi.fn();
const mockRefetchLabJobs = vi.fn();

const mockStore = {
  selectedStrategy: 'strategy.yml' as string | null,
  setSelectedStrategy: mockSetSelectedStrategy,
  activeLabJobId: null as string | null,
  setActiveLabJobId: mockSetActiveLabJobId,
  setActiveLabType: mockSetActiveLabType,
};

const mockHookState = {
  strategies: {
    data: {
      strategies: [{ name: 'strategy.yml', display_name: 'Strategy', category: 'production' }],
    },
    isLoading: false,
  },
  labJobs: {
    data: [] as Array<Record<string, unknown>>,
    isLoading: false,
    isFetching: false,
    refetch: mockRefetchLabJobs,
  },
  sse: {
    status: null as string | null,
    progress: null as number | null,
    message: null as string | null,
    isConnected: false,
  },
  jobStatus: {
    data: null as Record<string, unknown> | null,
  },
  generate: {
    isPending: false,
    error: null as Error | null,
  },
  evolve: {
    isPending: false,
    error: null as Error | null,
  },
  optimize: {
    isPending: false,
    error: null as Error | null,
  },
  improve: {
    isPending: false,
    error: null as Error | null,
  },
  cancel: {
    isPending: false,
  },
};

vi.mock('@/stores/backtestStore', () => ({
  useBacktestStore: () => mockStore,
}));

vi.mock('@/hooks/useBacktest', () => ({
  useStrategies: () => mockHookState.strategies,
}));

vi.mock('@/hooks/useLabSSE', () => ({
  useLabSSE: () => mockHookState.sse,
}));

vi.mock('@/hooks/useLab', () => ({
  useLabGenerate: () => ({
    mutateAsync: mockGenerateMutateAsync,
    isPending: mockHookState.generate.isPending,
    error: mockHookState.generate.error,
  }),
  useLabEvolve: () => ({
    mutateAsync: mockEvolveMutateAsync,
    isPending: mockHookState.evolve.isPending,
    error: mockHookState.evolve.error,
  }),
  useLabOptimize: () => ({
    mutateAsync: mockOptimizeMutateAsync,
    isPending: mockHookState.optimize.isPending,
    error: mockHookState.optimize.error,
  }),
  useLabImprove: () => ({
    mutateAsync: mockImproveMutateAsync,
    isPending: mockHookState.improve.isPending,
    error: mockHookState.improve.error,
  }),
  useCancelLabJob: () => ({
    mutate: mockCancelMutate,
    isPending: mockHookState.cancel.isPending,
  }),
  useLabJobs: () => mockHookState.labJobs,
  useLabJobStatus: () => mockHookState.jobStatus,
}));

vi.mock('@/components/Backtest/StrategySelector', () => ({
  StrategySelector: ({ value, onChange }: { value: string | null; onChange: (value: string) => void }) => (
    <button type="button" onClick={() => onChange('strategy-updated')}>
      Strategy Selector {value ?? 'none'}
    </button>
  ),
}));

vi.mock('./LabOperationSelector', () => ({
  LabOperationSelector: ({ value, onChange }: { value: string; onChange: (value: string) => void }) => (
    <div>
      <span>Operation:{value}</span>
      <button type="button" onClick={() => onChange('optimize')}>
        Switch Operation
      </button>
    </div>
  ),
}));

vi.mock('./LabGenerateForm', () => ({
  LabGenerateForm: ({
    onSubmit,
    disabled,
  }: {
    onSubmit: (req: { count: number }) => Promise<void>;
    disabled: boolean;
  }) => (
    <button type="button" disabled={disabled} onClick={() => void onSubmit({ count: 1 })}>
      Run Generate
    </button>
  ),
}));

vi.mock('./LabEvolveForm', () => ({
  LabEvolveForm: () => <div>Evolve Form</div>,
}));

vi.mock('./LabOptimizeForm', () => ({
  LabOptimizeForm: () => <div>Optimize Form</div>,
}));

vi.mock('./LabImproveForm', () => ({
  LabImproveForm: () => <div>Improve Form</div>,
}));

vi.mock('./LabJobHistoryTable', () => ({
  LabJobHistoryTable: ({
    jobs,
    onSelectJob,
    onRefresh,
  }: {
    jobs: Array<Record<string, unknown>> | undefined;
    onSelectJob: (job: Record<string, unknown>) => void;
    onRefresh: () => void;
  }) => (
    <div>
      <div>History Table</div>
      <button type="button" onClick={onRefresh}>
        History Refresh
      </button>
      <button type="button" onClick={() => jobs?.[0] && onSelectJob(jobs[0])}>
        History Select
      </button>
    </div>
  ),
}));

vi.mock('./LabJobProgress', () => ({
  LabJobProgress: ({ status, onCancel }: { status: string; onCancel?: () => void }) => (
    <div>
      <div>Progress:{status}</div>
      {onCancel ? (
        <button type="button" onClick={onCancel}>
          Cancel Job
        </button>
      ) : null}
    </div>
  ),
}));

vi.mock('./LabResultSection', () => ({
  LabResultSection: () => <div>Result Section</div>,
}));

describe('LabPanel', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockStore.selectedStrategy = 'strategy.yml';
    mockStore.activeLabJobId = null;

    mockHookState.labJobs.data = [];
    mockHookState.labJobs.isLoading = false;
    mockHookState.labJobs.isFetching = false;

    mockHookState.sse.status = null;
    mockHookState.sse.progress = null;
    mockHookState.sse.message = null;
    mockHookState.sse.isConnected = false;

    mockHookState.jobStatus.data = null;

    mockHookState.generate.isPending = false;
    mockHookState.generate.error = null;
    mockHookState.evolve.isPending = false;
    mockHookState.evolve.error = null;
    mockHookState.optimize.isPending = false;
    mockHookState.optimize.error = null;
    mockHookState.improve.isPending = false;
    mockHookState.improve.error = null;
    mockHookState.cancel.isPending = false;

    mockGenerateMutateAsync.mockResolvedValue({ job_id: 'lab-gen-1' });
  });

  it('starts generate job and updates active lab state', async () => {
    const user = userEvent.setup();

    render(<LabPanel />);

    await user.click(screen.getByRole('button', { name: 'Run Generate' }));

    await waitFor(() => expect(mockGenerateMutateAsync).toHaveBeenCalledWith({ count: 1 }));
    expect(mockSetActiveLabJobId).toHaveBeenCalledWith('lab-gen-1');
    expect(mockSetActiveLabType).toHaveBeenCalledWith('generate');
  });

  it('shows history tab and applies selected history job type', async () => {
    const user = userEvent.setup();
    mockHookState.labJobs.data = [
      {
        job_id: 'hist-1',
        status: 'completed',
        lab_type: 'optimize',
        strategy_name: 'strategy.yml',
        created_at: '2026-02-16T10:00:00Z',
      },
    ];

    render(<LabPanel />);

    await user.click(screen.getByRole('button', { name: 'History' }));
    expect(screen.getByText('History Table')).toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'History Refresh' }));
    expect(mockRefetchLabJobs).toHaveBeenCalledTimes(1);

    await user.click(screen.getByRole('button', { name: 'History Select' }));
    expect(mockSetActiveLabJobId).toHaveBeenCalledWith('hist-1');
    expect(mockSetActiveLabType).toHaveBeenCalledWith('optimize');

    await user.click(screen.getByRole('button', { name: 'Run' }));
    expect(screen.getByText('Operation:optimize')).toBeInTheDocument();
    expect(screen.getByText('Optimize Form')).toBeInTheDocument();
  });

  it('renders running progress and triggers cancel', async () => {
    const user = userEvent.setup();
    mockStore.activeLabJobId = 'running-1';
    mockHookState.sse.status = 'running';
    mockHookState.sse.progress = 0.4;
    mockHookState.sse.message = 'running';
    mockHookState.jobStatus.data = {
      status: 'running',
      created_at: '2026-02-16T10:00:00Z',
      started_at: '2026-02-16T10:00:01Z',
      error: null,
      result_data: null,
    };

    render(<LabPanel />);

    expect(screen.getByText('Progress:running')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Cancel Job' }));
    expect(mockCancelMutate).toHaveBeenCalledWith('running-1');
    expect(screen.queryByText('Result Section')).not.toBeInTheDocument();
  });

  it('renders result section on completed status and shows mutation error', () => {
    mockStore.activeLabJobId = 'done-1';
    mockHookState.generate.error = new Error('generate failed');
    mockHookState.sse.status = 'completed';
    mockHookState.jobStatus.data = {
      status: 'completed',
      created_at: '2026-02-16T10:00:00Z',
      started_at: '2026-02-16T10:00:01Z',
      error: null,
      result_data: {
        lab_type: 'generate',
        total_generated: 1,
        results: [],
      },
    };

    render(<LabPanel />);

    expect(screen.getByText('generate failed')).toBeInTheDocument();
    expect(screen.getByText('Result Section')).toBeInTheDocument();
  });
});
