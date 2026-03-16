import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { BacktestJobResponse, HealthResponse } from '@/types/backtest';
import { BacktestStatus } from './BacktestStatus';

const mockOnViewJob = vi.fn();
const mockRefetchHealth = vi.fn();
const mockRefetchJobs = vi.fn();

const mockQueryState = {
  health: null as HealthResponse | null,
  isLoadingHealth: false,
  jobs: undefined as BacktestJobResponse[] | undefined,
  isLoadingJobs: false,
};

const mockUseBacktestHealth = vi.fn(() => ({
  data: mockQueryState.health,
  isLoading: mockQueryState.isLoadingHealth,
  refetch: mockRefetchHealth,
}));

const mockUseJobs = vi.fn((_limit?: number) => ({
  data: mockQueryState.jobs,
  isLoading: mockQueryState.isLoadingJobs,
  refetch: mockRefetchJobs,
}));

vi.mock('@/hooks/useBacktest', () => ({
  useBacktestHealth: () => mockUseBacktestHealth(),
  useJobs: (limit?: number) => mockUseJobs(limit),
}));

vi.mock('./JobsTable', () => ({
  JobsTable: ({
    jobs,
    isLoading,
    onSelectJob,
  }: {
    jobs: BacktestJobResponse[] | undefined;
    isLoading: boolean;
    onSelectJob: (jobId: string) => void;
  }) => (
    <div>
      <div>jobs-count:{jobs?.length ?? 0}</div>
      <div>jobs-loading:{String(isLoading)}</div>
      <button type="button" onClick={() => onSelectJob('selected-job-id')}>
        Select Job
      </button>
    </div>
  ),
}));

function renderBacktestStatus() {
  return render(<BacktestStatus onViewJob={mockOnViewJob} />);
}

describe('BacktestStatus', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockQueryState.health = null;
    mockQueryState.isLoadingHealth = false;
    mockQueryState.jobs = undefined;
    mockQueryState.isLoadingJobs = false;
  });

  it('renders loading state while health check is in progress', () => {
    mockQueryState.isLoadingHealth = true;

    renderBacktestStatus();

    expect(screen.getByText('Checking...')).toBeInTheDocument();
    expect(screen.queryByText('Connected')).not.toBeInTheDocument();
    expect(screen.queryByText('Disconnected')).not.toBeInTheDocument();
  });

  it('renders connected state with service metadata', () => {
    mockQueryState.health = {
      service: 'trading25-bt',
      version: '1.2.3',
      status: 'ok',
    } as HealthResponse;

    renderBacktestStatus();

    expect(screen.getByText('Connected')).toBeInTheDocument();
    expect(screen.getByText('trading25-bt')).toBeInTheDocument();
    expect(screen.getByText('1.2.3')).toBeInTheDocument();
    expect(screen.getByText('ok')).toBeInTheDocument();
  });

  it('renders disconnected state when health is unavailable', () => {
    renderBacktestStatus();

    expect(screen.getByText('Disconnected')).toBeInTheDocument();
    expect(screen.getByText('Make sure bt server is running on port 3002')).toBeInTheDocument();
  });

  it('refreshes health and jobs when Refresh is clicked', async () => {
    const user = userEvent.setup();
    renderBacktestStatus();

    await user.click(screen.getByRole('button', { name: 'Refresh' }));

    expect(mockRefetchHealth).toHaveBeenCalledTimes(1);
    expect(mockRefetchJobs).toHaveBeenCalledTimes(1);
  });

  it('passes jobs query state to JobsTable and requests last 20 jobs', () => {
    mockQueryState.jobs = [
      {
        job_id: 'job-1',
        status: 'completed',
      } as BacktestJobResponse,
      {
        job_id: 'job-2',
        status: 'running',
      } as BacktestJobResponse,
    ];
    mockQueryState.isLoadingJobs = true;

    renderBacktestStatus();

    expect(mockUseJobs).toHaveBeenCalledWith(20);
    expect(screen.getByText('jobs-count:2')).toBeInTheDocument();
    expect(screen.getByText('jobs-loading:true')).toBeInTheDocument();
  });

  it('moves to results tab when selecting a job from JobsTable', async () => {
    const user = userEvent.setup();
    renderBacktestStatus();

    await user.click(screen.getByRole('button', { name: 'Select Job' }));

    expect(mockOnViewJob).toHaveBeenCalledWith('selected-job-id');
  });
});
