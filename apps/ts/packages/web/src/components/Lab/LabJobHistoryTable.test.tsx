import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';
import type { LabJobResponse } from '@/types/backtest';
import { LabJobHistoryTable } from './LabJobHistoryTable';

function createJob(overrides: Partial<LabJobResponse> = {}): LabJobResponse {
  return {
    job_id: 'job-1',
    status: 'completed',
    created_at: '2026-02-16T12:00:00Z',
    lab_type: 'generate',
    strategy_name: 'experimental/demo',
    ...overrides,
  };
}

describe('LabJobHistoryTable', () => {
  it('shows loading state', () => {
    render(
      <LabJobHistoryTable
        jobs={undefined}
        isLoading
        isRefreshing={false}
        selectedJobId={null}
        onSelectJob={vi.fn()}
        onRefresh={vi.fn()}
      />
    );

    expect(screen.getByText('Job History')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Refresh' })).toBeInTheDocument();
    expect(screen.queryByText('No lab jobs found')).not.toBeInTheDocument();
  });

  it('shows empty state when jobs are missing', () => {
    render(
      <LabJobHistoryTable
        jobs={[]}
        isLoading={false}
        isRefreshing={false}
        selectedJobId={null}
        onSelectJob={vi.fn()}
        onRefresh={vi.fn()}
      />
    );

    expect(screen.getByText('No lab jobs found')).toBeInTheDocument();
  });

  it('renders jobs and handles select/refresh actions', async () => {
    const user = userEvent.setup();
    const onSelectJob = vi.fn();
    const onRefresh = vi.fn();
    const job = createJob();

    render(
      <LabJobHistoryTable
        jobs={[job]}
        isLoading={false}
        isRefreshing={false}
        selectedJobId={null}
        onSelectJob={onSelectJob}
        onRefresh={onRefresh}
      />
    );

    expect(screen.getByText('Job History')).toBeInTheDocument();
    expect(screen.getByText('experimental/demo')).toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'View' }));
    expect(onSelectJob).toHaveBeenCalledWith(job);

    await user.click(screen.getByRole('button', { name: 'Refresh' }));
    expect(onRefresh).toHaveBeenCalledTimes(1);
  });

  it('renders monitor action for active statuses and handles missing fields', async () => {
    const user = userEvent.setup();
    const onSelectJob = vi.fn();
    const jobs: LabJobResponse[] = [
      createJob({ job_id: 'pending-1', status: 'pending', created_at: undefined }),
      createJob({ job_id: 'running-1', status: 'running', lab_type: 'optimize' }),
      createJob({ job_id: 'failed-1', status: 'failed', strategy_name: undefined }),
      createJob({ job_id: 'cancelled-1', status: 'cancelled' }),
    ];

    render(
      <LabJobHistoryTable
        jobs={jobs}
        isLoading={false}
        isRefreshing
        selectedJobId="running-1"
        onSelectJob={onSelectJob}
        onRefresh={vi.fn()}
      />
    );

    const refreshButton = screen.getByRole('button', { name: 'Refresh' });
    expect(refreshButton).toBeDisabled();
    expect(screen.getAllByText('-').length).toBeGreaterThanOrEqual(1);

    await user.click(screen.getAllByRole('button', { name: 'Monitor' })[0]);
    expect(onSelectJob).toHaveBeenCalledWith(jobs[0]);
  });
});
