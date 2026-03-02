import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';
import { JobHistoryTable, type JobHistoryColumn } from './JobHistoryTable';

type MockJob = {
  id: string;
  status: 'pending' | 'running' | 'completed' | 'failed' | 'cancelled' | 'queued';
  name: string;
};

const columns: JobHistoryColumn<MockJob>[] = [
  {
    key: 'name',
    header: 'Name',
    render: (job) => job.name,
  },
];

describe('JobHistoryTable', () => {
  it('renders loading and empty states', () => {
    const { rerender } = render(
      <JobHistoryTable
        jobs={undefined}
        isLoading
        emptyMessage="No jobs"
        columns={columns}
        getJobId={(job) => job.id}
        getStatus={(job) => job.status}
        onSelectJob={vi.fn()}
      />
    );
    expect(screen.queryByText('No jobs')).not.toBeInTheDocument();

    rerender(
      <JobHistoryTable
        jobs={[]}
        isLoading={false}
        emptyMessage="No jobs"
        columns={columns}
        getJobId={(job) => job.id}
        getStatus={(job) => job.status}
        onSelectJob={vi.fn()}
      />
    );
    expect(screen.getByText('No jobs')).toBeInTheDocument();
  });

  it('renders rows and handles select/refresh', async () => {
    const user = userEvent.setup();
    const onSelectJob = vi.fn();
    const onRefresh = vi.fn();
    const jobs: MockJob[] = [
      { id: 'job-1', status: 'pending', name: 'First' },
      { id: 'job-2', status: 'completed', name: 'Second' },
    ];

    render(
      <JobHistoryTable
        jobs={jobs}
        isLoading={false}
        isRefreshing={false}
        title="History"
        emptyMessage="No jobs"
        columns={columns}
        getJobId={(job) => job.id}
        getStatus={(job) => job.status}
        getActionLabel={(job) => (job.status === 'completed' ? 'View' : 'Monitor')}
        onSelectJob={onSelectJob}
        onRefresh={onRefresh}
      />
    );

    expect(screen.getByText('History')).toBeInTheDocument();
    expect(screen.getByText('First')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Monitor' })).toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'View' }));
    expect(onSelectJob).toHaveBeenCalledWith(jobs[1]);

    await user.click(screen.getByRole('button', { name: 'Refresh' }));
    expect(onRefresh).toHaveBeenCalledTimes(1);
  });

  it('shows placeholder when job selection is disabled', () => {
    const jobs: MockJob[] = [{ id: 'job-1', status: 'failed', name: 'Failed' }];
    render(
      <JobHistoryTable
        jobs={jobs}
        isLoading={false}
        emptyMessage="No jobs"
        columns={columns}
        getJobId={(job) => job.id}
        getStatus={(job) => job.status}
        canSelectJob={() => false}
        onSelectJob={vi.fn()}
      />
    );

    expect(screen.getByText('-')).toBeInTheDocument();
  });

  it('uses default action labels and status icons', () => {
    const { container } = render(
      <JobHistoryTable
        jobs={[
          { id: 'job-running', status: 'running', name: 'Running' },
          { id: 'job-cancelled', status: 'cancelled', name: 'Cancelled' },
          { id: 'job-queued', status: 'queued', name: 'Queued' },
        ]}
        isLoading={false}
        emptyMessage="No jobs"
        columns={columns}
        getJobId={(job) => job.id}
        getStatus={(job) => job.status}
        onSelectJob={vi.fn()}
      />
    );

    expect(screen.getByRole('button', { name: 'Monitor' })).toBeInTheDocument();
    expect(screen.getAllByRole('button', { name: 'View' })).toHaveLength(2);
    expect(container.querySelector('.text-blue-500')).not.toBeNull();
    expect(container.querySelector('.text-orange-500')).not.toBeNull();

    const queuedCell = screen.getByText('Queued').closest('tr')?.querySelector('td');
    expect(queuedCell).not.toBeNull();
    expect(queuedCell?.querySelector('svg')).toBeNull();
  });

  it('renders refresh action without title and respects refreshing state', async () => {
    const user = userEvent.setup();
    const onRefresh = vi.fn();

    const { container } = render(
      <JobHistoryTable
        jobs={[{ id: 'job-1', status: 'pending', name: 'First' }]}
        isLoading={false}
        isRefreshing
        emptyMessage="No jobs"
        columns={columns}
        getJobId={(job) => job.id}
        getStatus={(job) => job.status}
        onSelectJob={vi.fn()}
        onRefresh={onRefresh}
      />
    );

    const refreshButton = screen.getByRole('button', { name: 'Refresh' });
    expect(refreshButton).toBeDisabled();
    const icon = refreshButton.querySelector('svg');
    expect(icon).not.toBeNull();
    expect(icon?.getAttribute('class')).toContain('animate-spin');

    await user.click(refreshButton);
    expect(onRefresh).toHaveBeenCalledTimes(0);

    const headerSpacer = container.querySelector('.flex.items-center.justify-between span');
    expect(headerSpacer).not.toBeNull();
  });
});
