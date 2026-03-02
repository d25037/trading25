import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';
import type { ScreeningJobResponse } from '@/types/screening';
import { ScreeningJobHistoryTable } from './ScreeningJobHistoryTable';

function createJob(overrides: Partial<ScreeningJobResponse> = {}): ScreeningJobResponse {
  return {
    job_id: 'job-1',
    status: 'completed',
    created_at: '2026-02-18T12:00:00Z',
    markets: 'prime',
    recentDays: 10,
    sortBy: 'matchedDate',
    order: 'desc',
    ...overrides,
  };
}

describe('ScreeningJobHistoryTable', () => {
  it('shows empty state', () => {
    render(<ScreeningJobHistoryTable jobs={[]} isLoading={false} selectedJobId={null} onSelectJob={vi.fn()} />);
    expect(screen.getByText('No screening jobs found')).toBeInTheDocument();
  });

  it('renders jobs and selects a completed job', async () => {
    const user = userEvent.setup();
    const onSelectJob = vi.fn();
    const job = createJob({ job_id: 'job-completed', strategies: 'production/range_break_v15' });

    render(<ScreeningJobHistoryTable jobs={[job]} isLoading={false} selectedJobId={null} onSelectJob={onSelectJob} />);

    expect(screen.getByText('job-comp...')).toBeInTheDocument();
    expect(screen.getByText('production/range_break_v15')).toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'View' }));
    expect(onSelectJob).toHaveBeenCalledWith(job);
  });

  it('shows monitor action for pending job', () => {
    const job = createJob({ job_id: 'job-pending', status: 'pending' });
    render(<ScreeningJobHistoryTable jobs={[job]} isLoading={false} selectedJobId={null} onSelectJob={vi.fn()} />);
    expect(screen.getByRole('button', { name: 'Monitor' })).toBeInTheDocument();
  });
});
