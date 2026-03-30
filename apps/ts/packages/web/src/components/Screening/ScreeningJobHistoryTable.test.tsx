import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { useState } from 'react';
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
    render(
      <ScreeningJobHistoryTable
        entryDecidability="pre_open_decidable"
        jobs={[]}
        isLoading={false}
        showHistory
        onShowHistoryChange={vi.fn()}
        selectedJobId={null}
        onSelectJob={vi.fn()}
      />
    );
    expect(screen.getByText('No screening jobs found')).toBeInTheDocument();
  });

  it('renders jobs and selects a completed job', async () => {
    const user = userEvent.setup();
    const onSelectJob = vi.fn();
    const job = createJob({ job_id: 'job-completed', strategies: 'production/range_break_v15' });

    render(
      <ScreeningJobHistoryTable
        entryDecidability="pre_open_decidable"
        jobs={[job]}
        isLoading={false}
        showHistory
        onShowHistoryChange={vi.fn()}
        selectedJobId={null}
        onSelectJob={onSelectJob}
      />
    );

    expect(screen.getByText('job-comp...')).toBeInTheDocument();
    expect(screen.getByText('Prime')).toBeInTheDocument();
    expect(screen.getByText('production/range_break_v15')).toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'View' }));
    expect(onSelectJob).toHaveBeenCalledWith(job);
  });

  it('shows monitor action for pending job', () => {
    const job = createJob({ job_id: 'job-pending', status: 'pending' });
    render(
      <ScreeningJobHistoryTable
        entryDecidability="pre_open_decidable"
        jobs={[job]}
        isLoading={false}
        showHistory
        onShowHistoryChange={vi.fn()}
        selectedJobId={null}
        onSelectJob={vi.fn()}
      />
    );
    expect(screen.getByRole('button', { name: 'Monitor' })).toBeInTheDocument();
  });

  it('toggles job history visibility', async () => {
    const user = userEvent.setup();
    const job = createJob({ job_id: 'job-toggle' });

    function ControlledTable() {
      const [showHistory, setShowHistory] = useState(true);
      return (
        <ScreeningJobHistoryTable
          entryDecidability="pre_open_decidable"
          jobs={[job]}
          isLoading={false}
          showHistory={showHistory}
          onShowHistoryChange={setShowHistory}
          selectedJobId={null}
          onSelectJob={vi.fn()}
        />
      );
    }

    render(<ControlledTable />);

    const toggle = screen.getByRole('switch', { name: 'Show History' });
    expect(toggle).toBeChecked();
    expect(screen.getByRole('button', { name: 'View' })).toBeInTheDocument();

    await user.click(toggle);
    expect(toggle).not.toBeChecked();
    expect(screen.queryByRole('button', { name: 'View' })).not.toBeInTheDocument();

    await user.click(toggle);
    expect(toggle).toBeChecked();
    expect(screen.getByRole('button', { name: 'View' })).toBeInTheDocument();
  });

  it('shows same-day fallback label when strategies are omitted', () => {
    const job = createJob({ job_id: 'job-same-day', strategies: null });

    render(
      <ScreeningJobHistoryTable
        entryDecidability="requires_same_session_observation"
        jobs={[job]}
        isLoading={false}
        showHistory
        onShowHistoryChange={vi.fn()}
        selectedJobId={null}
        onSelectJob={vi.fn()}
      />
    );

    expect(screen.getByText('(all in-session production)')).toBeInTheDocument();
  });

  it('prefers scope label over raw markets in history rows', () => {
    const job = createJob({ job_id: 'job-topix', scopeLabel: 'TOPIX 500' });

    render(
      <ScreeningJobHistoryTable
        entryDecidability="pre_open_decidable"
        jobs={[job]}
        isLoading={false}
        showHistory
        onShowHistoryChange={vi.fn()}
        selectedJobId={null}
        onSelectJob={vi.fn()}
      />
    );

    expect(screen.getByText('TOPIX 500')).toBeInTheDocument();
  });
});
