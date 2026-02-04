import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';
import type { BacktestJobResponse } from '@/types/backtest';
import { JobsTable } from './JobsTable';

const baseJob = {
  progress: null,
  message: null,
  created_at: '2025-01-30T09:00:00Z',
  completed_at: null,
  error: null,
};

const mockJobs: BacktestJobResponse[] = [
  {
    ...baseJob,
    job_id: 'abcdef12-3456-7890-abcd-ef1234567890',
    status: 'completed',
    started_at: '2025-01-30T10:00:00Z',
    completed_at: '2025-01-30T10:05:00Z',
    result: { total_return: 0.15, sharpe_ratio: 1.2, max_drawdown: -0.08 },
  } as BacktestJobResponse,
  {
    ...baseJob,
    job_id: 'bbbbbbbb-1111-2222-3333-444444444444',
    status: 'running',
    started_at: '2025-01-30T11:00:00Z',
    result: null,
  },
  {
    ...baseJob,
    job_id: 'cccccccc-1111-2222-3333-444444444444',
    status: 'failed',
    started_at: null,
    result: null,
  },
  {
    ...baseJob,
    job_id: 'dddddddd-1111-2222-3333-444444444444',
    status: 'pending',
    started_at: null,
    result: null,
  },
];

describe('JobsTable', () => {
  it('renders loading state', () => {
    render(<JobsTable jobs={undefined} isLoading={true} onSelectJob={vi.fn()} />);
    // Loader2 spinner should be present (no text to check, just verify no error)
    expect(screen.queryByText('No jobs found')).not.toBeInTheDocument();
  });

  it('renders empty state when no jobs', () => {
    render(<JobsTable jobs={[]} isLoading={false} onSelectJob={vi.fn()} />);
    expect(screen.getByText('No jobs found')).toBeInTheDocument();
  });

  it('renders empty state when jobs is undefined', () => {
    render(<JobsTable jobs={undefined} isLoading={false} onSelectJob={vi.fn()} />);
    expect(screen.getByText('No jobs found')).toBeInTheDocument();
  });

  it('renders table headers', () => {
    render(<JobsTable jobs={mockJobs} isLoading={false} onSelectJob={vi.fn()} />);

    expect(screen.getByText('Status')).toBeInTheDocument();
    expect(screen.getByText('Job ID')).toBeInTheDocument();
    expect(screen.getByText('Started')).toBeInTheDocument();
    expect(screen.getByText('Return')).toBeInTheDocument();
    expect(screen.getByText('Actions')).toBeInTheDocument();
  });

  it('renders job rows with truncated IDs', () => {
    render(<JobsTable jobs={mockJobs} isLoading={false} onSelectJob={vi.fn()} />);

    expect(screen.getByText('abcdef12...')).toBeInTheDocument();
    expect(screen.getByText('bbbbbbbb...')).toBeInTheDocument();
  });

  it('renders View button only for completed jobs', () => {
    render(<JobsTable jobs={mockJobs} isLoading={false} onSelectJob={vi.fn()} />);

    const viewButtons = screen.getAllByText('View');
    expect(viewButtons).toHaveLength(1);
  });

  it('calls onSelectJob when View button is clicked', async () => {
    const user = userEvent.setup();
    const onSelectJob = vi.fn();

    render(<JobsTable jobs={mockJobs} isLoading={false} onSelectJob={onSelectJob} />);

    await user.click(screen.getByText('View'));
    expect(onSelectJob).toHaveBeenCalledWith('abcdef12-3456-7890-abcd-ef1234567890');
  });

  it('renders dash for jobs without start date', () => {
    render(<JobsTable jobs={mockJobs} isLoading={false} onSelectJob={vi.fn()} />);

    // Jobs without started_at should show '-'
    const dashes = screen.getAllByText('-');
    expect(dashes.length).toBeGreaterThan(0);
  });
});
