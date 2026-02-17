import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import type { OptimizationJobResponse } from '@/types/backtest';
import { OptimizationJobProgressCard } from './OptimizationJobProgressCard';

const baseJob: OptimizationJobResponse = {
  job_id: 'opt-job-1',
  status: 'completed',
  progress: 1,
  message: 'done',
  created_at: '2026-02-17T10:00:00Z',
  started_at: '2026-02-17T10:00:05Z',
  completed_at: '2026-02-17T10:01:00Z',
  error: null,
  best_score: 1.0,
  best_params: { period: 20, threshold: 0.4 },
  worst_score: 0.12,
  worst_params: { period: 5, threshold: 0.9 },
  total_combinations: 9,
  notebook_path: null,
};

function createJob(overrides: Partial<OptimizationJobResponse>): OptimizationJobResponse {
  return {
    ...baseJob,
    ...overrides,
  };
}

describe('OptimizationJobProgressCard', () => {
  it('renders nothing when no job and not loading', () => {
    const { container } = render(<OptimizationJobProgressCard job={null} />);
    expect(container.firstChild).toBeNull();
  });

  it('renders submitting state while loading without job', () => {
    render(<OptimizationJobProgressCard job={null} isLoading={true} />);
    expect(screen.getByText('Submitting...')).toBeInTheDocument();
  });

  it('renders running state with progress message and elapsed timer', () => {
    const job = createJob({
      status: 'running',
      message: 'trial 3/9',
      best_params: null,
      worst_params: null,
    });

    render(<OptimizationJobProgressCard job={job} />);

    expect(screen.getByText('Optimizing')).toBeInTheDocument();
    expect(screen.getByText('trial 3/9')).toBeInTheDocument();
    expect(screen.getByText(/⏱/)).toBeInTheDocument();
  });

  it('renders failed state error', () => {
    const job = createJob({
      status: 'failed',
      error: 'optimization failed',
      best_params: null,
      worst_params: null,
    });

    render(<OptimizationJobProgressCard job={job} />);

    expect(screen.getByText('Failed')).toBeInTheDocument();
    expect(screen.getByText('optimization failed')).toBeInTheDocument();
  });

  it('renders cancelled state label', () => {
    const job = createJob({
      status: 'cancelled',
      best_params: null,
      worst_params: null,
    });

    render(<OptimizationJobProgressCard job={job} />);
    expect(screen.getByText('Cancelled')).toBeInTheDocument();
  });

  it('handles unknown status defensively', () => {
    const job = createJob({
      status: 'unknown' as OptimizationJobResponse['status'],
      best_params: null,
      worst_params: null,
    });

    const { container } = render(<OptimizationJobProgressCard job={job} />);
    expect(container.firstChild).not.toBeNull();
  });

  it('renders best/worst params and scores for completed jobs', () => {
    render(<OptimizationJobProgressCard job={baseJob} />);

    expect(screen.getByText('Completed')).toBeInTheDocument();
    expect(screen.getByText('Combinations:')).toBeInTheDocument();
    expect(screen.getByText('9')).toBeInTheDocument();
    expect(screen.getByText('Best Params')).toBeInTheDocument();
    expect(screen.getByText('Worst Params')).toBeInTheDocument();
    expect(screen.getByText('score: 1.0000')).toBeInTheDocument();
    expect(screen.getByText('score: 0.1200')).toBeInTheDocument();
    expect(screen.getByText(/"period": 20/)).toBeInTheDocument();
    expect(screen.getByText(/"period": 5/)).toBeInTheDocument();
  });

  it('falls back to score-only summary when params are unavailable', () => {
    const job = createJob({
      best_params: null,
      worst_params: null,
      best_score: 0.82,
      worst_score: 0.11,
    });

    render(<OptimizationJobProgressCard job={job} />);

    expect(screen.getByText('Best Score:')).toBeInTheDocument();
    expect(screen.getByText('Worst Score:')).toBeInTheDocument();
    expect(screen.getByText('0.8200')).toBeInTheDocument();
    expect(screen.getByText('0.1100')).toBeInTheDocument();
  });

  it('shows score fallback for only the missing param side', () => {
    const job = createJob({
      best_params: { period: 20 },
      best_score: 0.9,
      worst_params: null,
      worst_score: 0.2,
    });

    render(<OptimizationJobProgressCard job={job} />);

    expect(screen.getByText('Best Params')).toBeInTheDocument();
    expect(screen.getByText('score: 0.9000')).toBeInTheDocument();
    expect(screen.getByText('Worst Score:')).toBeInTheDocument();
    expect(screen.getByText('0.2000')).toBeInTheDocument();
  });
});
