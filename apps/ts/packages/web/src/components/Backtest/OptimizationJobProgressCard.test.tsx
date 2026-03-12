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
  html_path: null,
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
      progress: 0.25,
      message: 'trial 3/9',
      best_params: null,
      worst_params: null,
    });

    render(<OptimizationJobProgressCard job={job} />);

    expect(screen.getByText('Optimizing')).toBeInTheDocument();
    expect(screen.getByText('Fast stage')).toBeInTheDocument();
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

  it('renders verification summary for completed jobs', () => {
    const job = createJob({
      fast_candidates: [
        {
          candidate_id: 'grid_0001',
          rank: 1,
          score: 1,
          metrics: {
            total_return: 10,
            sharpe_ratio: 1.2,
            max_drawdown: -5,
            trade_count: 8,
          },
        },
      ],
      verification: {
        overall_status: 'completed',
        requested_top_k: 1,
        completed_count: 1,
        mismatch_count: 0,
        winner_changed: false,
        authoritative_candidate_id: 'grid_0001',
        candidates: [
          {
            candidate_id: 'grid_0001',
            fast_rank: 1,
            fast_score: 1,
            verification_status: 'verified',
            fast_metrics: {
              total_return: 10,
              sharpe_ratio: 1.2,
              max_drawdown: -5,
              trade_count: 8,
            },
            verified_metrics: {
              total_return: 9.5,
              sharpe_ratio: 1.1,
              max_drawdown: -4.8,
              trade_count: 8,
            },
            delta: {
              total_return_delta: -0.5,
              sharpe_ratio_delta: -0.1,
              max_drawdown_delta: 0.2,
              trade_count_delta: 0,
            },
            mismatch_reasons: [],
          },
        ],
      },
    });

    render(<OptimizationJobProgressCard job={job} />);

    expect(screen.getByText('Verification')).toBeInTheDocument();
    expect(screen.getByText('Authoritative')).toBeInTheDocument();
    expect(screen.getAllByText('grid_0001').length).toBeGreaterThan(0);
  });
});
