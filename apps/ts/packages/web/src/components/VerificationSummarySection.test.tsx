import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import { VerificationSummarySection } from './VerificationSummarySection';

describe('VerificationSummarySection', () => {
  it('renders fast ranking and verification comparison', () => {
    render(
      <VerificationSummarySection
        fastCandidates={[
          {
            candidate_id: 'grid_0001',
            rank: 1,
            score: 1.2345,
            metrics: {
              total_return: 12.3,
              sharpe_ratio: 1.4,
              max_drawdown: -4.5,
              trade_count: 7,
            },
          },
        ]}
        verification={{
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
              fast_score: 1.2345,
              verification_status: 'verified',
              fast_metrics: {
                total_return: 12.3,
                sharpe_ratio: 1.4,
                max_drawdown: -4.5,
                trade_count: 7,
              },
              verified_metrics: {
                total_return: 11.9,
                sharpe_ratio: 1.31,
                max_drawdown: -4.4,
                trade_count: 7,
              },
              delta: {
                total_return_delta: -0.4,
                sharpe_ratio_delta: -0.09,
                max_drawdown_delta: 0.1,
                trade_count_delta: 0,
              },
              mismatch_reasons: [],
            },
          ],
        }}
      />
    );

    expect(screen.getByText('Fast Ranking')).toBeInTheDocument();
    expect(screen.getByText('Verification')).toBeInTheDocument();
    expect(screen.getAllByText('grid_0001').length).toBeGreaterThan(0);
    expect(screen.getByText('verified')).toBeInTheDocument();
    expect(screen.getByText('Authoritative')).toBeInTheDocument();
  });
});
