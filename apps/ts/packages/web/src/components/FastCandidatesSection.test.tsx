import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import { FastCandidatesSection } from './FastCandidatesSection';

describe('FastCandidatesSection', () => {
  it('renders fast-ranking candidates without verification details', () => {
    render(
      <FastCandidatesSection
        fastCandidates={[
          {
            candidate_id: 'grid_0001',
            rank: 1,
            score: 1.2345,
            metrics: {
              total_return: 12.5,
              sharpe_ratio: 1.4,
              max_drawdown: -5.0,
              trade_count: 18,
            },
          },
        ]}
      />,
    );

    expect(screen.getByText('Fast Ranking')).toBeInTheDocument();
    expect(screen.getByText('grid_0001')).toBeInTheDocument();
    expect(screen.queryByText('Verification')).not.toBeInTheDocument();
  });
});
