import { render, screen } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { ValueCompositeScoreStrip } from './ValueCompositeScoreStrip';

const mockUseValueCompositeScore = vi.fn();

vi.mock('@/hooks/useValueCompositeScore', () => ({
  useValueCompositeScore: (...args: unknown[]) => mockUseValueCompositeScore(...args),
}));

describe('ValueCompositeScoreStrip', () => {
  beforeEach(() => {
    mockUseValueCompositeScore.mockReset();
  });

  it('renders a market-specific value score when available', () => {
    mockUseValueCompositeScore.mockReturnValue({
      data: {
        scoreAvailable: true,
        scoreMethod: 'prime_size_tilt',
        universeCount: 1240,
        item: {
          rank: 34,
          score: 0.8234,
          lowPbrScore: 0.61,
          smallMarketCapScore: 0.92,
          lowForwardPerScore: 0.78,
        },
      },
    });

    render(<ValueCompositeScoreStrip symbol="7203" enabled />);

    expect(screen.getByText('Value Score')).toBeInTheDocument();
    expect(screen.getByText('82.3')).toBeInTheDocument();
    expect(screen.getByText('Rank 34 / 1,240')).toBeInTheDocument();
    expect(screen.getByText('Prime size tilt')).toBeInTheDocument();
  });

  it('shows the unavailable reason when a supported market is missing forward EPS', () => {
    mockUseValueCompositeScore.mockReturnValue({
      data: {
        scoreAvailable: false,
        unsupportedReason: 'forward_eps_missing',
        item: null,
      },
    });

    render(<ValueCompositeScoreStrip symbol="285A" enabled />);

    expect(screen.getByText('Value Score unavailable: forward EPS missing')).toBeInTheDocument();
  });

  it('shows the unavailable reason when a supported market is missing BPS', () => {
    mockUseValueCompositeScore.mockReturnValue({
      data: {
        scoreAvailable: false,
        unsupportedReason: 'bps_missing',
        item: null,
      },
    });

    render(<ValueCompositeScoreStrip symbol="6809" enabled />);

    expect(screen.getByText('Value Score unavailable: BPS missing')).toBeInTheDocument();
  });

  it('hides unsupported markets', () => {
    mockUseValueCompositeScore.mockReturnValue({
      data: {
        scoreAvailable: false,
        unsupportedReason: 'unsupported_market',
        item: null,
      },
    });

    const { container } = render(<ValueCompositeScoreStrip symbol="3999" enabled />);

    expect(container).toBeEmptyDOMElement();
  });
});
