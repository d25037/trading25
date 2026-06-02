import { render, screen } from '@testing-library/react';
import type { ReactNode } from 'react';
import { describe, expect, it, vi } from 'vitest';
import type { MarketBubbleFootprintLatest } from '@/types/marketBubbleFootprint';
import { BubbleFootprintBanner } from './BubbleFootprintBanner';

vi.mock('@tanstack/react-router', () => ({
  Link: ({
    children,
    search,
    to,
    ...props
  }: {
    children: ReactNode;
    search?: { experimentId?: string };
    to: string;
  }) => (
    <a href={`${to}?experimentId=${search?.experimentId ?? ''}`} {...props}>
      {children}
    </a>
  ),
}));

const data: MarketBubbleFootprintLatest = {
  date: '2026-05-29',
  markets: ['prime', 'standard', 'growth'],
  overallRegime: 'blowoff_watch',
  overallScore: 4,
  nearBlowoff: true,
  researchExperimentId: 'market-behavior/market-bubble-footprint',
  reratingExperimentId: 'market-behavior/rerating-bubble-regime-forward-response',
  horizons: [
    {
      horizon: 20,
      score: 4,
      regime: 'blowoff_watch',
      nearBlowoff: false,
      breadthUpPct: 42.88,
      intensityLabel: 'Blowoff watch',
      activeFlags: [],
    },
    {
      horizon: 60,
      score: 3,
      regime: 'crowded',
      nearBlowoff: true,
      breadthUpPct: 24.77,
      intensityLabel: 'Near blowoff',
      activeFlags: [],
    },
    {
      horizon: 120,
      score: 4,
      regime: 'blowoff_watch',
      nearBlowoff: false,
      breadthUpPct: 48.86,
      intensityLabel: 'Blowoff watch',
      activeFlags: [],
    },
    {
      horizon: 252,
      score: 4,
      regime: 'blowoff_watch',
      nearBlowoff: false,
      breadthUpPct: 66.88,
      intensityLabel: 'Blowoff watch',
      activeFlags: [],
    },
  ],
};

describe('BubbleFootprintBanner', () => {
  it('shows horizon scores and near blowoff context', () => {
    render(<BubbleFootprintBanner data={data} />);

    expect(screen.getByText('Market Regime')).toBeInTheDocument();
    expect(screen.getByLabelText('Regime marker: Blowoff watch')).toBeInTheDocument();
    expect(screen.getAllByText('Blowoff watch').length).toBeGreaterThan(0);
    expect(screen.getByText(/score 3/)).toBeInTheDocument();
    expect(screen.getByText('breadth 25%')).toBeInTheDocument();
    expect(screen.getByTitle(/60D Near blowoff/)).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Footprint' }).getAttribute('href')).toContain('/research/detail');
  });

  it('uses the regime label for the status marker instead of a fixed warning icon', () => {
    render(<BubbleFootprintBanner data={{ ...data, overallRegime: 'normal', overallScore: 1, nearBlowoff: false }} />);

    expect(screen.getByLabelText('Regime marker: Normal')).toBeInTheDocument();
    expect(screen.getByText('Normal')).toBeInTheDocument();
  });
});
