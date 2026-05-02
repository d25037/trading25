import { render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import type { ValueCompositeRankingResponse } from '@/types/valueCompositeRanking';
import { ValueCompositeRankingTable } from './ValueCompositeRankingTable';

function buildResponse(
  markets: string[],
  technicalMetrics: NonNullable<ValueCompositeRankingResponse['items'][number]['technicalMetrics']>
): ValueCompositeRankingResponse {
  return {
    date: '2026-04-24',
    markets,
    metricKey: 'standard_value_composite',
    scoreMethod: markets.includes('prime') ? 'prime_size_tilt' : 'standard_pbr_tilt',
    forwardEpsMode: 'latest',
    scorePolicy: 'test',
    weights: {},
    itemCount: 1,
    items: [
      {
        rank: 1,
        code: '99840',
        companyName: 'Test Co',
        marketCode: markets[0] ?? 'standard',
        sector33Name: 'Info',
        currentPrice: 520,
        volume: 100000,
        score: 0.91,
        lowPbrScore: 0.9,
        smallMarketCapScore: 0.8,
        lowForwardPerScore: 0.7,
        pbr: 0.52,
        forwardPer: 5,
        marketCapBilJpy: 5.2,
        technicalMetrics,
      },
    ],
    lastUpdated: '2026-04-24T00:00:00Z',
  };
}

describe('ValueCompositeRankingTable', () => {
  it('shows standard raw technical metrics', () => {
    render(
      <ValueCompositeRankingTable
        data={buildResponse(['standard'], {
          featureDate: '2026-04-24',
          reboundFrom252dLowPct: 62.7,
          return252dPct: 35.1,
        })}
        isLoading={false}
        error={null}
        onStockClick={vi.fn()}
      />
    );

    expect(screen.getByText('252d Low Reb')).toBeInTheDocument();
    expect(screen.getByText('252d Ret')).toBeInTheDocument();
    expect(screen.getByText('+62.7%')).toBeInTheDocument();
    expect(screen.getByText('+35.1%')).toBeInTheDocument();
    expect(screen.queryByText('Vol 60d')).not.toBeInTheDocument();
  });

  it('shows prime raw volatility metrics', () => {
    render(
      <ValueCompositeRankingTable
        data={buildResponse(['prime'], {
          featureDate: '2026-04-24',
          volatility20dPct: 38.8,
          volatility60dPct: 41.3,
          downsideVolatility60dPct: 25.9,
        })}
        isLoading={false}
        error={null}
        onStockClick={vi.fn()}
      />
    );

    expect(screen.getByText('Vol 20d')).toBeInTheDocument();
    expect(screen.getByText('Vol 60d')).toBeInTheDocument();
    expect(screen.getByText('Down Vol 60d')).toBeInTheDocument();
    expect(screen.getByText('38.8%')).toBeInTheDocument();
    expect(screen.getByText('41.3%')).toBeInTheDocument();
    expect(screen.getByText('25.9%')).toBeInTheDocument();
    expect(screen.queryByText('252d Low Reb')).not.toBeInTheDocument();
  });
});
