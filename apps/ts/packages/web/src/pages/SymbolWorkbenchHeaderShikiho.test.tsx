import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, test, vi } from 'vitest';
import { useChartStore } from '@/stores/chartStore';
import { ChartHeader } from './SymbolWorkbenchHeader';

vi.mock('@/components/Ranking/DailyRankingSnapshot', () => ({
  DailyRankingSnapshot: () => <div data-testid="daily-ranking-snapshot">Daily Ranking</div>,
}));

vi.mock('@/components/SymbolWorkbench/ShikihoPanel', () => ({
  ShikihoPanel: ({ onSelectSymbol }: { onSelectSymbol: (symbol: string) => void }) => (
    <button type="button" data-testid="shikiho-panel" onClick={() => onSelectSymbol('7201')}>
      会社四季報
    </button>
  ),
}));

vi.mock('@/hooks/useWatchlist', () => ({
  useWatchlists: () => ({ data: { watchlists: [] }, isLoading: false, error: null }),
  useAddWatchlistItem: () => ({ mutate: vi.fn(), isPending: false, error: null }),
}));

describe('ChartHeader Shikiho integration', () => {
  test('renders Shikiho immediately after Daily Ranking and forwards comparison selection', async () => {
    const onSelectSymbol = vi.fn();
    render(
      <ChartHeader
        settings={useChartStore.getState().settings}
        selectedSymbol="7203"
        stockInfo={undefined}
        latestMarketCaps={{ freeFloat: null, issuedShares: null }}
        rankingSnapshot={undefined}
        rankingSnapshotLoading={false}
        rankingSnapshotError={null}
        onRetryRankingSnapshot={vi.fn()}
        shikihoSnapshot={null}
        shikihoDiagnostic={null}
        shikihoCaptureState="not_captured"
        isShikihoRefreshing={false}
        onRefreshShikiho={vi.fn()}
        onSelectSymbol={onSelectSymbol}
        strategyName={null}
        matchedDate={null}
        signalProvenance={null}
        signalDiagnostics={null}
        fundamentalsProvenance={null}
        refreshFeedback={null}
        isRefreshing={false}
        onRefresh={vi.fn()}
        onOpenMobileSettings={vi.fn()}
      />
    );

    const ranking = screen.getByTestId('daily-ranking-snapshot');
    const shikiho = screen.getByTestId('shikiho-panel');
    expect(shikiho).toHaveTextContent('会社四季報');
    expect(ranking.compareDocumentPosition(shikiho) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();

    await userEvent.click(shikiho);
    expect(onSelectSymbol).toHaveBeenCalledWith('7201');
  });
});
