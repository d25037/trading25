import { useNavigate } from '@tanstack/react-router';
import { BarChart3, TrendingUp } from 'lucide-react';
import { useCallback } from 'react';
import {
  FundamentalRankingFilters,
  FundamentalRankingSummary,
  FundamentalRankingTable,
} from '@/components/FundamentalRanking';
import { IndexPerformanceTable, RankingFilters, RankingSummary, RankingTable } from '@/components/Ranking';
import { Button } from '@/components/ui/button';
import { useFundamentalRanking } from '@/hooks/useFundamentalRanking';
import { useRankingRouteState } from '@/hooks/usePageRouteState';
import { useRanking } from '@/hooks/useRanking';
import { cn } from '@/lib/utils';
import type { FundamentalRankingParams } from '@/types/fundamentalRanking';
import type { RankingPageTab, RankingParams } from '@/types/ranking';

const subTabs: { id: RankingPageTab; label: string; icon: typeof BarChart3 }[] = [
  { id: 'ranking', label: 'Daily Ranking', icon: BarChart3 },
  { id: 'fundamentalRanking', label: 'Fundamental Ranking', icon: TrendingUp },
];

interface RankingSidebarProps {
  activeSubTab: RankingPageTab;
  rankingParams: RankingParams;
  fundamentalRankingParams: FundamentalRankingParams;
  setRankingParams: (params: RankingParams) => void;
  setFundamentalRankingParams: (params: FundamentalRankingParams) => void;
}

function RankingSidebar({
  activeSubTab,
  rankingParams,
  fundamentalRankingParams,
  setRankingParams,
  setFundamentalRankingParams,
}: RankingSidebarProps) {
  if (activeSubTab === 'ranking') {
    return <RankingFilters params={rankingParams} onChange={setRankingParams} />;
  }

  return <FundamentalRankingFilters params={fundamentalRankingParams} onChange={setFundamentalRankingParams} />;
}

export function RankingPage() {
  const {
    activeSubTab,
    rankingParams,
    fundamentalRankingParams,
    setActiveSubTab,
    setRankingParams,
    setFundamentalRankingParams,
  } = useRankingRouteState();
  const navigate = useNavigate();
  const rankingQuery = useRanking(rankingParams, true);
  const fundamentalRankingQuery = useFundamentalRanking(fundamentalRankingParams, activeSubTab === 'fundamentalRanking');

  const handleStockClick = useCallback(
    (code: string) => {
      void navigate({
        to: '/charts',
        search: { symbol: code },
      });
    },
    [navigate]
  );
  const handleIndexClick = useCallback(
    (code: string) => {
      void navigate({
        to: '/indices',
        search: { code },
      });
    },
    [navigate]
  );

  return (
    <div className="flex h-full flex-col p-4">
      <div className="mb-4 flex gap-2">
        {subTabs.map((tab) => {
          const Icon = tab.icon;
          const isActive = activeSubTab === tab.id;
          return (
            <Button
              key={tab.id}
              variant={isActive ? 'default' : 'outline'}
              size="sm"
              className={cn('gap-2', isActive && 'shadow-md')}
              onClick={() => setActiveSubTab(tab.id)}
            >
              <Icon className="h-4 w-4" />
              {tab.label}
            </Button>
          );
        })}
      </div>

      <div className="flex min-h-0 flex-1 gap-4">
        <div className="w-64 flex-shrink-0">
          <RankingSidebar
            activeSubTab={activeSubTab}
            rankingParams={rankingParams}
            fundamentalRankingParams={fundamentalRankingParams}
            setRankingParams={setRankingParams}
            setFundamentalRankingParams={setFundamentalRankingParams}
          />
        </div>

        <div className="flex min-h-0 min-w-0 flex-1 flex-col gap-4">
          <IndexPerformanceTable
            items={rankingQuery.data?.indexPerformance}
            isLoading={rankingQuery.isLoading}
            error={rankingQuery.error}
            onIndexClick={handleIndexClick}
          />

          {activeSubTab === 'ranking' ? (
            <>
              <RankingSummary data={rankingQuery.data} />
              <RankingTable
                rankings={rankingQuery.data?.rankings}
                isLoading={rankingQuery.isLoading}
                error={rankingQuery.error}
                onStockClick={handleStockClick}
                periodDays={rankingParams.periodDays}
              />
            </>
          ) : (
            <>
              <FundamentalRankingSummary data={fundamentalRankingQuery.data} />
              <FundamentalRankingTable
                rankings={fundamentalRankingQuery.data?.rankings}
                isLoading={fundamentalRankingQuery.isLoading}
                error={fundamentalRankingQuery.error}
                onStockClick={handleStockClick}
              />
            </>
          )}
        </div>
      </div>
    </div>
  );
}
