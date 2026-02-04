import { BarChart3, Filter } from 'lucide-react';
import { useCallback, useState } from 'react';
import { RankingFilters, RankingSummary, RankingTable } from '@/components/Ranking';
import { ScreeningFilters } from '@/components/Screening/ScreeningFilters';
import { ScreeningSummary } from '@/components/Screening/ScreeningSummary';
import { ScreeningTable } from '@/components/Screening/ScreeningTable';
import { Button } from '@/components/ui/button';
import { useRanking } from '@/hooks/useRanking';
import { useScreening } from '@/hooks/useScreening';
import { cn } from '@/lib/utils';
import { useChartStore } from '@/stores/chartStore';
import { useUiStore } from '@/stores/uiStore';
import type { RankingParams } from '@/types/ranking';
import type { ScreeningParams } from '@/types/screening';

type AnalysisSubTab = 'screening' | 'ranking';

const DEFAULT_SCREENING_PARAMS: ScreeningParams = {
  markets: 'prime',
  rangeBreakFast: true,
  rangeBreakSlow: true,
  recentDays: 10,
  sortBy: 'date',
  order: 'desc',
  limit: 50,
};

const DEFAULT_RANKING_PARAMS: RankingParams = {
  markets: 'prime',
  limit: 20,
  lookbackDays: 1,
  periodDays: 250,
};

const subTabs: { id: AnalysisSubTab; label: string; icon: typeof BarChart3 }[] = [
  { id: 'screening', label: 'Screening', icon: Filter },
  { id: 'ranking', label: 'Ranking', icon: BarChart3 },
];

export function AnalysisPage() {
  const [activeSubTab, setActiveSubTab] = useState<AnalysisSubTab>('screening');
  const [screeningParams, setScreeningParams] = useState<ScreeningParams>(DEFAULT_SCREENING_PARAMS);
  const [rankingParams, setRankingParams] = useState<RankingParams>(DEFAULT_RANKING_PARAMS);

  const { setSelectedSymbol } = useChartStore();
  const { setActiveTab } = useUiStore();

  // Fetch data based on active sub-tab
  const screeningQuery = useScreening(screeningParams, activeSubTab === 'screening');
  const rankingQuery = useRanking(rankingParams, activeSubTab === 'ranking');

  const handleStockClick = useCallback(
    (code: string) => {
      setSelectedSymbol(code);
      setActiveTab('charts');
    },
    [setSelectedSymbol, setActiveTab]
  );

  return (
    <div className="flex h-full flex-col p-4">
      {/* Sub-tab navigation */}
      <div className="flex gap-2 mb-4">
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

      {/* Content area */}
      <div className="flex flex-1 gap-4 min-h-0">
        {/* Sidebar */}
        <div className="w-64 flex-shrink-0">
          {activeSubTab === 'screening' ? (
            <ScreeningFilters params={screeningParams} onChange={setScreeningParams} />
          ) : (
            <RankingFilters params={rankingParams} onChange={setRankingParams} />
          )}
        </div>

        {/* Main Content */}
        <div className="flex-1 flex flex-col min-w-0">
          {activeSubTab === 'screening' ? (
            <>
              <ScreeningSummary
                summary={screeningQuery.data?.summary}
                markets={screeningQuery.data?.markets || []}
                recentDays={screeningQuery.data?.recentDays || 0}
                referenceDate={screeningQuery.data?.referenceDate}
              />
              <ScreeningTable
                results={screeningQuery.data?.results || []}
                isLoading={screeningQuery.isLoading}
                error={screeningQuery.error}
                onStockClick={handleStockClick}
              />
            </>
          ) : (
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
          )}
        </div>
      </div>
    </div>
  );
}
