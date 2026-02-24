import { MarketsSelect, NumberSelect } from '@/components/shared/filters';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import type { FundamentalRankingParams } from '@/types/fundamentalRanking';

const FUNDAMENTAL_RANKING_MARKET_OPTIONS = [
  { value: 'prime', label: 'Prime' },
  { value: 'standard', label: 'Standard' },
  { value: 'prime,standard', label: 'Prime + Standard' },
];

const LIMIT_OPTIONS = [
  { value: 10, label: '10' },
  { value: 20, label: '20' },
  { value: 50, label: '50' },
  { value: 100, label: '100' },
];

interface FundamentalRankingFiltersProps {
  params: FundamentalRankingParams;
  onChange: (params: FundamentalRankingParams) => void;
}

export function FundamentalRankingFilters({ params, onChange }: FundamentalRankingFiltersProps) {
  const updateParam = <K extends keyof FundamentalRankingParams>(key: K, value: FundamentalRankingParams[K]) => {
    onChange({ ...params, [key]: value });
  };

  return (
    <Card className="glass-panel">
      <CardHeader className="pb-3">
        <CardTitle className="text-base">Fundamental Ranking Filters</CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <MarketsSelect
          value={params.markets || 'prime'}
          onChange={(v) => updateParam('markets', v)}
          options={FUNDAMENTAL_RANKING_MARKET_OPTIONS}
          id="fundamental-ranking-markets"
        />
        <NumberSelect
          value={params.limit || 20}
          onChange={(v) => updateParam('limit', v)}
          options={LIMIT_OPTIONS}
          id="fundamental-ranking-limit"
          label="Results per ranking"
        />
      </CardContent>
    </Card>
  );
}
