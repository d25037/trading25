import { DateInput, MarketsSelect, NumberSelect } from '@/components/shared/filters';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import type { RankingParams } from '@/types/ranking';

const RANKING_MARKET_OPTIONS = [
  { value: 'prime', label: 'Prime' },
  { value: 'standard', label: 'Standard' },
  { value: 'prime,standard', label: 'Prime + Standard' },
];

const LOOKBACK_OPTIONS = [
  { value: 1, label: '1 day' },
  { value: 5, label: '5 days' },
  { value: 10, label: '10 days' },
  { value: 20, label: '20 days' },
];

const LIMIT_OPTIONS = [
  { value: 10, label: '10' },
  { value: 20, label: '20' },
  { value: 50, label: '50' },
  { value: 100, label: '100' },
];

const PERIOD_OPTIONS = [
  { value: 60, label: '60 days' },
  { value: 120, label: '120 days' },
  { value: 250, label: '250 days (1Y)' },
];

interface RankingFiltersProps {
  params: RankingParams;
  onChange: (params: RankingParams) => void;
}

export function RankingFilters({ params, onChange }: RankingFiltersProps) {
  const updateParam = <K extends keyof RankingParams>(key: K, value: RankingParams[K]) => {
    onChange({ ...params, [key]: value });
  };

  return (
    <Card className="glass-panel">
      <CardHeader className="pb-3">
        <CardTitle className="text-base">Ranking Filters</CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <MarketsSelect
          value={params.markets || 'prime'}
          onChange={(v) => updateParam('markets', v)}
          options={RANKING_MARKET_OPTIONS}
          id="ranking-markets"
        />
        <NumberSelect
          value={params.lookbackDays || 1}
          onChange={(v) => updateParam('lookbackDays', v)}
          options={LOOKBACK_OPTIONS}
          id="ranking-lookbackDays"
          label="Lookback Days"
        />
        <NumberSelect
          value={params.limit || 20}
          onChange={(v) => updateParam('limit', v)}
          options={LIMIT_OPTIONS}
          id="ranking-limit"
          label="Results per ranking"
        />
        <NumberSelect
          value={params.periodDays || 250}
          onChange={(v) => updateParam('periodDays', v)}
          options={PERIOD_OPTIONS}
          id="ranking-periodDays"
          label="Period Days (High/Low)"
        />
        <DateInput value={params.date} onChange={(v) => updateParam('date', v)} id="ranking-date" />
      </CardContent>
    </Card>
  );
}
