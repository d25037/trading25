import { MarketsSelect, NumberSelect } from '@/components/shared/filters';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Label } from '@/components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
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

const EPS_FILTER_OPTIONS = [
  { value: 'all', label: 'All stocks' },
  {
    value: 'forecastAboveRecentFyActuals',
    label: 'Latest Forecast EPS > Recent FY Actual EPS',
  },
] as const;

const LOOKBACK_FY_COUNT_OPTIONS = [
  { value: 1, label: '1 FY' },
  { value: 2, label: '2 FY' },
  { value: 3, label: '3 FY' },
  { value: 5, label: '5 FY' },
  { value: 10, label: '10 FY' },
];

interface FundamentalRankingFiltersProps {
  params: FundamentalRankingParams;
  onChange: (params: FundamentalRankingParams) => void;
}

export function FundamentalRankingFilters({ params, onChange }: FundamentalRankingFiltersProps) {
  const updateParam = <K extends keyof FundamentalRankingParams>(key: K, value: FundamentalRankingParams[K]) => {
    onChange({ ...params, [key]: value });
  };
  const forecastFilterEnabled = params.forecastAboveRecentFyActuals ?? params.forecastAboveAllActuals ?? false;
  const epsFilterValue = forecastFilterEnabled ? 'forecastAboveRecentFyActuals' : 'all';
  const lookbackFyCount = params.forecastLookbackFyCount ?? 3;

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
        <div className="space-y-2">
          <Label htmlFor="fundamental-ranking-eps-condition" className="text-xs">
            EPS Condition
          </Label>
          <Select
            value={epsFilterValue}
            onValueChange={(value) => updateParam('forecastAboveRecentFyActuals', value === 'forecastAboveRecentFyActuals')}
          >
            <SelectTrigger id="fundamental-ranking-eps-condition" className="h-8 text-xs">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {EPS_FILTER_OPTIONS.map((option) => (
                <SelectItem key={option.value} value={option.value}>
                  {option.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
        <NumberSelect
          value={lookbackFyCount}
          onChange={(v) => updateParam('forecastLookbackFyCount', v)}
          options={LOOKBACK_FY_COUNT_OPTIONS}
          id="fundamental-ranking-lookback-fy-count"
          label="Recent FY lookback"
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
