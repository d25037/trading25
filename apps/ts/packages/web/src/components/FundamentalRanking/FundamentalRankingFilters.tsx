import { Surface } from '@/components/Layout/Workspace';
import { MarketsSelect, NumberSelect } from '@/components/shared/filters';
import { Label } from '@/components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import type { FundamentalRankingParams } from '@/types/fundamentalRanking';

const MARKET_OPTIONS = [
  { value: 'prime', label: 'Prime' },
  { value: 'standard', label: 'Standard' },
  { value: 'growth', label: 'Growth' },
  { value: 'prime,standard', label: 'Prime + Standard' },
];
const LIMIT_OPTIONS = [10, 20, 50, 100].map((value) => ({ value, label: String(value) }));
const LOOKBACK_OPTIONS = [1, 2, 3, 5, 10, 20].map((value) => ({ value, label: `${value} FY` }));

export function FundamentalRankingFilters({
  params,
  onChange,
}: {
  params: FundamentalRankingParams;
  onChange: (params: FundamentalRankingParams) => void;
}) {
  const updateParam = <K extends keyof FundamentalRankingParams>(key: K, value: FundamentalRankingParams[K]) => {
    onChange({ ...params, [key]: value });
  };
  const forecastFilterEnabled = params.forecastAboveRecentFyActuals ?? false;

  return (
    <Surface className="flex flex-wrap items-end gap-3 p-3">
      <MarketsSelect
        value={params.markets ?? 'prime'}
        onChange={(value) => updateParam('markets', value)}
        options={MARKET_OPTIONS}
        id="fundamental-ranking-markets"
      />
      <div className="space-y-1.5">
        <Label htmlFor="fundamental-ranking-eps-condition" className="text-xs">
          EPS Condition
        </Label>
        <Select
          value={forecastFilterEnabled ? 'forecastAboveRecentFyActuals' : 'all'}
          onValueChange={(value) => updateParam('forecastAboveRecentFyActuals', value !== 'all')}
        >
          <SelectTrigger id="fundamental-ranking-eps-condition" className="h-8 w-64 text-xs">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All stocks</SelectItem>
            <SelectItem value="forecastAboveRecentFyActuals">Latest Forecast EPS &gt; Recent FY Actual EPS</SelectItem>
          </SelectContent>
        </Select>
      </div>
      <NumberSelect
        value={params.forecastLookbackFyCount ?? 3}
        onChange={(value) => updateParam('forecastLookbackFyCount', value)}
        options={LOOKBACK_OPTIONS}
        id="fundamental-ranking-lookback-fy-count"
        label="Recent FY lookback"
        disabled={!forecastFilterEnabled}
      />
      <NumberSelect
        value={params.limit ?? 20}
        onChange={(value) => updateParam('limit', value)}
        options={LIMIT_OPTIONS}
        id="fundamental-ranking-limit"
        label="Results per ranking"
      />
    </Surface>
  );
}
