import { DateInput, MarketsSelect, NumberSelect } from '@/components/shared/filters';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Label } from '@/components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import type { ScreeningParams, ScreeningSortBy, SortOrder } from '@/types/screening';

const RECENT_DAYS_OPTIONS = [
  { value: 5, label: '5 days' },
  { value: 10, label: '10 days' },
  { value: 20, label: '20 days' },
  { value: 30, label: '30 days' },
  { value: 60, label: '60 days' },
];

const LIMIT_OPTIONS = [
  { value: 25, label: '25' },
  { value: 50, label: '50' },
  { value: 100, label: '100' },
  { value: 200, label: '200' },
];

interface ScreeningFiltersProps {
  params: ScreeningParams;
  onChange: (params: ScreeningParams) => void;
  strategyOptions: string[];
  strategiesLoading?: boolean;
}

function parseSelectedStrategies(strategies: string | undefined): string[] {
  if (!strategies) {
    return [];
  }
  return strategies
    .split(',')
    .map((value) => value.trim())
    .filter(Boolean);
}

function stringifyStrategies(strategies: string[]): string | undefined {
  if (strategies.length === 0) {
    return undefined;
  }
  return strategies.join(',');
}

export function ScreeningFilters({
  params,
  onChange,
  strategyOptions,
  strategiesLoading = false,
}: ScreeningFiltersProps) {
  const updateParam = <K extends keyof ScreeningParams>(key: K, value: ScreeningParams[K]) => {
    onChange({ ...params, [key]: value });
  };

  const selectedStrategies = parseSelectedStrategies(params.strategies);

  const toggleStrategy = (strategyName: string) => {
    const selected = new Set(selectedStrategies);
    if (selected.has(strategyName)) {
      selected.delete(strategyName);
    } else {
      selected.add(strategyName);
    }
    updateParam('strategies', stringifyStrategies([...selected]));
  };

  return (
    <Card className="glass-panel">
      <CardHeader className="pb-3">
        <CardTitle className="text-base">Filters</CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <MarketsSelect
          value={params.markets || 'prime'}
          onChange={(v) => updateParam('markets', v)}
          id="screening-markets"
        />

        <div className="space-y-2">
          <div className="flex items-center justify-between">
            <Label className="text-xs">Strategies</Label>
            <Button
              type="button"
              size="sm"
              variant="ghost"
              className="h-6 px-2 text-[11px]"
              onClick={() => updateParam('strategies', undefined)}
            >
              All production
            </Button>
          </div>
          {strategiesLoading ? (
            <p className="text-xs text-muted-foreground">Loading production strategies...</p>
          ) : strategyOptions.length === 0 ? (
            <p className="text-xs text-muted-foreground">No production strategies available</p>
          ) : (
            <div className="flex flex-wrap gap-1.5">
              {strategyOptions.map((strategyName) => {
                const isSelected = selectedStrategies.includes(strategyName);
                return (
                  <Button
                    key={strategyName}
                    type="button"
                    size="sm"
                    variant={isSelected ? 'default' : 'outline'}
                    className="h-7 px-2 text-[11px]"
                    onClick={() => toggleStrategy(strategyName)}
                  >
                    {strategyName}
                  </Button>
                );
              })}
            </div>
          )}
          <p className="text-[11px] text-muted-foreground">
            {selectedStrategies.length === 0
              ? 'No explicit selection: all production strategies are evaluated.'
              : `${selectedStrategies.length} strategies selected`}
          </p>
        </div>

        <NumberSelect
          value={params.recentDays || 10}
          onChange={(v) => updateParam('recentDays', v)}
          options={RECENT_DAYS_OPTIONS}
          id="recentDays"
          label="Recent Days"
        />

        <DateInput
          value={params.date}
          onChange={(v) => updateParam('date', v)}
          id="screening-date"
          label="Reference Date (optional)"
        />

        <div className="space-y-2">
          <Label htmlFor="sortBy" className="text-xs">
            Sort By
          </Label>
          <Select
            value={params.sortBy || 'matchedDate'}
            onValueChange={(v) => updateParam('sortBy', v as ScreeningSortBy)}
          >
            <SelectTrigger id="sortBy" className="h-8 text-xs">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="matchedDate">Matched Date</SelectItem>
              <SelectItem value="stockCode">Stock Code</SelectItem>
              <SelectItem value="matchStrategyCount">Match Strategy Count</SelectItem>
            </SelectContent>
          </Select>
        </div>

        <div className="space-y-2">
          <Label htmlFor="order" className="text-xs">
            Order
          </Label>
          <Select value={params.order || 'desc'} onValueChange={(v) => updateParam('order', v as SortOrder)}>
            <SelectTrigger id="order" className="h-8 text-xs">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="desc">Descending</SelectItem>
              <SelectItem value="asc">Ascending</SelectItem>
            </SelectContent>
          </Select>
        </div>

        <NumberSelect
          value={params.limit || 50}
          onChange={(v) => updateParam('limit', v)}
          options={LIMIT_OPTIONS}
          id="screening-limit"
          label="Limit"
        />
      </CardContent>
    </Card>
  );
}
