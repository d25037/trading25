import { SectionEyebrow, Surface } from '@/components/Layout/Workspace';
import { DateInput, type MarketOption, MarketsSelect, NumberSelect } from '@/components/shared/filters';
import { Button } from '@/components/ui/button';
import { Label } from '@/components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { canonicalizeMarkets, formatMarketsLabel } from '@/lib/marketUtils';
import { cn } from '@/lib/utils';
import type { EntryDecidability, ScreeningParams, ScreeningSortBy, SortOrder } from '@/types/screening';

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

const AUTO_MARKETS_VALUE = '__auto__';
const BASE_MARKET_OPTIONS: MarketOption[] = [
  { value: 'prime', label: 'Prime' },
  { value: 'standard', label: 'Standard' },
  { value: 'growth', label: 'Growth' },
  { value: 'prime,standard', label: 'Prime + Standard' },
  { value: 'prime,standard,growth', label: 'All Markets' },
];

interface ScreeningFiltersProps {
  entryDecidability: EntryDecidability;
  params: ScreeningParams;
  onChange: (params: ScreeningParams) => void;
  strategyOptions: string[];
  autoMarkets: string[];
  autoScopeLabel: string;
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

function normalizeMarketsValue(value: string | undefined): string | undefined {
  if (!value) {
    return undefined;
  }

  const normalized = canonicalizeMarkets(value.split(','));
  return normalized.length > 0 ? normalized.join(',') : undefined;
}

function buildMarketsOptions(
  autoMarkets: string[],
  autoScopeLabel: string,
  explicitMarkets: string | undefined
): MarketOption[] {
  const options: MarketOption[] = [
    {
      value: AUTO_MARKETS_VALUE,
      label: autoScopeLabel === 'Auto' ? 'Auto' : `Auto (${autoScopeLabel})`,
    },
    ...BASE_MARKET_OPTIONS,
  ];
  const seen = new Set(options.map((option) => option.value));

  for (const candidate of [normalizeMarketsValue(autoMarkets.join(',')), normalizeMarketsValue(explicitMarkets)]) {
    if (!candidate || seen.has(candidate)) {
      continue;
    }
    options.push({
      value: candidate,
      label: formatMarketsLabel(candidate.split(',')),
    });
    seen.add(candidate);
  }

  return options;
}

export function ScreeningFilters({
  entryDecidability,
  params,
  onChange,
  strategyOptions,
  autoMarkets,
  autoScopeLabel,
  strategiesLoading = false,
}: ScreeningFiltersProps) {
  const updateParam = <K extends keyof ScreeningParams>(key: K, value: ScreeningParams[K]) => {
    onChange({ ...params, [key]: value });
  };

  const selectedStrategies = parseSelectedStrategies(params.strategies);
  const strategyGroupLabel =
    entryDecidability === 'requires_same_session_observation' ? 'in-session production' : 'pre-open production';
  const selectedMarketsValue = normalizeMarketsValue(params.markets);
  const marketOptions = buildMarketsOptions(autoMarkets, autoScopeLabel, selectedMarketsValue);

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
    <Surface className="p-4">
      <div className="space-y-1 pb-3">
        <SectionEyebrow>Filter Rail</SectionEyebrow>
        <h2 className="text-base font-semibold text-foreground">Filters</h2>
        <p className="text-xs text-muted-foreground">
          Choose universe, strategies, and execution window before each run.
        </p>
      </div>
      <div className="space-y-3">
        <MarketsSelect
          value={selectedMarketsValue ?? AUTO_MARKETS_VALUE}
          onChange={(value) => updateParam('markets', value === AUTO_MARKETS_VALUE ? undefined : value)}
          options={marketOptions}
          id="screening-markets"
          label="Universe"
        />

        <div className="space-y-2">
          <div className="flex items-center justify-between">
            <Label className="text-xs">Strategies</Label>
            <Button
              type="button"
              size="sm"
              variant="ghost"
              className="h-6 rounded-lg px-2 text-[11px] text-muted-foreground hover:bg-[var(--app-surface-muted)] hover:text-foreground"
              onClick={() => updateParam('strategies', undefined)}
            >
              All {strategyGroupLabel}
            </Button>
          </div>
          {strategiesLoading ? (
            <p className="text-xs text-muted-foreground">Loading {strategyGroupLabel} strategies...</p>
          ) : strategyOptions.length === 0 ? (
            <p className="text-xs text-muted-foreground">No {strategyGroupLabel} strategies available</p>
          ) : (
            <div className="flex flex-wrap gap-1">
              {strategyOptions.map((strategyName) => {
                const isSelected = selectedStrategies.includes(strategyName);
                return (
                  <Button
                    key={strategyName}
                    type="button"
                    size="sm"
                    variant="ghost"
                    aria-pressed={isSelected}
                    className={cn(
                      'h-6 rounded-lg border px-2 text-[10px]',
                      isSelected
                        ? 'border-border/70 bg-[var(--app-surface-emphasis)] text-foreground shadow-sm'
                        : 'border-border/70 bg-background/80 text-muted-foreground hover:bg-[var(--app-surface-muted)] hover:text-foreground'
                    )}
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
              ? `No explicit selection: all ${strategyGroupLabel} strategies are evaluated.`
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
      </div>
    </Surface>
  );
}
