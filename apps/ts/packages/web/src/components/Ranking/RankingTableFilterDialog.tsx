import type { RankingItem, WatchlistSummaryResponse } from '@trading25/contracts/types/api-response-types';
import { SlidersHorizontal, X } from 'lucide-react';
import { useEffect, useState } from 'react';
import { StockSearchInput } from '@/components/Stock/StockSearchInput';
import { Button } from '@/components/ui/button';
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from '@/components/ui/dialog';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { cn } from '@/lib/utils';
import type { DailyRankingTableFilters, DailyRankingValuationSignalFilter } from '@/types/ranking';
import {
  RANKING_REGIME_STATE_OPTIONS,
  RANKING_RISK_STATE_OPTIONS,
  RANKING_TECHNICAL_STATE_OPTIONS,
} from './rankingState';
import { countActiveDailyRankingTableFilters } from './rankingTableFilters';

interface RankingTableFilterDialogProps {
  items: RankingItem[];
  filters: DailyRankingTableFilters;
  watchlists?: WatchlistSummaryResponse[];
  watchlistsLoading?: boolean;
  watchlistsError?: Error | null;
  onChange: (filters: DailyRankingTableFilters) => void;
}

type NumberFilterKey = keyof Pick<
  DailyRankingTableFilters,
  | 'minChangePct'
  | 'maxChangePct'
  | 'minTradingValue'
  | 'maxTradingValue'
  | 'minMarketCap'
  | 'maxMarketCap'
  | 'minSma5AboveCount5d'
  | 'maxSma5AboveCount5d'
  | 'minPer'
  | 'maxPer'
  | 'minForwardPer'
  | 'maxForwardPer'
  | 'minPsr'
  | 'maxPsr'
  | 'minForwardPsr'
  | 'maxForwardPsr'
  | 'minPbr'
  | 'maxPbr'
  | 'minLiquidityZ'
  | 'maxLiquidityZ'
  | 'minSectorScore'
  | 'maxSectorScore'
>;

type FilterKey = keyof DailyRankingTableFilters;

interface ActiveFilterDescriptor {
  id: string;
  label: string;
  keys: FilterKey[];
}

const ALL_VALUE = '__all__';

const VALUATION_SIGNAL_OPTIONS = [
  { value: ALL_VALUE, label: 'All Signals' },
  { value: 'deep_value', label: 'Deep Value' },
  { value: 'undervalued', label: 'Undervalued' },
  { value: 'overvalued', label: 'Overvalued' },
  { value: 'very_overvalued', label: 'Very Overvalued' },
  { value: 'no_earnings', label: 'No Earnings' },
] as const satisfies readonly { value: DailyRankingValuationSignalFilter | typeof ALL_VALUE; label: string }[];

const NUMERIC_GROUPS = [
  { label: 'Change %', minKey: 'minChangePct', maxKey: 'maxChangePct' },
  { label: 'Trading Value', minKey: 'minTradingValue', maxKey: 'maxTradingValue' },
  { label: 'Market Cap', minKey: 'minMarketCap', maxKey: 'maxMarketCap' },
  { label: 'SMA5', minKey: 'minSma5AboveCount5d', maxKey: 'maxSma5AboveCount5d' },
  { label: 'PER', minKey: 'minPer', maxKey: 'maxPer' },
  { label: 'Fwd PER', minKey: 'minForwardPer', maxKey: 'maxForwardPer' },
  { label: 'PSR', minKey: 'minPsr', maxKey: 'maxPsr' },
  { label: 'Fwd PSR', minKey: 'minForwardPsr', maxKey: 'maxForwardPsr' },
  { label: 'PBR', minKey: 'minPbr', maxKey: 'maxPbr' },
  { label: 'Liquidity Z', minKey: 'minLiquidityZ', maxKey: 'maxLiquidityZ' },
  { label: 'Sector Strength', minKey: 'minSectorScore', maxKey: 'maxSectorScore' },
] as const satisfies readonly { label: string; minKey: NumberFilterKey; maxKey: NumberFilterKey }[];

const ACTIVE_CONTROL_CLASS = 'border-primary/70 bg-primary/5 shadow-sm shadow-primary/5 focus-visible:ring-primary/30';
const ACTIVE_LABEL_CLASS = 'text-primary';

function uniqueOptions(values: string[]): { value: string; label: string }[] {
  return [...new Set(values.filter(Boolean))]
    .sort((a, b) => a.localeCompare(b))
    .map((value) => ({ value, label: value }));
}

function parseNumberInput(value: string): number | undefined {
  if (value.trim() === '') return undefined;
  const parsed = Number.parseFloat(value);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function formatNumberInput(value: number | undefined): string {
  return typeof value === 'number' && Number.isFinite(value) ? String(value) : '';
}

function hasTextValue(value: string | undefined): boolean {
  return Boolean(value?.trim());
}

function hasNumberValue(value: number | undefined): boolean {
  return typeof value === 'number' && Number.isFinite(value);
}

function findOptionLabel(options: readonly { value: string; label: string }[], value: string | undefined): string {
  if (!value) return '';
  return options.find((option) => option.value === value)?.label ?? value;
}

function findWatchlistLabel(watchlists: readonly WatchlistSummaryResponse[], watchlistId: number | undefined): string {
  if (typeof watchlistId !== 'number') return '';
  return watchlists.find((watchlist) => watchlist.id === watchlistId)?.name ?? `#${watchlistId}`;
}

function buildActiveFilterDescriptors(
  filters: DailyRankingTableFilters,
  marketOptions: readonly { value: string; label: string }[],
  sectorOptions: readonly { value: string; label: string }[],
  watchlists: readonly WatchlistSummaryResponse[]
): ActiveFilterDescriptor[] {
  const descriptors: ActiveFilterDescriptor[] = [];
  if (hasTextValue(filters.text)) {
    descriptors.push({ id: 'text', label: `Search: ${filters.text?.trim()}`, keys: ['text'] });
  }
  if (hasTextValue(filters.market)) {
    descriptors.push({
      id: 'market',
      label: `Market: ${findOptionLabel(marketOptions, filters.market)}`,
      keys: ['market'],
    });
  }
  if (hasTextValue(filters.sector33Name)) {
    descriptors.push({
      id: 'sector33Name',
      label: `Sector: ${findOptionLabel(sectorOptions, filters.sector33Name)}`,
      keys: ['sector33Name'],
    });
  }
  if (typeof filters.watchlistId === 'number' && filters.watchlistId > 0) {
    descriptors.push({
      id: 'watchlistId',
      label: `Watchlist: ${findWatchlistLabel(watchlists, filters.watchlistId)}`,
      keys: ['watchlistId'],
    });
  }
  if (filters.regimeState) {
    descriptors.push({
      id: 'regimeState',
      label: `Regime: ${findOptionLabel(RANKING_REGIME_STATE_OPTIONS, filters.regimeState)}`,
      keys: ['regimeState'],
    });
  }
  if (filters.valuationSignal) {
    descriptors.push({
      id: 'valuationSignal',
      label: `Signal: ${findOptionLabel(VALUATION_SIGNAL_OPTIONS, filters.valuationSignal)}`,
      keys: ['valuationSignal'],
    });
  }
  if (filters.riskState) {
    descriptors.push({
      id: 'riskState',
      label: `Warning: ${findOptionLabel(RANKING_RISK_STATE_OPTIONS, filters.riskState)}`,
      keys: ['riskState'],
    });
  }
  if (filters.technicalState) {
    descriptors.push({
      id: 'technicalState',
      label: `Confirmation: ${findOptionLabel(RANKING_TECHNICAL_STATE_OPTIONS, filters.technicalState)}`,
      keys: ['technicalState'],
    });
  }
  for (const group of NUMERIC_GROUPS) {
    const minValue = filters[group.minKey];
    const maxValue = filters[group.maxKey];
    if (hasNumberValue(minValue)) {
      descriptors.push({
        id: group.minKey,
        label: `${group.label} >= ${formatNumberInput(minValue)}`,
        keys: [group.minKey],
      });
    }
    if (hasNumberValue(maxValue)) {
      descriptors.push({
        id: group.maxKey,
        label: `${group.label} <= ${formatNumberInput(maxValue)}`,
        keys: [group.maxKey],
      });
    }
  }
  return descriptors;
}

export function RankingTableFilterDialog({
  items,
  filters,
  watchlists = [],
  watchlistsLoading = false,
  watchlistsError = null,
  onChange,
}: RankingTableFilterDialogProps) {
  const activeCount = countActiveDailyRankingTableFilters(filters);
  const marketOptions = uniqueOptions(items.map((item) => item.marketCode));
  const sectorOptions = uniqueOptions(items.map((item) => item.sector33Name));
  const watchlistOptions = watchlists.map((watchlist) => ({ value: String(watchlist.id), label: watchlist.name }));
  const activeFilterDescriptors = buildActiveFilterDescriptors(filters, marketOptions, sectorOptions, watchlists);

  const updateFilter = <K extends keyof DailyRankingTableFilters>(key: K, value: DailyRankingTableFilters[K]) => {
    onChange({ ...filters, [key]: value });
  };
  const clearFilterKeys = (keys: FilterKey[]) => {
    const nextFilters = { ...filters };
    for (const key of keys) {
      nextFilters[key] = undefined;
    }
    onChange(nextFilters);
  };
  const clearFilters = () => onChange({});

  return (
    <Dialog>
      <DialogTrigger asChild>
        <Button type="button" variant="outline" size="sm" className="h-8 gap-1.5 px-2 text-xs">
          <SlidersHorizontal className="h-3.5 w-3.5" />
          <span>Filter</span>
          {activeCount > 0 ? (
            <span className="rounded bg-primary px-1.5 py-0.5 text-[10px] font-semibold text-primary-foreground">
              {activeCount}
            </span>
          ) : null}
        </Button>
      </DialogTrigger>
      <DialogContent className="max-h-[85vh] overflow-y-auto sm:max-w-3xl">
        <DialogHeader>
          <DialogTitle>Table Filters</DialogTitle>
          <DialogDescription>Filter the current Daily Ranking rows.</DialogDescription>
        </DialogHeader>

        {activeFilterDescriptors.length > 0 ? (
          <section
            aria-label="Active table filters"
            className="flex flex-wrap items-center gap-1.5 rounded-md border border-primary/20 bg-primary/5 p-2"
          >
            <span className="mr-1 text-[11px] font-semibold text-primary">Active</span>
            {activeFilterDescriptors.map((descriptor) => (
              <button
                key={descriptor.id}
                type="button"
                className="inline-flex max-w-full items-center gap-1 rounded border border-primary/25 bg-background/90 px-2 py-1 text-[11px] font-medium text-primary shadow-sm hover:bg-primary/10 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary/40"
                onClick={() => clearFilterKeys(descriptor.keys)}
                aria-label={`Remove ${descriptor.label}`}
              >
                <span className="truncate">{descriptor.label}</span>
                <X className="h-3 w-3 shrink-0" aria-hidden="true" />
              </button>
            ))}
          </section>
        ) : null}

        <div className="grid gap-4">
          <div className="space-y-2">
            <Label
              htmlFor="ranking-table-filter-text"
              className={cn('text-xs', hasTextValue(filters.text) && ACTIVE_LABEL_CLASS)}
            >
              Search
            </Label>
            <StockSearchInput
              id="ranking-table-filter-text"
              name="ranking-table-filter-text"
              className={cn('h-8 text-xs', hasTextValue(filters.text) && ACTIVE_CONTROL_CLASS)}
              value={filters.text ?? ''}
              onValueChange={(value) => updateFilter('text', value || undefined)}
              onSelect={(stock) => updateFilter('text', stock.code)}
              placeholder="Code or company name"
              searchLimit={50}
            />
          </div>

          <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
            <SelectFilter
              id="ranking-table-filter-market"
              label="Market"
              value={filters.market}
              options={marketOptions}
              allLabel="All Markets"
              onChange={(value) => updateFilter('market', value)}
              isActive={hasTextValue(filters.market)}
            />
            <SelectFilter
              id="ranking-table-filter-sector"
              label="Sector"
              value={filters.sector33Name}
              options={sectorOptions}
              allLabel="All Sectors"
              onChange={(value) => updateFilter('sector33Name', value)}
              isActive={hasTextValue(filters.sector33Name)}
            />
            <SelectFilter
              id="ranking-table-filter-watchlist"
              label="Watchlist"
              value={filters.watchlistId != null ? String(filters.watchlistId) : undefined}
              options={watchlistOptions}
              allLabel={watchlistsLoading ? 'Loading Watchlists' : 'All Watchlists'}
              onChange={(value) => updateFilter('watchlistId', value ? Number(value) : undefined)}
              isActive={typeof filters.watchlistId === 'number' && filters.watchlistId > 0}
            />
            <SelectFilter
              id="ranking-table-filter-regime"
              label="Regime"
              value={filters.regimeState}
              options={RANKING_REGIME_STATE_OPTIONS.filter((option) => option.value !== 'all')}
              allLabel="All Regimes"
              onChange={(value) => updateFilter('regimeState', value as DailyRankingTableFilters['regimeState'])}
              isActive={Boolean(filters.regimeState)}
            />
            <SelectFilter
              id="ranking-table-filter-signal"
              label="Signal"
              value={filters.valuationSignal}
              options={VALUATION_SIGNAL_OPTIONS.filter((option) => option.value !== ALL_VALUE)}
              allLabel="All Signals"
              onChange={(value) =>
                updateFilter('valuationSignal', value as DailyRankingTableFilters['valuationSignal'])
              }
              isActive={Boolean(filters.valuationSignal)}
            />
            <SelectFilter
              id="ranking-table-filter-risk"
              label="Warning"
              value={filters.riskState}
              options={RANKING_RISK_STATE_OPTIONS.filter((option) => option.value !== 'all')}
              allLabel="All Warnings"
              onChange={(value) => updateFilter('riskState', value as DailyRankingTableFilters['riskState'])}
              isActive={Boolean(filters.riskState)}
            />
            <SelectFilter
              id="ranking-table-filter-technical"
              label="Confirmation"
              value={filters.technicalState}
              options={RANKING_TECHNICAL_STATE_OPTIONS.filter((option) => option.value !== 'all')}
              allLabel="All Confirmations"
              onChange={(value) => updateFilter('technicalState', value as DailyRankingTableFilters['technicalState'])}
              isActive={Boolean(filters.technicalState)}
            />
          </div>
          {watchlistsError ? <p className="text-xs text-destructive">{watchlistsError.message}</p> : null}

          <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
            {NUMERIC_GROUPS.map((group) => (
              <RangeInput
                key={group.minKey}
                label={group.label}
                minValue={filters[group.minKey]}
                maxValue={filters[group.maxKey]}
                onMinChange={(value) => updateFilter(group.minKey, value)}
                onMaxChange={(value) => updateFilter(group.maxKey, value)}
              />
            ))}
          </div>
        </div>

        <DialogFooter>
          <Button type="button" variant="outline" size="sm" onClick={clearFilters}>
            Clear
          </Button>
          <DialogClose asChild>
            <Button type="button" size="sm">
              Close
            </Button>
          </DialogClose>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function SelectFilter({
  id,
  label,
  value,
  options,
  allLabel,
  onChange,
  isActive,
}: {
  id: string;
  label: string;
  value: string | undefined;
  options: readonly { value: string; label: string }[];
  allLabel: string;
  onChange: (value: string | undefined) => void;
  isActive?: boolean;
}) {
  return (
    <div className="space-y-2">
      <Label htmlFor={id} className={cn('text-xs', isActive && ACTIVE_LABEL_CLASS)}>
        {label}
      </Label>
      <Select
        value={value ?? ALL_VALUE}
        onValueChange={(nextValue) => onChange(nextValue === ALL_VALUE ? undefined : nextValue)}
      >
        <SelectTrigger id={id} className={cn('h-8 text-xs', isActive && ACTIVE_CONTROL_CLASS)}>
          <SelectValue />
        </SelectTrigger>
        <SelectContent>
          <SelectItem value={ALL_VALUE}>{allLabel}</SelectItem>
          {options.map((option) => (
            <SelectItem key={option.value} value={option.value}>
              {option.label}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
    </div>
  );
}

function RangeInput({
  label,
  minValue,
  maxValue,
  onMinChange,
  onMaxChange,
}: {
  label: string;
  minValue: number | undefined;
  maxValue: number | undefined;
  onMinChange: (value: number | undefined) => void;
  onMaxChange: (value: number | undefined) => void;
}) {
  const hasMinValue = hasNumberValue(minValue);
  const hasMaxValue = hasNumberValue(maxValue);
  return (
    <div className="space-y-2">
      <Label className={cn('text-xs', (hasMinValue || hasMaxValue) && ACTIVE_LABEL_CLASS)}>{label}</Label>
      <div className="grid grid-cols-2 gap-2">
        <DecimalFilterInput
          label={`${label} Min`}
          value={minValue}
          onChange={onMinChange}
          placeholder={`${label} Min`}
          isActive={hasMinValue}
        />
        <DecimalFilterInput
          label={`${label} Max`}
          value={maxValue}
          onChange={onMaxChange}
          placeholder={`${label} Max`}
          isActive={hasMaxValue}
        />
      </div>
    </div>
  );
}

function DecimalFilterInput({
  label,
  value,
  onChange,
  placeholder,
  isActive,
}: {
  label: string;
  value: number | undefined;
  onChange: (value: number | undefined) => void;
  placeholder: string;
  isActive: boolean;
}) {
  const [draftValue, setDraftValue] = useState(formatNumberInput(value));
  const [isFocused, setIsFocused] = useState(false);

  useEffect(() => {
    if (!isFocused) {
      setDraftValue(formatNumberInput(value));
    }
  }, [isFocused, value]);

  return (
    <Input
      className={cn('h-8 text-xs', isActive && ACTIVE_CONTROL_CLASS)}
      inputMode="decimal"
      value={draftValue}
      onFocus={() => setIsFocused(true)}
      onBlur={() => {
        setIsFocused(false);
        setDraftValue(formatNumberInput(value));
      }}
      onChange={(event) => {
        const nextValue = event.currentTarget.value;
        setDraftValue(nextValue);
        onChange(parseNumberInput(nextValue));
      }}
      placeholder={placeholder}
      aria-label={label}
    />
  );
}
