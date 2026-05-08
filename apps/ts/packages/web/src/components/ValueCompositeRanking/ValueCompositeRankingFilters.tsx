import { SectionEyebrow, SegmentedTabs, Surface } from '@/components/Layout/Workspace';
import { DateInput, MarketsSelect, NumberSelect } from '@/components/shared/filters';
import { Switch } from '@/components/ui/switch';
import type {
  ValueCompositeForwardEpsMode,
  ValueCompositeProfileId,
  ValueCompositeRankingParams,
} from '@/types/valueCompositeRanking';

const VALUE_COMPOSITE_MARKET_OPTIONS = [
  { value: 'standard', label: 'Standard' },
  { value: 'prime', label: 'Prime' },
  { value: 'prime,standard', label: 'Prime + Standard' },
];

const LIMIT_OPTIONS = [
  { value: 20, label: '20' },
  { value: 50, label: '50' },
  { value: 100, label: '100' },
  { value: 200, label: '200' },
];

const PROFILE_OPTIONS = [
  { value: 'standard_breakout_120d20' as ValueCompositeProfileId, label: 'Standard 120d' },
  { value: 'prime_size75_forward_per25' as ValueCompositeProfileId, label: 'Prime size75' },
];

const FORWARD_EPS_MODE_OPTIONS = [
  { value: 'latest' as ValueCompositeForwardEpsMode, label: 'Latest revised EPS' },
  { value: 'fy' as ValueCompositeForwardEpsMode, label: 'FY-only EPS' },
];

interface ValueCompositeRankingFiltersProps {
  params: ValueCompositeRankingParams;
  onChange: (params: ValueCompositeRankingParams) => void;
}

export function ValueCompositeRankingFilters({ params, onChange }: ValueCompositeRankingFiltersProps) {
  const updateParam = <K extends keyof ValueCompositeRankingParams>(key: K, value: ValueCompositeRankingParams[K]) => {
    onChange({ ...params, [key]: value });
  };
  const updateProfile = (profileId: ValueCompositeProfileId) => {
    const markets = profileId === 'prime_size75_forward_per25' ? 'prime' : 'standard';
    onChange({ ...params, profileId, markets, scoreMethod: undefined });
  };

  return (
    <Surface className="p-4">
      <div className="space-y-1 pb-3">
        <SectionEyebrow>Filter Rail</SectionEyebrow>
        <h2 className="text-base font-semibold text-foreground">Value Score Filters</h2>
        <p className="text-xs text-muted-foreground">Rank low-PBR, small-cap, low-forward-PER candidates.</p>
      </div>
      <div className="space-y-3">
        <div className="space-y-2">
          <SectionEyebrow className="mb-0">Profile</SectionEyebrow>
          <SegmentedTabs
            items={PROFILE_OPTIONS}
            value={params.profileId ?? 'standard_breakout_120d20'}
            onChange={updateProfile}
            className="grid grid-cols-2 gap-1"
            itemClassName="h-8 justify-center rounded-lg px-2 py-1.5 text-xs"
          />
        </div>
        <div className="space-y-2">
          <SectionEyebrow className="mb-0">Forward EPS Basis</SectionEyebrow>
          <SegmentedTabs
            items={FORWARD_EPS_MODE_OPTIONS}
            value={params.forwardEpsMode ?? 'latest'}
            onChange={(forwardEpsMode) => updateParam('forwardEpsMode', forwardEpsMode)}
            className="grid grid-cols-2 gap-1"
            itemClassName="h-8 justify-center rounded-lg px-2 py-1.5 text-xs"
          />
          <p className="text-xs text-muted-foreground">
            Latest revised EPS matches the previous default: revised quarterly forecasts when available, otherwise FY.
            FY-only pins the latest FY forecast.
          </p>
        </div>
        <MarketsSelect
          value={params.markets || 'standard'}
          onChange={(v) => updateParam('markets', v)}
          options={VALUE_COMPOSITE_MARKET_OPTIONS}
          id="value-composite-ranking-markets"
        />
        <div className="flex items-center justify-between gap-3 rounded-lg border border-border/60 px-3 py-2">
          <div className="space-y-0.5">
            <label className="text-xs font-medium text-foreground" htmlFor="value-composite-liquidity-filter">
              ADV60 {">="} 10mn
            </label>
            <p className="text-[11px] text-muted-foreground">Apply as a hard liquidity filter.</p>
          </div>
          <Switch
            id="value-composite-liquidity-filter"
            checked={params.applyLiquidityFilter ?? true}
            onCheckedChange={(checked) => updateParam('applyLiquidityFilter', checked)}
          />
        </div>
        <DateInput
          value={params.date}
          onChange={(date) => updateParam('date', date)}
          id="value-composite-ranking-date"
        />
        <NumberSelect
          value={params.limit || 50}
          onChange={(v) => updateParam('limit', v)}
          options={LIMIT_OPTIONS}
          id="value-composite-ranking-limit"
          label="Results"
        />
      </div>
    </Surface>
  );
}
