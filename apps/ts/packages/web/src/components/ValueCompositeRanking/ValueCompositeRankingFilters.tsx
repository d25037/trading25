import { SectionEyebrow, SegmentedTabs, Surface } from '@/components/Layout/Workspace';
import { DateInput, MarketsSelect, NumberSelect } from '@/components/shared/filters';
import type {
  ValueCompositeForwardEpsMode,
  ValueCompositeRankingParams,
  ValueCompositeScoreMethod,
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

const SCORE_METHOD_OPTIONS = [
  { value: 'standard_pbr_tilt' as ValueCompositeScoreMethod, label: 'PBR tilt' },
  { value: 'standard_size_tilt' as ValueCompositeScoreMethod, label: 'Size tilt' },
  { value: 'equal_weight' as ValueCompositeScoreMethod, label: 'Equal weight' },
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

  return (
    <Surface className="p-4">
      <div className="space-y-1 pb-3">
        <SectionEyebrow>Filter Rail</SectionEyebrow>
        <h2 className="text-base font-semibold text-foreground">Value Score Filters</h2>
        <p className="text-xs text-muted-foreground">Rank low-PBR, small-cap, low-forward-PER candidates.</p>
      </div>
      <div className="space-y-3">
        <div className="space-y-2">
          <SectionEyebrow className="mb-0">Score Method</SectionEyebrow>
          <SegmentedTabs
            items={SCORE_METHOD_OPTIONS}
            value={params.scoreMethod ?? 'standard_pbr_tilt'}
            onChange={(scoreMethod) => updateParam('scoreMethod', scoreMethod)}
            className="grid grid-cols-3 gap-1"
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
