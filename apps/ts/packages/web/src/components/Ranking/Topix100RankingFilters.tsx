import { SectionEyebrow, SegmentedTabs, Surface } from '@/components/Layout/Workspace';
import { DateInput } from '@/components/shared/filters';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import type {
  RankingParams,
  Topix100PriceBucketFilter,
  Topix100VolumeBucketFilter,
} from '@/types/ranking';
import {
  getTopix100RankingMetricDescription,
  resolveTopix100RankingMetric,
  TOPIX100_RANKING_METRIC_OPTIONS,
} from './topix100RankingMetric';

const PRICE_BUCKET_OPTIONS: { value: Topix100PriceBucketFilter; label: string }[] = [
  { value: 'all', label: 'All Buckets' },
  { value: 'q1', label: 'Q1' },
  { value: 'q10', label: 'Q10' },
  { value: 'q456', label: 'Q4-6' },
];

const VOLUME_BUCKET_OPTIONS: { value: Topix100VolumeBucketFilter; label: string }[] = [
  { value: 'all', label: 'All Volume' },
  { value: 'high', label: 'Volume High' },
  { value: 'low', label: 'Volume Low' },
];

interface Topix100RankingFiltersProps {
  params: RankingParams;
  onChange: (params: RankingParams) => void;
}

export function Topix100RankingFilters({ params, onChange }: Topix100RankingFiltersProps) {
  const updateParam = <K extends keyof RankingParams>(key: K, value: RankingParams[K]) => {
    onChange({ ...params, [key]: value });
  };
  const metric = resolveTopix100RankingMetric(params.topix100Metric);

  return (
    <Surface className="p-4">
      <div className="space-y-1 pb-3">
        <SectionEyebrow>TOPIX100 Study View</SectionEyebrow>
        <h2 className="text-base font-semibold text-foreground">TOPIX100 Ranking</h2>
        <p className="text-xs text-muted-foreground">
          Default sort is price / SMA20 gap. Toggle to the legacy price SMA 20/80 view when you want the prior study
          framing.
        </p>
      </div>
      <div className="space-y-3">
        <DateInput value={params.date} onChange={(value) => updateParam('date', value)} id="topix100-ranking-date" />

        <div className="space-y-1.5">
          <p className="text-xs font-medium text-foreground">Ranking Metric</p>
          <SegmentedTabs
            items={TOPIX100_RANKING_METRIC_OPTIONS}
            value={metric}
            onChange={(value) => updateParam('topix100Metric', value)}
            itemClassName="h-9 justify-start rounded-lg px-3 py-1.5 text-xs"
            className="flex-col"
          />
          <p className="text-[11px] text-muted-foreground">{getTopix100RankingMetricDescription(metric)}</p>
        </div>

        <div className="space-y-1.5">
          <label htmlFor="topix100-price-bucket" className="text-xs font-medium text-foreground">
            Price Bucket
          </label>
          <Select
            value={params.topix100PriceBucket ?? 'all'}
            onValueChange={(value) => updateParam('topix100PriceBucket', value as Topix100PriceBucketFilter)}
          >
            <SelectTrigger id="topix100-price-bucket" className="h-9 text-xs">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {PRICE_BUCKET_OPTIONS.map((option) => (
                <SelectItem key={option.value} value={option.value} className="text-xs">
                  {option.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>

        <div className="space-y-1.5">
          <label htmlFor="topix100-volume-bucket" className="text-xs font-medium text-foreground">
            Volume Bucket
          </label>
          <Select
            value={params.topix100VolumeBucket ?? 'all'}
            onValueChange={(value) => updateParam('topix100VolumeBucket', value as Topix100VolumeBucketFilter)}
          >
            <SelectTrigger id="topix100-volume-bucket" className="h-9 text-xs">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {VOLUME_BUCKET_OPTIONS.map((option) => (
                <SelectItem key={option.value} value={option.value} className="text-xs">
                  {option.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      </div>
    </Surface>
  );
}
