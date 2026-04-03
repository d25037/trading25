import { SectionEyebrow, SegmentedTabs, Surface } from '@/components/Layout/Workspace';
import { DateInput, NumberSelect } from '@/components/shared/filters';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import type {
  RankingParams,
  Topix100PriceBucketFilter,
  Topix100VolumeBucketFilter,
} from '@/types/ranking';
import {
  getTopix100RankingMetricLabel,
  getTopix100RankingMetricDescription,
  getTopix100PriceBucketLabel,
  resolveTopix100PriceSmaWindow,
  resolveTopix100RankingMetric,
  TOPIX100_PRICE_SMA_WINDOW_OPTIONS,
  TOPIX100_RANKING_METRIC_OPTIONS,
} from './topix100RankingMetric';

const PRICE_BUCKET_OPTIONS: { value: Topix100PriceBucketFilter; label: string }[] = [
  { value: 'all', label: getTopix100PriceBucketLabel('all') },
  { value: 'q10', label: getTopix100PriceBucketLabel('q10') },
  { value: 'q234', label: getTopix100PriceBucketLabel('q234') },
  { value: 'q1', label: getTopix100PriceBucketLabel('q1') },
];

const VOLUME_BUCKET_OPTIONS: { value: Topix100VolumeBucketFilter; label: string }[] = [
  { value: 'all', label: 'All Volume' },
  { value: 'low', label: 'Volume Low' },
  { value: 'high', label: 'Volume High' },
];

interface Topix100RankingFiltersProps {
  params: RankingParams;
  onChange: (params: RankingParams) => void;
}

function buildStudyDescription(
  metric: RankingParams['topix100Metric'],
  smaWindow: RankingParams['topix100SmaWindow']
): string {
  const resolvedMetric = resolveTopix100RankingMetric(metric);
  const resolvedSmaWindow = resolveTopix100PriceSmaWindow(smaWindow);
  const metricDescription = getTopix100RankingMetricDescription(resolvedMetric, resolvedSmaWindow);

  if (resolvedMetric === 'price_vs_sma_gap') {
    return `Start at ${getTopix100RankingMetricLabel(resolvedMetric, resolvedSmaWindow)}. ${metricDescription}`;
  }

  return metricDescription;
}

export function Topix100RankingFilters({ params, onChange }: Topix100RankingFiltersProps) {
  const updateParam = <K extends keyof RankingParams>(key: K, value: RankingParams[K]) => {
    onChange({ ...params, [key]: value });
  };
  const metric = resolveTopix100RankingMetric(params.topix100Metric);
  const smaWindow = resolveTopix100PriceSmaWindow(params.topix100SmaWindow);
  const studyDescription = buildStudyDescription(params.topix100Metric, params.topix100SmaWindow);

  return (
    <Surface className="p-3">
      <div className="space-y-0.5 pb-2.5">
        <SectionEyebrow>Study</SectionEyebrow>
        <h2 className="text-base font-semibold text-foreground">TOPIX100 SMA Divergence</h2>
        <p className="text-xs text-muted-foreground">{studyDescription}</p>
      </div>
      <div className="space-y-2.5">
        <DateInput value={params.date} onChange={(value) => updateParam('date', value)} id="topix100-ranking-date" />

        <div className="space-y-1">
          <p className="text-xs font-medium text-foreground">Ranking Metric</p>
          <SegmentedTabs
            items={TOPIX100_RANKING_METRIC_OPTIONS}
            value={metric}
            onChange={(value) => updateParam('topix100Metric', value)}
            itemClassName="h-9 justify-start rounded-lg px-3 py-1.5 text-xs"
            className="flex-col"
          />
        </div>

        {metric === 'price_vs_sma_gap' ? (
          <NumberSelect
            value={smaWindow}
            onChange={(value) => updateParam('topix100SmaWindow', value as RankingParams['topix100SmaWindow'])}
            options={TOPIX100_PRICE_SMA_WINDOW_OPTIONS}
            id="topix100-sma-window"
            label="SMA Window"
            description="SMA50 baseline. Volume split uses SMA 5/20. SMA100 broadens oversold; SMA20 shortens the move."
          />
        ) : null}

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
