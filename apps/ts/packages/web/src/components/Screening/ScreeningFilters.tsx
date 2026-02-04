import { DateInput, MarketsSelect, NumberSelect } from '@/components/shared/filters';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Switch } from '@/components/ui/switch';
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
}

export function ScreeningFilters({ params, onChange }: ScreeningFiltersProps) {
  const updateParam = <K extends keyof ScreeningParams>(key: K, value: ScreeningParams[K]) => {
    onChange({ ...params, [key]: value });
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

        {/* Screening Types */}
        <div className="space-y-3">
          <Label htmlFor="screeningTypes" className="text-xs">
            Screening Types
          </Label>
          <div className="flex items-center justify-between">
            <span className="text-xs text-muted-foreground">Range Break Fast</span>
            <Switch
              checked={params.rangeBreakFast !== false}
              onCheckedChange={(checked) => updateParam('rangeBreakFast', checked)}
            />
          </div>
          <div className="flex items-center justify-between">
            <span className="text-xs text-muted-foreground">Range Break Slow</span>
            <Switch
              checked={params.rangeBreakSlow !== false}
              onCheckedChange={(checked) => updateParam('rangeBreakSlow', checked)}
            />
          </div>
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

        {/* Min Break Percentage */}
        <div className="space-y-2">
          <Label htmlFor="minBreakPct" className="text-xs">
            Min Break %
          </Label>
          <Input
            id="minBreakPct"
            type="number"
            step="0.5"
            min="0"
            placeholder="e.g. 5.0"
            className="h-8 text-xs"
            value={params.minBreakPercentage ?? ''}
            onChange={(e) => updateParam('minBreakPercentage', e.target.value ? Number(e.target.value) : undefined)}
          />
        </div>

        {/* Min Volume Ratio */}
        <div className="space-y-2">
          <Label htmlFor="minVolRatio" className="text-xs">
            Min Volume Ratio
          </Label>
          <Input
            id="minVolRatio"
            type="number"
            step="0.1"
            min="0"
            placeholder="e.g. 1.5"
            className="h-8 text-xs"
            value={params.minVolumeRatio ?? ''}
            onChange={(e) => updateParam('minVolumeRatio', e.target.value ? Number(e.target.value) : undefined)}
          />
        </div>

        {/* Sort */}
        <div className="space-y-2">
          <Label htmlFor="sortBy" className="text-xs">
            Sort By
          </Label>
          <Select value={params.sortBy || 'date'} onValueChange={(v) => updateParam('sortBy', v as ScreeningSortBy)}>
            <SelectTrigger id="sortBy" className="h-8 text-xs">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="date">Date</SelectItem>
              <SelectItem value="stockCode">Stock Code</SelectItem>
              <SelectItem value="volumeRatio">Volume Ratio</SelectItem>
              <SelectItem value="breakPercentage">Break %</SelectItem>
            </SelectContent>
          </Select>
        </div>

        {/* Order */}
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
