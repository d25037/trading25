import { SectionEyebrow, Surface } from '@/components/Layout/Workspace';
import { DateInput, MarketsSelect, NumberSelect } from '@/components/shared/filters';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import type { RankingParams } from '@/types/ranking';
import {
  applyRankingPreset,
  getRankingPreset,
  RANKING_PRESET_OPTIONS,
  RANKING_REGIME_STATE_OPTIONS,
  RANKING_RISK_STATE_OPTIONS,
  RANKING_TECHNICAL_STATE_OPTIONS,
  type RankingPreset,
} from './rankingState';

const RANKING_MARKET_OPTIONS = [
  { value: 'prime', label: 'Prime' },
  { value: 'standard', label: 'Standard' },
  { value: 'prime,standard', label: 'Prime + Standard' },
];

export const RANKING_LOOKBACK_OPTIONS = [
  { value: 1, label: '1 day' },
  { value: 5, label: '5 days' },
  { value: 10, label: '10 days' },
  { value: 20, label: '20 days' },
];

const PERIOD_OPTIONS = [
  { value: 60, label: '60 days' },
  { value: 120, label: '120 days' },
  { value: 250, label: '250 days (1Y)' },
];

const FORWARD_EPS_DISCLOSURE_OPTIONS = [
  { value: 0, label: 'All' },
  { value: 126, label: '126 days' },
  { value: 63, label: '63 days' },
  { value: 252, label: '252 days' },
];

export const SECTOR_SCORE_FAMILY_OPTIONS = [
  { value: 'current', label: 'Sector Score' },
  { value: 'long_hybrid_leadership', label: 'Long Hybrid Leadership' },
] as const;

interface RankingFiltersProps {
  params: RankingParams;
  onChange: (params: RankingParams) => void;
}

export function RankingFilters({ params, onChange }: RankingFiltersProps) {
  const rankingPreset = getRankingPreset(params);
  const updateParam = <K extends keyof RankingParams>(key: K, value: RankingParams[K]) => {
    onChange({ ...params, [key]: value });
  };
  const updatePreset = (preset: RankingPreset) => {
    onChange(applyRankingPreset(params, preset));
  };

  return (
    <Surface className="p-4">
      <div className="space-y-1 pb-3">
        <SectionEyebrow>Filter Rail</SectionEyebrow>
        <h2 className="text-base font-semibold text-foreground">Ranking Filters</h2>
        <p className="text-xs text-muted-foreground">Adjust market scope, comparison window, and reference session.</p>
      </div>
      <div className="space-y-3">
        <MarketsSelect
          value={params.markets || 'prime'}
          onChange={(v) => updateParam('markets', v)}
          options={RANKING_MARKET_OPTIONS}
          id="ranking-markets"
        />
        <NumberSelect
          value={params.lookbackDays || 1}
          onChange={(v) => updateParam('lookbackDays', v)}
          options={RANKING_LOOKBACK_OPTIONS}
          id="ranking-lookbackDays"
          label="Lookback Days"
        />
        <NumberSelect
          value={params.forwardEpsDisclosedWithinDays ?? 0}
          onChange={(v) => updateParam('forwardEpsDisclosedWithinDays', v)}
          options={FORWARD_EPS_DISCLOSURE_OPTIONS}
          id="ranking-forward-eps-disclosed-within-days"
          label="Fwd EPS Disclosure"
        />
        <div className="space-y-2">
          <label className="text-xs font-medium" htmlFor="ranking-sector-score-family">
            Sector Selector
          </label>
          <Select
            value={params.sectorScoreFamily ?? 'current'}
            onValueChange={(value) => updateParam('sectorScoreFamily', value as RankingParams['sectorScoreFamily'])}
          >
            <SelectTrigger id="ranking-sector-score-family" className="h-8 text-xs">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {SECTOR_SCORE_FAMILY_OPTIONS.map((option) => (
                <SelectItem key={option.value} value={option.value}>
                  {option.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
        <div className="space-y-2">
          <label className="text-xs font-medium" htmlFor="ranking-preset">
            Preset
          </label>
          <Select value={rankingPreset} onValueChange={(value) => updatePreset(value as RankingPreset)}>
            <SelectTrigger id="ranking-preset" className="h-8 text-xs">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {RANKING_PRESET_OPTIONS.map((option) => (
                <SelectItem key={option.value} value={option.value}>
                  {option.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
        <details className="rounded border border-border/50 px-3 py-2">
          <summary className="cursor-pointer text-xs font-medium text-muted-foreground">Advanced</summary>
          <div className="mt-3 space-y-3">
            <div className="space-y-2">
              <label className="text-xs font-medium" htmlFor="ranking-regime-state">
                Regime
              </label>
              <Select
                value={params.regimeState ?? 'all'}
                onValueChange={(value) =>
                  updateParam('regimeState', value === 'all' ? undefined : (value as RankingParams['regimeState']))
                }
              >
                <SelectTrigger id="ranking-regime-state" className="h-8 text-xs">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {RANKING_REGIME_STATE_OPTIONS.map((option) => (
                    <SelectItem key={option.value} value={option.value}>
                      {option.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-2">
              <label className="text-xs font-medium" htmlFor="ranking-risk-state">
                Warning
              </label>
              <Select
                value={params.riskState ?? 'all'}
                onValueChange={(value) =>
                  updateParam('riskState', value === 'all' ? undefined : (value as RankingParams['riskState']))
                }
              >
                <SelectTrigger id="ranking-risk-state" className="h-8 text-xs">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {RANKING_RISK_STATE_OPTIONS.map((option) => (
                    <SelectItem key={option.value} value={option.value}>
                      {option.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-2">
              <label className="text-xs font-medium" htmlFor="ranking-confirmation-state">
                Confirmation
              </label>
              <Select
                value={params.technicalState ?? 'all'}
                onValueChange={(value) =>
                  updateParam(
                    'technicalState',
                    value === 'all' ? undefined : (value as RankingParams['technicalState'])
                  )
                }
              >
                <SelectTrigger id="ranking-confirmation-state" className="h-8 text-xs">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {RANKING_TECHNICAL_STATE_OPTIONS.map((option) => (
                    <SelectItem key={option.value} value={option.value}>
                      {option.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>
        </details>
        <DateInput value={params.date} onChange={(v) => updateParam('date', v)} id="ranking-date" />
      </div>
    </Surface>
  );
}

export function TechnicalEventFilters({ params, onChange }: RankingFiltersProps) {
  const updateParam = <K extends keyof RankingParams>(key: K, value: RankingParams[K]) => {
    onChange({ ...params, [key]: value });
  };

  return (
    <Surface className="p-4">
      <div className="space-y-1 pb-3">
        <SectionEyebrow>Filter Rail</SectionEyebrow>
        <h2 className="text-base font-semibold text-foreground">Technical Events</h2>
        <p className="text-xs text-muted-foreground">
          Find stocks making range highs or lows within the selected scope.
        </p>
      </div>
      <div className="space-y-3">
        <MarketsSelect
          value={params.markets || 'prime'}
          onChange={(v) => updateParam('markets', v)}
          options={RANKING_MARKET_OPTIONS}
          id="ranking-technical-markets"
        />
        <div className="space-y-2">
          <label className="text-xs font-medium" htmlFor="ranking-technical-event-type">
            Event Type
          </label>
          <Select
            value={params.technicalEventType || 'periodHigh'}
            onValueChange={(value) => updateParam('technicalEventType', value as RankingParams['technicalEventType'])}
          >
            <SelectTrigger id="ranking-technical-event-type" className="h-8 text-xs">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="periodHigh">New High</SelectItem>
              <SelectItem value="periodLow">New Low</SelectItem>
            </SelectContent>
          </Select>
        </div>
        <NumberSelect
          value={params.periodDays || 250}
          onChange={(v) => updateParam('periodDays', v)}
          options={PERIOD_OPTIONS}
          id="ranking-technical-periodDays"
          label="Period Days"
        />
        <DateInput value={params.date} onChange={(v) => updateParam('date', v)} id="ranking-technical-date" />
      </div>
    </Surface>
  );
}
