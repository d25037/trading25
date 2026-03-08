import { Zap } from 'lucide-react';
import { useCallback, useId } from 'react';
import { Button } from '@/components/ui/button';
import { Label } from '@/components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Switch } from '@/components/ui/switch';
import {
  type Phase1SignalType,
  SIGNAL_CATEGORIES,
  SIGNAL_DEFAULTS,
  SIGNAL_LABELS,
} from '@/hooks/useBtSignals';
import { useChartStore, type SignalConfig } from '@/stores/chartStore';
import { IndicatorToggle, NumberInput } from './IndicatorToggle';

const RELATIVE_MODE_DISABLED_SIGNAL_TYPES = new Set<Phase1SignalType>([
  'volume_ratio_above',
  'volume_ratio_below',
  'trading_value',
  'trading_value_range',
]);

interface SignalOverlayControlsProps {
  disabled?: boolean;
}

export function SignalOverlayControls({ disabled }: SignalOverlayControlsProps) {
  const { settings, toggleSignalOverlay, addSignal, removeSignal, updateSignal, toggleSignal } = useChartStore();
  const { relativeMode } = settings;
  // signalOverlayが未定義の場合のデフォルト値
  const signalOverlay = settings.signalOverlay ?? { enabled: false, signals: [] };

  const signalOverlayId = useId();

  const handleAddSignal = useCallback(
    (type: Phase1SignalType) => {
      const defaults = SIGNAL_DEFAULTS[type];
      addSignal({
        type,
        params: { ...defaults },
        mode: 'entry',
        enabled: true,
      });
    },
    [addSignal]
  );

  // relativeMode時に無効化されるシグナル
  const isSignalDisabledInRelativeMode = (type: string) => {
    return relativeMode && RELATIVE_MODE_DISABLED_SIGNAL_TYPES.has(type as Phase1SignalType);
  };

  // 追加可能なシグナル（まだ追加されていないもの）
  const availableSignals = Object.entries(SIGNAL_CATEGORIES).flatMap(([, category]) =>
    category.signals.filter((type) => !signalOverlay.signals.some((s) => s.type === type))
  );

  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between p-2 rounded glass-panel">
        <div className="flex items-center gap-1.5">
          <Zap className="h-3.5 w-3.5 text-muted-foreground" />
          <Label htmlFor={signalOverlayId} className="text-xs font-medium cursor-pointer">
            Signal Overlay
          </Label>
        </div>
        <Switch
          id={signalOverlayId}
          checked={signalOverlay.enabled}
          onCheckedChange={toggleSignalOverlay}
          disabled={disabled}
        />
      </div>

      {signalOverlay.enabled && (
        <div className="space-y-2 pl-2">
          {/* Add Signal Selector */}
          {availableSignals.length > 0 && (
            <div className="flex gap-2">
              <Select onValueChange={(value) => handleAddSignal(value as Phase1SignalType)}>
                <SelectTrigger className="h-8 text-xs glass-panel border-border/30 flex-1">
                  <SelectValue placeholder="Add signal..." />
                </SelectTrigger>
                <SelectContent className="bg-background/95 backdrop-blur-md border-border shadow-xl max-h-64">
                  {Object.entries(SIGNAL_CATEGORIES).map(([key, category]) => {
                    const availableInCategory = category.signals.filter(
                      (type) => !signalOverlay.signals.some((s) => s.type === type)
                    );
                    if (availableInCategory.length === 0) return null;
                    return (
                      <div key={key}>
                        <div className="px-2 py-1 text-xs font-semibold text-muted-foreground">
                          {category.label}
                        </div>
                        {availableInCategory.map((type) => (
                          <SelectItem
                            key={type}
                            value={type}
                            className="text-xs"
                            disabled={isSignalDisabledInRelativeMode(type)}
                          >
                            {SIGNAL_LABELS[type as Phase1SignalType]}
                            {isSignalDisabledInRelativeMode(type) && ' (disabled in relative mode)'}
                          </SelectItem>
                        ))}
                      </div>
                    );
                  })}
                </SelectContent>
              </Select>
            </div>
          )}

          {/* Configured Signals */}
          {signalOverlay.signals.map((signal) => (
            <SignalConfigPanel
              key={signal.type}
              signal={signal}
              disabled={disabled || isSignalDisabledInRelativeMode(signal.type)}
              onToggle={() => toggleSignal(signal.type)}
              onUpdate={(updates) => updateSignal(signal.type, updates)}
              onRemove={() => removeSignal(signal.type)}
            />
          ))}

          {signalOverlay.signals.length === 0 && (
            <div className="text-xs text-muted-foreground text-center py-2">
              No signals configured. Add a signal above.
            </div>
          )}
        </div>
      )}
    </div>
  );
}

interface SignalConfigPanelProps {
  signal: SignalConfig;
  disabled?: boolean;
  onToggle: () => void;
  onUpdate: (updates: Partial<SignalConfig>) => void;
  onRemove: () => void;
}

function SignalConfigPanel({ signal, disabled, onToggle, onUpdate, onRemove }: SignalConfigPanelProps) {
  const label = SIGNAL_LABELS[signal.type as Phase1SignalType] || signal.type;

  return (
    <IndicatorToggle
      label={label}
      enabled={signal.enabled}
      onToggle={onToggle}
      disabled={disabled}
    >
      <div className="space-y-2">
        {/* Mode Selector */}
        <div className="flex items-center gap-2">
          <Label className="text-xs w-12">Mode</Label>
          <Select
            value={signal.mode}
            onValueChange={(value: 'entry' | 'exit') => onUpdate({ mode: value })}
            disabled={disabled || signal.type === 'buy_and_hold'}
          >
            <SelectTrigger className="h-7 text-xs flex-1">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="entry" className="text-xs">
                Entry
              </SelectItem>
              <SelectItem value="exit" className="text-xs" disabled={signal.type === 'buy_and_hold'}>
                Exit
              </SelectItem>
            </SelectContent>
          </Select>
        </div>

        {/* Signal-specific parameters */}
        <SignalParamsEditor
          type={signal.type as Phase1SignalType}
          params={signal.params}
          onUpdate={(params) => onUpdate({ params })}
          disabled={disabled}
        />

        {/* Remove button */}
        <Button
          variant="ghost"
          size="sm"
          className="w-full h-7 text-xs text-destructive hover:text-destructive hover:bg-destructive/10"
          onClick={onRemove}
        >
          Remove
        </Button>
      </div>
    </IndicatorToggle>
  );
}

interface SignalParamsEditorProps {
  type: Phase1SignalType;
  params: Record<string, number | string | boolean>;
  onUpdate: (params: Record<string, number | string | boolean>) => void;
  disabled?: boolean;
}

function SignalParamsEditor({ type, params, onUpdate, disabled }: SignalParamsEditorProps) {
  const updateParam = (key: string, value: number | string | boolean) => {
    onUpdate({ ...params, ...{ [key]: value } });
  };

  switch (type) {
    case 'rsi_threshold':
      return (
        <div className="grid grid-cols-2 gap-1.5">
          <NumberInput
            label="Period"
            value={params.period as number}
            onChange={(v) => updateParam('period', v)}
            defaultValue={14}
            disabled={disabled}
          />
          <NumberInput
            label="Threshold"
            value={params.threshold as number}
            onChange={(v) => updateParam('threshold', v)}
            defaultValue={30}
            disabled={disabled}
          />
        </div>
      );

    case 'rsi_spread':
      return (
        <div className="grid grid-cols-2 gap-1.5">
          <NumberInput
            label="Fast Period"
            value={params.fast_period as number}
            onChange={(v) => updateParam('fast_period', v)}
            defaultValue={5}
            disabled={disabled}
          />
          <NumberInput
            label="Slow Period"
            value={params.slow_period as number}
            onChange={(v) => updateParam('slow_period', v)}
            defaultValue={14}
            disabled={disabled}
          />
          <NumberInput
            label="Threshold"
            value={params.threshold as number}
            onChange={(v) => updateParam('threshold', v)}
            defaultValue={10}
            disabled={disabled}
          />
        </div>
      );

    case 'period_extrema_break':
      return (
        <div className="grid grid-cols-2 gap-1.5">
          <NumberInput
            label="Period"
            value={params.period as number}
            onChange={(v) => updateParam('period', v)}
            defaultValue={20}
            disabled={disabled}
          />
          <div className="space-y-1">
            <Label className="text-[10px] text-muted-foreground">Direction</Label>
            <Select
              value={params.direction as string}
              onValueChange={(v) => updateParam('direction', v)}
              disabled={disabled}
            >
              <SelectTrigger className="h-7 text-xs">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="high" className="text-xs">High</SelectItem>
                <SelectItem value="low" className="text-xs">Low</SelectItem>
              </SelectContent>
            </Select>
          </div>
          <NumberInput
            label="Lookback"
            value={params.lookback_days as number}
            onChange={(v) => updateParam('lookback_days', v)}
            defaultValue={1}
            disabled={disabled}
          />
        </div>
      );

    case 'period_extrema_position':
      return (
        <div className="grid grid-cols-2 gap-1.5">
          <NumberInput
            label="Period"
            value={params.period as number}
            onChange={(v) => updateParam('period', v)}
            defaultValue={20}
            disabled={disabled}
          />
          <div className="space-y-1">
            <Label className="text-[10px] text-muted-foreground">Direction</Label>
            <Select
              value={params.direction as string}
              onValueChange={(v) => updateParam('direction', v)}
              disabled={disabled}
            >
              <SelectTrigger className="h-7 text-xs">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="high" className="text-xs">High</SelectItem>
                <SelectItem value="low" className="text-xs">Low</SelectItem>
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-1">
            <Label className="text-[10px] text-muted-foreground">State</Label>
            <Select
              value={params.state as string}
              onValueChange={(v) => updateParam('state', v)}
              disabled={disabled}
            >
              <SelectTrigger className="h-7 text-xs">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="at_extrema" className="text-xs">At Extrema</SelectItem>
                <SelectItem value="away_from_extrema" className="text-xs">Away From Extrema</SelectItem>
              </SelectContent>
            </Select>
          </div>
          <NumberInput
            label="Lookback"
            value={params.lookback_days as number}
            onChange={(v) => updateParam('lookback_days', v)}
            defaultValue={1}
            disabled={disabled}
          />
        </div>
      );

    case 'atr_support_position':
    case 'atr_support_cross':
      return (
        <div className="grid grid-cols-2 gap-1.5">
          <NumberInput
            label="Period"
            value={params.lookback_period as number}
            onChange={(v) => updateParam('lookback_period', v)}
            defaultValue={20}
            disabled={disabled}
          />
          <NumberInput
            label="ATR x"
            value={params.atr_multiplier as number}
            onChange={(v) => updateParam('atr_multiplier', v)}
            step="0.1"
            defaultValue={2.0}
            disabled={disabled}
          />
          <div className="space-y-1">
            <Label className="text-[10px] text-muted-foreground">Direction</Label>
            <Select
              value={params.direction as string}
              onValueChange={(v) => updateParam('direction', v)}
              disabled={disabled}
            >
              <SelectTrigger className="h-7 text-xs">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="below" className="text-xs">Below</SelectItem>
                <SelectItem value="above" className="text-xs">Above</SelectItem>
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-1">
            <Label className="text-[10px] text-muted-foreground">Price</Label>
            <Select
              value={params.price_column as string}
              onValueChange={(v) => updateParam('price_column', v)}
              disabled={disabled}
            >
              <SelectTrigger className="h-7 text-xs">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="close" className="text-xs">Close</SelectItem>
                <SelectItem value="low" className="text-xs">Low</SelectItem>
              </SelectContent>
            </Select>
          </div>
          {type === 'atr_support_cross' && (
            <NumberInput
              label="Lookback"
              value={params.lookback_days as number}
              onChange={(v) => updateParam('lookback_days', v)}
              defaultValue={1}
              disabled={disabled}
            />
          )}
        </div>
      );

    case 'baseline_cross':
      return (
        <div className="grid grid-cols-2 gap-1.5">
          <NumberInput
            label="Period"
            value={params.baseline_period as number}
            onChange={(v) => updateParam('baseline_period', v)}
            defaultValue={200}
            disabled={disabled}
          />
          <div className="space-y-1">
            <Label className="text-[10px] text-muted-foreground">Baseline</Label>
            <Select
              value={params.baseline_type as string}
              onValueChange={(v) => updateParam('baseline_type', v)}
              disabled={disabled}
            >
              <SelectTrigger className="h-7 text-xs">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="sma" className="text-xs">SMA</SelectItem>
                <SelectItem value="ema" className="text-xs">EMA</SelectItem>
                <SelectItem value="vwema" className="text-xs">VWEMA</SelectItem>
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-1">
            <Label className="text-[10px] text-muted-foreground">Direction</Label>
            <Select
              value={params.direction as string}
              onValueChange={(v) => updateParam('direction', v)}
              disabled={disabled}
            >
              <SelectTrigger className="h-7 text-xs">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="above" className="text-xs">Above</SelectItem>
                <SelectItem value="below" className="text-xs">Below</SelectItem>
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-1">
            <Label className="text-[10px] text-muted-foreground">Price</Label>
            <Select
              value={params.price_column as string}
              onValueChange={(v) => updateParam('price_column', v)}
              disabled={disabled}
            >
              <SelectTrigger className="h-7 text-xs">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="close" className="text-xs">Close</SelectItem>
                <SelectItem value="high" className="text-xs">High</SelectItem>
                <SelectItem value="low" className="text-xs">Low</SelectItem>
              </SelectContent>
            </Select>
          </div>
          <NumberInput
            label="Lookback"
            value={params.lookback_days as number}
            onChange={(v) => updateParam('lookback_days', v)}
            defaultValue={1}
            disabled={disabled}
          />
        </div>
      );

    case 'baseline_deviation':
      return (
        <div className="grid grid-cols-2 gap-1.5">
          <NumberInput
            label="Period"
            value={params.baseline_period as number}
            onChange={(v) => updateParam('baseline_period', v)}
            defaultValue={20}
            disabled={disabled}
          />
          <NumberInput
            label="Threshold"
            value={params.deviation_threshold as number}
            onChange={(v) => updateParam('deviation_threshold', v)}
            step="0.01"
            defaultValue={0.05}
            disabled={disabled}
          />
          <div className="space-y-1">
            <Label className="text-[10px] text-muted-foreground">Baseline</Label>
            <Select
              value={params.baseline_type as string}
              onValueChange={(v) => updateParam('baseline_type', v)}
              disabled={disabled}
            >
              <SelectTrigger className="h-7 text-xs">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="sma" className="text-xs">SMA</SelectItem>
                <SelectItem value="ema" className="text-xs">EMA</SelectItem>
                <SelectItem value="vwema" className="text-xs">VWEMA</SelectItem>
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-1">
            <Label className="text-[10px] text-muted-foreground">Direction</Label>
            <Select
              value={params.direction as string}
              onValueChange={(v) => updateParam('direction', v)}
              disabled={disabled}
            >
              <SelectTrigger className="h-7 text-xs">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="above" className="text-xs">Above</SelectItem>
                <SelectItem value="below" className="text-xs">Below</SelectItem>
              </SelectContent>
            </Select>
          </div>
        </div>
      );

    case 'baseline_position':
      return (
        <div className="grid grid-cols-2 gap-1.5">
          <NumberInput
            label="Period"
            value={params.baseline_period as number}
            onChange={(v) => updateParam('baseline_period', v)}
            defaultValue={20}
            disabled={disabled}
          />
          <div className="space-y-1">
            <Label className="text-[10px] text-muted-foreground">Baseline</Label>
            <Select
              value={params.baseline_type as string}
              onValueChange={(v) => updateParam('baseline_type', v)}
              disabled={disabled}
            >
              <SelectTrigger className="h-7 text-xs">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="sma" className="text-xs">SMA</SelectItem>
                <SelectItem value="ema" className="text-xs">EMA</SelectItem>
                <SelectItem value="vwema" className="text-xs">VWEMA</SelectItem>
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-1">
            <Label className="text-[10px] text-muted-foreground">Price</Label>
            <Select
              value={params.price_column as string}
              onValueChange={(v) => updateParam('price_column', v)}
              disabled={disabled}
            >
              <SelectTrigger className="h-7 text-xs">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="close" className="text-xs">Close</SelectItem>
                <SelectItem value="high" className="text-xs">High</SelectItem>
                <SelectItem value="low" className="text-xs">Low</SelectItem>
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-1">
            <Label className="text-[10px] text-muted-foreground">Direction</Label>
            <Select
              value={params.direction as string}
              onValueChange={(v) => updateParam('direction', v)}
              disabled={disabled}
            >
              <SelectTrigger className="h-7 text-xs">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="above" className="text-xs">Above</SelectItem>
                <SelectItem value="below" className="text-xs">Below</SelectItem>
              </SelectContent>
            </Select>
          </div>
        </div>
      );

    case 'retracement_position':
    case 'retracement_cross':
      return (
        <div className="grid grid-cols-2 gap-1.5">
          <NumberInput
            label="Period"
            value={params.lookback_period as number}
            onChange={(v) => updateParam('lookback_period', v)}
            defaultValue={20}
            disabled={disabled}
          />
          <NumberInput
            label="Level"
            value={params.retracement_level as number}
            onChange={(v) => updateParam('retracement_level', v)}
            step="0.001"
            defaultValue={0.382}
            disabled={disabled}
          />
          <div className="space-y-1">
            <Label className="text-[10px] text-muted-foreground">Direction</Label>
            <Select
              value={params.direction as string}
              onValueChange={(v) => updateParam('direction', v)}
              disabled={disabled}
            >
              <SelectTrigger className="h-7 text-xs">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="below" className="text-xs">Below</SelectItem>
                <SelectItem value="above" className="text-xs">Above</SelectItem>
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-1">
            <Label className="text-[10px] text-muted-foreground">Price</Label>
            <Select
              value={params.price_column as string}
              onValueChange={(v) => updateParam('price_column', v)}
              disabled={disabled}
            >
              <SelectTrigger className="h-7 text-xs">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="close" className="text-xs">Close</SelectItem>
                <SelectItem value="low" className="text-xs">Low</SelectItem>
              </SelectContent>
            </Select>
          </div>
          {type === 'retracement_cross' && (
            <NumberInput
              label="Lookback"
              value={params.lookback_days as number}
              onChange={(v) => updateParam('lookback_days', v)}
              defaultValue={1}
              disabled={disabled}
            />
          )}
        </div>
      );

    case 'bollinger_position':
    case 'bollinger_cross':
      return (
        <div className="grid grid-cols-2 gap-1.5">
          <NumberInput
            label="Window"
            value={params.window as number}
            onChange={(v) => updateParam('window', v)}
            defaultValue={20}
            disabled={disabled}
          />
          <NumberInput
            label="Alpha"
            value={params.alpha as number}
            onChange={(v) => updateParam('alpha', v)}
            step="0.1"
            defaultValue={2.0}
            disabled={disabled}
          />
          <div className="space-y-1">
            <Label className="text-[10px] text-muted-foreground">Level</Label>
            <Select
              value={params.level as string}
              onValueChange={(v) => updateParam('level', v)}
              disabled={disabled}
            >
              <SelectTrigger className="h-7 text-xs">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="upper" className="text-xs">Upper</SelectItem>
                <SelectItem value="middle" className="text-xs">Middle</SelectItem>
                <SelectItem value="lower" className="text-xs">Lower</SelectItem>
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-1">
            <Label className="text-[10px] text-muted-foreground">Direction</Label>
            <Select
              value={params.direction as string}
              onValueChange={(v) => updateParam('direction', v)}
              disabled={disabled}
            >
              <SelectTrigger className="h-7 text-xs">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="below" className="text-xs">Below</SelectItem>
                <SelectItem value="above" className="text-xs">Above</SelectItem>
              </SelectContent>
            </Select>
          </div>
          {type === 'bollinger_cross' && (
            <NumberInput
              label="Lookback"
              value={params.lookback_days as number}
              onChange={(v) => updateParam('lookback_days', v)}
              defaultValue={1}
              disabled={disabled}
            />
          )}
        </div>
      );

    case 'volatility_percentile':
      return (
        <div className="grid grid-cols-2 gap-1.5">
          <NumberInput
            label="Window"
            value={params.window as number}
            onChange={(v) => updateParam('window', v)}
            defaultValue={20}
            disabled={disabled}
          />
          <NumberInput
            label="Lookback"
            value={params.lookback as number}
            onChange={(v) => updateParam('lookback', v)}
            defaultValue={252}
            disabled={disabled}
          />
          <NumberInput
            label="Percentile"
            value={params.percentile as number}
            onChange={(v) => updateParam('percentile', v)}
            step="1"
            defaultValue={50}
            disabled={disabled}
          />
        </div>
      );

    case 'volume_ratio_above':
    case 'volume_ratio_below':
      return (
        <div className="grid grid-cols-2 gap-1.5">
          <NumberInput
            label="Threshold"
            value={params.ratio_threshold as number}
            onChange={(v) => updateParam('ratio_threshold', v)}
            step="0.1"
            defaultValue={type === 'volume_ratio_above' ? 1.5 : 0.7}
            disabled={disabled}
          />
          <NumberInput
            label="Short Period"
            value={params.short_period as number}
            onChange={(v) => updateParam('short_period', v)}
            defaultValue={20}
            disabled={disabled}
          />
          <NumberInput
            label="Long Period"
            value={params.long_period as number}
            onChange={(v) => updateParam('long_period', v)}
            defaultValue={100}
            disabled={disabled}
          />
          <div className="space-y-1">
            <Label className="text-[10px] text-muted-foreground">MA</Label>
            <Select
              value={params.ma_type as string}
              onValueChange={(v) => updateParam('ma_type', v)}
              disabled={disabled}
            >
              <SelectTrigger className="h-7 text-xs">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="sma" className="text-xs">SMA</SelectItem>
                <SelectItem value="ema" className="text-xs">EMA</SelectItem>
              </SelectContent>
            </Select>
          </div>
        </div>
      );

    case 'buy_and_hold':
      return <div className="text-xs text-muted-foreground">No parameters</div>;

    case 'trading_value':
      return (
        <div className="grid grid-cols-2 gap-1.5">
          <NumberInput
            label="Period"
            value={params.period as number}
            onChange={(v) => updateParam('period', v)}
            defaultValue={15}
            disabled={disabled}
          />
          <NumberInput
            label="Threshold (M)"
            value={(params.threshold_value as number) / 1000000}
            onChange={(v) => updateParam('threshold_value', v * 1000000)}
            defaultValue={100}
            disabled={disabled}
          />
        </div>
      );

    default:
      return (
        <div className="text-xs text-muted-foreground">
          Parameters: {JSON.stringify(params, null, 2)}
        </div>
      );
  }
}
