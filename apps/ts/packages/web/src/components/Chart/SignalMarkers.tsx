import { Zap } from 'lucide-react';
import { useCallback, useId, useMemo } from 'react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Switch } from '@/components/ui/switch';
import { useSignalReference } from '@/hooks/useBacktest';
import { useChartStore, type SignalConfig } from '@/stores/chartStore';
import type { SignalDefinition, SignalFieldDefinition } from '@/types/backtest';
import { IndicatorToggle, NumberInput } from './IndicatorToggle';

interface SignalOverlayControlsProps {
  disabled?: boolean;
}

function getDefaultParams(definition: SignalDefinition): Record<string, number | string | boolean> {
  const result: Record<string, number | string | boolean> = {};
  for (const field of definition.fields) {
    if (field.name === 'enabled') continue;
    if (typeof field.default === 'number' || typeof field.default === 'string' || typeof field.default === 'boolean') {
      result[field.name] = field.default;
    }
  }
  return result;
}

function isFloatDefault(field: SignalFieldDefinition): boolean {
  return typeof field.default === 'number' && !Number.isInteger(field.default);
}

export function SignalOverlayControls({ disabled }: SignalOverlayControlsProps) {
  const { settings, toggleSignalOverlay, addSignal, removeSignal, updateSignal, toggleSignal } = useChartStore();
  const { data } = useSignalReference();
  const { relativeMode } = settings;
  const signalOverlay = settings.signalOverlay ?? { enabled: false, signals: [] };
  const signalOverlayId = useId();

  const definitions = useMemo(() => data?.signals ?? [], [data?.signals]);
  const definitionsByType = useMemo(
    () => new Map(definitions.map((definition) => [definition.signal_type, definition])),
    [definitions]
  );
  const categories = useMemo(() => data?.categories ?? [], [data?.categories]);

  const availableDefinitions = useMemo(
    () =>
      definitions.filter(
        (definition) =>
          definition.chart?.supported !== false &&
          !signalOverlay.signals.some((signal) => signal.type === definition.signal_type)
      ),
    [definitions, signalOverlay.signals]
  );

  const handleAddSignal = useCallback(
    (signalType: string) => {
      const definition = definitionsByType.get(signalType);
      if (!definition) return;
      addSignal({
        type: signalType,
        params: getDefaultParams(definition),
        mode: definition.chart?.supported_modes?.includes('entry') ? 'entry' : 'exit',
        enabled: true,
      });
    },
    [addSignal, definitionsByType]
  );

  const isSignalDisabledInRelativeMode = (definition: SignalDefinition | undefined) =>
    relativeMode && definition?.chart?.supports_relative_mode === false;

  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between p-2 rounded glass-panel">
        <div className="flex items-center gap-1.5">
          <Zap className="h-3.5 w-3.5 text-muted-foreground" />
          <Label htmlFor={signalOverlayId} className="text-xs font-medium cursor-pointer">
            Signal Overlay
          </Label>
        </div>
        <Switch id={signalOverlayId} checked={signalOverlay.enabled} onCheckedChange={toggleSignalOverlay} disabled={disabled} />
      </div>

      {signalOverlay.enabled && (
        <div className="space-y-2 pl-2">
          {availableDefinitions.length > 0 && (
            <div className="flex gap-2">
              <Select onValueChange={handleAddSignal}>
                <SelectTrigger className="h-8 text-xs glass-panel border-border/30 flex-1">
                  <SelectValue placeholder="Add signal..." />
                </SelectTrigger>
                <SelectContent className="bg-background/95 backdrop-blur-md border-border shadow-xl max-h-64">
                  {categories.map((category) => {
                    const availableInCategory = availableDefinitions.filter(
                      (definition) => definition.category === category.key
                    );
                    if (availableInCategory.length === 0) return null;
                    return (
                      <div key={category.key}>
                        <div className="px-2 py-1 text-xs font-semibold text-muted-foreground">{category.label}</div>
                        {availableInCategory.map((definition) => (
                          <SelectItem
                            key={definition.signal_type}
                            value={definition.signal_type}
                            className="text-xs"
                            disabled={isSignalDisabledInRelativeMode(definition)}
                          >
                            {definition.name}
                            {isSignalDisabledInRelativeMode(definition) && ' (disabled in relative mode)'}
                          </SelectItem>
                        ))}
                      </div>
                    );
                  })}
                </SelectContent>
              </Select>
            </div>
          )}

          {signalOverlay.signals.map((signal) => {
            const definition = definitionsByType.get(signal.type);
            return (
              <SignalConfigPanel
                key={signal.type}
                signal={signal}
                definition={definition}
                disabled={disabled || isSignalDisabledInRelativeMode(definition)}
                onToggle={() => toggleSignal(signal.type)}
                onUpdate={(updates) => updateSignal(signal.type, updates)}
                onRemove={() => removeSignal(signal.type)}
              />
            );
          })}

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
  definition?: SignalDefinition;
  disabled?: boolean;
  onToggle: () => void;
  onUpdate: (updates: Partial<SignalConfig>) => void;
  onRemove: () => void;
}

function SignalConfigPanel({ signal, definition, disabled, onToggle, onUpdate, onRemove }: SignalConfigPanelProps) {
  const label = definition?.name ?? signal.type;
  const supportedModes = definition?.chart?.supported_modes ?? ['entry', 'exit'];

  return (
    <IndicatorToggle label={label} enabled={signal.enabled} onToggle={onToggle} disabled={disabled}>
      <div className="space-y-2">
        <div className="flex items-center gap-2">
          <Label className="text-xs w-12">Mode</Label>
          <Select value={signal.mode} onValueChange={(value: 'entry' | 'exit') => onUpdate({ mode: value })} disabled={disabled}>
            <SelectTrigger className="h-7 text-xs flex-1">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="entry" className="text-xs" disabled={!supportedModes.includes('entry')}>
                Entry
              </SelectItem>
              <SelectItem value="exit" className="text-xs" disabled={!supportedModes.includes('exit')}>
                Exit
              </SelectItem>
            </SelectContent>
          </Select>
        </div>

        <SignalParamsEditor
          definition={definition}
          params={signal.params}
          onUpdate={(params) => onUpdate({ params })}
          disabled={disabled}
        />

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
  definition?: SignalDefinition;
  params: Record<string, number | string | boolean>;
  onUpdate: (params: Record<string, number | string | boolean>) => void;
  disabled?: boolean;
}

function SignalParamsEditor({ definition, params, onUpdate, disabled }: SignalParamsEditorProps) {
  if (!definition) {
    return <div className="text-xs text-muted-foreground">Signal metadata unavailable.</div>;
  }

  const updateParam = (key: string, value: number | string | boolean) => {
    onUpdate({ ...params, [key]: value });
  };

  return (
    <div className="grid grid-cols-2 gap-1.5">
      {definition.fields
        .filter((field) => field.name !== 'enabled')
        .map((field) => {
          const currentValue = params[field.name] ?? field.default;

          if (field.type === 'number') {
            return (
              <NumberInput
                key={field.name}
                label={field.name}
                value={typeof currentValue === 'number' ? currentValue : Number(field.default ?? 0)}
                onChange={(value) => updateParam(field.name, value)}
                step={isFloatDefault(field) ? '0.1' : undefined}
                defaultValue={typeof field.default === 'number' ? field.default : 0}
                disabled={disabled}
              />
            );
          }

          if (field.type === 'boolean') {
            return (
              <div key={field.name} className="col-span-2 flex items-center justify-between rounded border border-border/40 px-2 py-1.5">
                <Label className="text-xs">{field.name}</Label>
                <Switch
                  checked={Boolean(currentValue)}
                  onCheckedChange={(checked) => updateParam(field.name, checked)}
                  disabled={disabled}
                />
              </div>
            );
          }

          if (field.type === 'select' && field.options) {
            return (
              <div key={field.name} className="space-y-1">
                <Label className="text-[10px] text-muted-foreground">{field.name}</Label>
                <Select
                  value={typeof currentValue === 'string' ? currentValue : String(field.default ?? field.options[0] ?? '')}
                  onValueChange={(value) => updateParam(field.name, value)}
                  disabled={disabled}
                >
                  <SelectTrigger className="h-7 text-xs">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {field.options.map((option) => (
                      <SelectItem key={option} value={option} className="text-xs">
                        {option}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
            );
          }

          return (
            <div key={field.name} className="space-y-1">
              <Label className="text-[10px] text-muted-foreground">{field.name}</Label>
              <Input
                value={typeof currentValue === 'string' ? currentValue : String(currentValue ?? '')}
                onChange={(event) => updateParam(field.name, event.target.value)}
                className="h-7 text-xs"
                disabled={disabled}
              />
            </div>
          );
        })}
    </div>
  );
}
