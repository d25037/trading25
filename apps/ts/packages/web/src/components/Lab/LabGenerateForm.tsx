import type { LabGenerateRequest } from '@trading25/api-clients/backtest';
import { useState } from 'react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Switch } from '@/components/ui/switch';

type UniversePresetSelection = 'default' | 'prime' | 'standard' | 'growth' | 'topix100' | 'primeExTopix500';

interface LabGenerateFormProps {
  onSubmit: (request: LabGenerateRequest) => void;
  disabled?: boolean;
}

export function LabGenerateForm({ onSubmit, disabled }: LabGenerateFormProps) {
  const [count, setCount] = useState('10');
  const [top, setTop] = useState('5');
  const [direction, setDirection] = useState<'longonly' | 'shortonly' | 'both'>('longonly');
  const [timeframe, setTimeframe] = useState<'daily' | 'weekly'>('daily');
  const [universePreset, setUniversePreset] = useState<UniversePresetSelection>('default');
  const [entryFilterOnly, setEntryFilterOnly] = useState(false);
  const [categoryScope, setCategoryScope] = useState<'all' | 'fundamental'>('all');

  const handleSubmit = () => {
    const request: LabGenerateRequest = {
      count: Number(count) || 10,
      top: Number(top) || 5,
      direction,
      timeframe,
      entry_filter_only: entryFilterOnly,
      save: true,
    };
    if (universePreset !== 'default') {
      request.universe_preset = universePreset;
    }
    if (categoryScope === 'fundamental') {
      request.allowed_categories = ['fundamental'];
    }
    onSubmit(request);
  };

  return (
    <div className="space-y-3">
      <div className="grid grid-cols-2 gap-3">
        <div className="space-y-1.5">
          <Label htmlFor="gen-count" className="text-xs">
            Count
          </Label>
          <Input
            id="gen-count"
            type="number"
            min={1}
            max={100}
            value={count}
            onChange={(e) => setCount(e.target.value)}
            disabled={disabled}
          />
        </div>
        <div className="space-y-1.5">
          <Label htmlFor="gen-top" className="text-xs">
            Top
          </Label>
          <Input
            id="gen-top"
            type="number"
            min={1}
            max={100}
            value={top}
            onChange={(e) => setTop(e.target.value)}
            disabled={disabled}
          />
        </div>
      </div>

      <div className="grid grid-cols-2 gap-3">
        <div className="space-y-1.5">
          <Label className="text-xs">Direction</Label>
          <Select value={direction} onValueChange={(v) => setDirection(v as typeof direction)} disabled={disabled}>
            <SelectTrigger>
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="longonly">Long Only</SelectItem>
              <SelectItem value="shortonly">Short Only</SelectItem>
              <SelectItem value="both">Both</SelectItem>
            </SelectContent>
          </Select>
        </div>
        <div className="space-y-1.5">
          <Label className="text-xs">Timeframe</Label>
          <Select
            value={timeframe}
            onValueChange={(value) => setTimeframe(value as typeof timeframe)}
            disabled={disabled}
          >
            <SelectTrigger>
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="daily">Daily</SelectItem>
              <SelectItem value="weekly">Weekly</SelectItem>
            </SelectContent>
          </Select>
        </div>
      </div>

      <div className="space-y-1.5">
        <Label className="text-xs">Universe Preset</Label>
        <Select
          value={universePreset}
          onValueChange={(value) => setUniversePreset(value as UniversePresetSelection)}
          disabled={disabled}
        >
          <SelectTrigger>
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="default">Backend default</SelectItem>
            <SelectItem value="prime">Prime</SelectItem>
            <SelectItem value="standard">Standard</SelectItem>
            <SelectItem value="growth">Growth</SelectItem>
            <SelectItem value="topix100">TOPIX100</SelectItem>
            <SelectItem value="primeExTopix500">Prime ex TOPIX500</SelectItem>
          </SelectContent>
        </Select>
      </div>

      <div className="flex items-center justify-between">
        <Label htmlFor="gen-entry-only" className="text-xs">
          Entry Filter Only
        </Label>
        <Switch
          id="gen-entry-only"
          checked={entryFilterOnly}
          onCheckedChange={setEntryFilterOnly}
          disabled={disabled}
        />
      </div>

      <div className="space-y-1.5">
        <Label className="text-xs">Allowed Categories</Label>
        <Select
          value={categoryScope}
          onValueChange={(v) => setCategoryScope(v as typeof categoryScope)}
          disabled={disabled}
        >
          <SelectTrigger>
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All</SelectItem>
            <SelectItem value="fundamental">Fundamental Only</SelectItem>
          </SelectContent>
        </Select>
      </div>

      <Button className="w-full" onClick={handleSubmit} disabled={disabled}>
        Generate Strategies
      </Button>
    </div>
  );
}
