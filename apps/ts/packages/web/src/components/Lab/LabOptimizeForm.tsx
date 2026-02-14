import { useState } from 'react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Switch } from '@/components/ui/switch';
import type { LabOptimizeRequest } from '@/types/backtest';

type CategoryScope = 'all' | 'fundamental';

function resolveCategoryScope(value: string): CategoryScope {
  return value === 'fundamental' ? 'fundamental' : 'all';
}

interface LabOptimizeFormProps {
  strategyName: string | null;
  onSubmit: (request: LabOptimizeRequest) => void;
  disabled?: boolean;
}

export function LabOptimizeForm({ strategyName, onSubmit, disabled }: LabOptimizeFormProps) {
  const [trials, setTrials] = useState('50');
  const [entryFilterOnly, setEntryFilterOnly] = useState(false);
  const [categoryScope, setCategoryScope] = useState<CategoryScope>('all');
  const [sampler, setSampler] = useState<'tpe' | 'random' | 'cmaes'>('tpe');
  const [structureMode, setStructureMode] = useState<'params_only' | 'random_add'>('params_only');
  const [randomAddEntrySignals, setRandomAddEntrySignals] = useState('1');
  const [randomAddExitSignals, setRandomAddExitSignals] = useState('1');
  const [seed, setSeed] = useState('');

  const parseIntInRange = (value: string, defaultValue: number, min: number, max: number) => {
    const parsed = Number.parseInt(value, 10);
    if (!Number.isFinite(parsed)) return defaultValue;
    return Math.min(max, Math.max(min, parsed));
  };

  const parseOptionalInt = (value: string): number | undefined => {
    if (value.trim() === '') return undefined;
    const parsed = Number.parseInt(value, 10);
    return Number.isFinite(parsed) ? parsed : undefined;
  };

  const handleSubmit = () => {
    if (!strategyName) return;

    const request: LabOptimizeRequest = {
      strategy_name: strategyName,
      trials: parseIntInRange(trials, 50, 10, 1000),
      sampler,
      structure_mode: structureMode,
    };

    if (structureMode === 'random_add') {
      request.random_add_entry_signals = parseIntInRange(randomAddEntrySignals, 1, 0, 10);
      request.random_add_exit_signals = parseIntInRange(randomAddExitSignals, 1, 0, 10);
      const parsedSeed = parseOptionalInt(seed);
      if (parsedSeed !== undefined) {
        request.seed = parsedSeed;
      }
    }

    if (entryFilterOnly) {
      request.entry_filter_only = true;
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
          <Label htmlFor="opt-trials" className="text-xs">
            Trials
          </Label>
          <Input
            id="opt-trials"
            type="number"
            min={10}
            max={1000}
            value={trials}
            onChange={(e) => setTrials(e.target.value)}
            disabled={disabled}
          />
        </div>
        <div className="space-y-1.5">
          <Label className="text-xs">Sampler</Label>
          <Select
            value={sampler}
            onValueChange={(value: 'tpe' | 'random' | 'cmaes') => setSampler(value)}
            disabled={disabled}
          >
            <SelectTrigger>
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="tpe">TPE</SelectItem>
              <SelectItem value="cmaes">CMA-ES</SelectItem>
              <SelectItem value="random">Random</SelectItem>
            </SelectContent>
          </Select>
        </div>
      </div>

      <div className="flex items-center justify-between">
        <Label htmlFor="opt-entry-only" className="text-xs">
          Entry Filter Only
        </Label>
        <Switch
          id="opt-entry-only"
          checked={entryFilterOnly}
          onCheckedChange={setEntryFilterOnly}
          disabled={disabled}
        />
      </div>

      <div className="space-y-1.5">
        <Label className="text-xs">Allowed Categories</Label>
        <Select
          value={categoryScope}
          onValueChange={(value) => setCategoryScope(resolveCategoryScope(value))}
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

      <div className="space-y-1.5">
        <Label className="text-xs">Structure Mode</Label>
        <Select
          value={structureMode}
          onValueChange={(value: 'params_only' | 'random_add') => setStructureMode(value)}
          disabled={disabled}
        >
          <SelectTrigger>
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="params_only">Params Only</SelectItem>
            <SelectItem value="random_add">Random Add Signals</SelectItem>
          </SelectContent>
        </Select>
      </div>

      {structureMode === 'random_add' && (
        <div className="grid grid-cols-3 gap-3">
          <div className="space-y-1.5">
            <Label htmlFor="opt-random-entry" className="text-xs">
              Add Entry Signals
            </Label>
            <Input
              id="opt-random-entry"
              type="number"
              min={0}
              max={10}
              value={randomAddEntrySignals}
              onChange={(e) => setRandomAddEntrySignals(e.target.value)}
              disabled={disabled}
            />
          </div>
          <div className="space-y-1.5">
            <Label htmlFor="opt-random-exit" className="text-xs">
              Add Exit Signals
            </Label>
            <Input
              id="opt-random-exit"
              type="number"
              min={0}
              max={10}
              value={randomAddExitSignals}
              onChange={(e) => setRandomAddExitSignals(e.target.value)}
              disabled={disabled}
            />
          </div>
          <div className="space-y-1.5">
            <Label htmlFor="opt-seed" className="text-xs">
              Seed (optional)
            </Label>
            <Input
              id="opt-seed"
              type="number"
              value={seed}
              onChange={(e) => setSeed(e.target.value)}
              disabled={disabled}
            />
          </div>
        </div>
      )}

      <Button className="w-full" onClick={handleSubmit} disabled={disabled || !strategyName}>
        Start Optimization
      </Button>
    </div>
  );
}
