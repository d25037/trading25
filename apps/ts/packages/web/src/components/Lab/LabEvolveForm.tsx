import { useState } from 'react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import type { LabEvolveRequest } from '@/types/backtest';

type CategoryScope = 'all' | 'fundamental';
type TargetScope = 'entry_filter_only' | 'exit_trigger_only' | 'both';

function resolveCategoryScope(value: string): CategoryScope {
  return value === 'fundamental' ? 'fundamental' : 'all';
}

function resolveTargetScope(value: string): TargetScope {
  if (value === 'entry_filter_only' || value === 'exit_trigger_only' || value === 'both') return value;
  return 'both';
}

interface LabEvolveFormProps {
  strategyName: string | null;
  onSubmit: (request: LabEvolveRequest) => void;
  disabled?: boolean;
}

export function LabEvolveForm({ strategyName, onSubmit, disabled }: LabEvolveFormProps) {
  const [generations, setGenerations] = useState('10');
  const [population, setPopulation] = useState('20');
  const [targetScope, setTargetScope] = useState<TargetScope>('both');
  const [categoryScope, setCategoryScope] = useState<CategoryScope>('all');
  const [structureMode, setStructureMode] = useState<'params_only' | 'random_add'>('params_only');
  const [randomAddEntrySignals, setRandomAddEntrySignals] = useState('1');
  const [randomAddExitSignals, setRandomAddExitSignals] = useState('1');
  const [seed, setSeed] = useState('');
  const isEntryTargeted = targetScope !== 'exit_trigger_only';
  const isExitTargeted = targetScope !== 'entry_filter_only';

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

    const request: LabEvolveRequest = {
      strategy_name: strategyName,
      generations: parseIntInRange(generations, 10, 1, 100),
      population: parseIntInRange(population, 20, 10, 500),
      structure_mode: structureMode,
      target_scope: targetScope,
    };

    if (structureMode === 'random_add') {
      request.random_add_entry_signals = isEntryTargeted ? parseIntInRange(randomAddEntrySignals, 1, 0, 10) : 0;
      request.random_add_exit_signals = isExitTargeted ? parseIntInRange(randomAddExitSignals, 1, 0, 10) : 0;
      const parsedSeed = parseOptionalInt(seed);
      if (parsedSeed !== undefined) {
        request.seed = parsedSeed;
      }
    }

    if (targetScope === 'entry_filter_only') {
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
          <Label htmlFor="evolve-gen" className="text-xs">
            Generations
          </Label>
          <Input
            id="evolve-gen"
            type="number"
            min={1}
            max={100}
            value={generations}
            onChange={(e) => setGenerations(e.target.value)}
            disabled={disabled}
          />
        </div>
        <div className="space-y-1.5">
          <Label htmlFor="evolve-pop" className="text-xs">
            Population
          </Label>
          <Input
            id="evolve-pop"
            type="number"
            min={10}
            max={500}
            value={population}
            onChange={(e) => setPopulation(e.target.value)}
            disabled={disabled}
          />
        </div>
      </div>

      <div className="space-y-1.5">
        <Label className="text-xs">Target Scope</Label>
        <Select
          value={targetScope}
          onValueChange={(value) => setTargetScope(resolveTargetScope(value))}
          disabled={disabled}
        >
          <SelectTrigger>
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="both">both</SelectItem>
            <SelectItem value="entry_filter_only">entry filter only</SelectItem>
            <SelectItem value="exit_trigger_only">exit trigger only</SelectItem>
          </SelectContent>
        </Select>
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
            <SelectItem value="all">all</SelectItem>
            <SelectItem value="fundamental">fundamental only</SelectItem>
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
            <SelectItem value="params_only">Adjust Existing Signal Params</SelectItem>
            <SelectItem value="random_add">Fix Existing Signals + Add New Signals</SelectItem>
          </SelectContent>
        </Select>
      </div>

      {structureMode === 'random_add' && (
        <div className={`grid gap-3 ${targetScope === 'both' ? 'grid-cols-3' : 'grid-cols-2'}`}>
          {isEntryTargeted && (
            <div className="space-y-1.5">
              <Label htmlFor="evolve-random-entry" className="text-xs">
                Add Entry Signals
              </Label>
              <Input
                id="evolve-random-entry"
                type="number"
                min={0}
                max={10}
                value={randomAddEntrySignals}
                onChange={(e) => setRandomAddEntrySignals(e.target.value)}
                disabled={disabled}
              />
            </div>
          )}
          {isExitTargeted && (
            <div className="space-y-1.5">
              <Label htmlFor="evolve-random-exit" className="text-xs">
                Add Exit Signals
              </Label>
              <Input
                id="evolve-random-exit"
                type="number"
                min={0}
                max={10}
                value={randomAddExitSignals}
                onChange={(e) => setRandomAddExitSignals(e.target.value)}
                disabled={disabled}
              />
            </div>
          )}
          <div className="space-y-1.5">
            <Label htmlFor="evolve-seed" className="text-xs">
              Seed (optional)
            </Label>
            <Input
              id="evolve-seed"
              type="number"
              value={seed}
              onChange={(e) => setSeed(e.target.value)}
              disabled={disabled}
            />
          </div>
        </div>
      )}

      <Button className="w-full" onClick={handleSubmit} disabled={disabled || !strategyName}>
        Start Evolution
      </Button>
    </div>
  );
}
