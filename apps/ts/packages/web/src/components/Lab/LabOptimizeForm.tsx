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
  const [sampler, setSampler] = useState('tpe');
  const [entryFilterOnly, setEntryFilterOnly] = useState(false);
  const [categoryScope, setCategoryScope] = useState<CategoryScope>('all');

  const handleSubmit = () => {
    if (!strategyName) return;
    const request: LabOptimizeRequest = {
      strategy_name: strategyName,
      trials: Number(trials) || 50,
      sampler,
    };
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
            min={1}
            max={1000}
            value={trials}
            onChange={(e) => setTrials(e.target.value)}
            disabled={disabled}
          />
        </div>
        <div className="space-y-1.5">
          <Label className="text-xs">Sampler</Label>
          <Select value={sampler} onValueChange={setSampler} disabled={disabled}>
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

      <Button className="w-full" onClick={handleSubmit} disabled={disabled || !strategyName}>
        Start Optimization
      </Button>
    </div>
  );
}
