import { useState } from 'react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Switch } from '@/components/ui/switch';
import type { LabEvolveRequest } from '@/types/backtest';

type CategoryScope = 'all' | 'fundamental';

function resolveCategoryScope(value: string): CategoryScope {
  return value === 'fundamental' ? 'fundamental' : 'all';
}

interface LabEvolveFormProps {
  strategyName: string | null;
  onSubmit: (request: LabEvolveRequest) => void;
  disabled?: boolean;
}

export function LabEvolveForm({ strategyName, onSubmit, disabled }: LabEvolveFormProps) {
  const [generations, setGenerations] = useState('10');
  const [population, setPopulation] = useState('20');
  const [entryFilterOnly, setEntryFilterOnly] = useState(false);
  const [categoryScope, setCategoryScope] = useState<CategoryScope>('all');

  const handleSubmit = () => {
    if (!strategyName) return;
    const request: LabEvolveRequest = {
      strategy_name: strategyName,
      generations: Number(generations) || 10,
      population: Number(population) || 20,
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
            min={4}
            max={200}
            value={population}
            onChange={(e) => setPopulation(e.target.value)}
            disabled={disabled}
          />
        </div>
      </div>

      <div className="flex items-center justify-between">
        <Label htmlFor="evolve-entry-only" className="text-xs">
          Entry Filter Only
        </Label>
        <Switch
          id="evolve-entry-only"
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
        Start Evolution
      </Button>
    </div>
  );
}
