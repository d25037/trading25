import { useState } from 'react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import type { LabEvolveRequest } from '@/types/backtest';

interface LabEvolveFormProps {
  strategyName: string | null;
  onSubmit: (request: LabEvolveRequest) => void;
  disabled?: boolean;
}

export function LabEvolveForm({ strategyName, onSubmit, disabled }: LabEvolveFormProps) {
  const [generations, setGenerations] = useState('10');
  const [population, setPopulation] = useState('20');

  const handleSubmit = () => {
    if (!strategyName) return;
    onSubmit({
      strategy_name: strategyName,
      generations: Number(generations) || 10,
      population: Number(population) || 20,
    });
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

      <Button className="w-full" onClick={handleSubmit} disabled={disabled || !strategyName}>
        Start Evolution
      </Button>
    </div>
  );
}
