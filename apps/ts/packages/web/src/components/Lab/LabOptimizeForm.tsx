import { useState } from 'react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import type { LabOptimizeRequest } from '@/types/backtest';

interface LabOptimizeFormProps {
  strategyName: string | null;
  onSubmit: (request: LabOptimizeRequest) => void;
  disabled?: boolean;
}

export function LabOptimizeForm({ strategyName, onSubmit, disabled }: LabOptimizeFormProps) {
  const [trials, setTrials] = useState('50');
  const [sampler, setSampler] = useState('tpe');

  const handleSubmit = () => {
    if (!strategyName) return;
    onSubmit({
      strategy_name: strategyName,
      trials: Number(trials) || 50,
      sampler,
    });
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

      <Button className="w-full" onClick={handleSubmit} disabled={disabled || !strategyName}>
        Start Optimization
      </Button>
    </div>
  );
}
