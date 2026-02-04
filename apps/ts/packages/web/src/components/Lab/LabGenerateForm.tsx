import { useState } from 'react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import type { LabGenerateRequest } from '@/types/backtest';

interface LabGenerateFormProps {
  onSubmit: (request: LabGenerateRequest) => void;
  disabled?: boolean;
}

export function LabGenerateForm({ onSubmit, disabled }: LabGenerateFormProps) {
  const [count, setCount] = useState('10');
  const [top, setTop] = useState('5');
  const [direction, setDirection] = useState<'longonly' | 'shortonly' | 'both'>('longonly');
  const [timeframe, setTimeframe] = useState('daily');
  const [dataset, setDataset] = useState('');

  const handleSubmit = () => {
    const request: LabGenerateRequest = {
      count: Number(count) || 10,
      top: Number(top) || 5,
      direction,
      timeframe,
    };
    if (dataset.trim()) {
      request.dataset = dataset.trim();
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
          <Select value={timeframe} onValueChange={setTimeframe} disabled={disabled}>
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
        <Label htmlFor="gen-dataset" className="text-xs">
          Dataset (optional)
        </Label>
        <Input
          id="gen-dataset"
          placeholder="e.g., prime.db"
          value={dataset}
          onChange={(e) => setDataset(e.target.value)}
          disabled={disabled}
        />
      </div>

      <Button className="w-full" onClick={handleSubmit} disabled={disabled}>
        Generate Strategies
      </Button>
    </div>
  );
}
