import { useState } from 'react';
import { Button } from '@/components/ui/button';
import { Label } from '@/components/ui/label';
import { Switch } from '@/components/ui/switch';
import type { LabImproveRequest } from '@/types/backtest';

interface LabImproveFormProps {
  strategyName: string | null;
  onSubmit: (request: LabImproveRequest) => void;
  disabled?: boolean;
}

export function LabImproveForm({ strategyName, onSubmit, disabled }: LabImproveFormProps) {
  const [autoApply, setAutoApply] = useState(false);

  const handleSubmit = () => {
    if (!strategyName) return;
    onSubmit({
      strategy_name: strategyName,
      auto_apply: autoApply,
    });
  };

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <Label htmlFor="improve-auto" className="text-xs">
          Auto Apply
        </Label>
        <Switch id="improve-auto" checked={autoApply} onCheckedChange={setAutoApply} disabled={disabled} />
      </div>
      <p className="text-xs text-muted-foreground">
        When enabled, suggested improvements will be automatically applied to the strategy.
      </p>

      <Button className="w-full" onClick={handleSubmit} disabled={disabled || !strategyName}>
        Analyze & Improve
      </Button>
    </div>
  );
}
