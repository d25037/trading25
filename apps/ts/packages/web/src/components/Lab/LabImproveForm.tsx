import { useState } from 'react';
import { Button } from '@/components/ui/button';
import { Label } from '@/components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Switch } from '@/components/ui/switch';
import type { LabImproveRequest } from '@/types/backtest';

interface LabImproveFormProps {
  strategyName: string | null;
  onSubmit: (request: LabImproveRequest) => void;
  disabled?: boolean;
}

export function LabImproveForm({ strategyName, onSubmit, disabled }: LabImproveFormProps) {
  const [autoApply, setAutoApply] = useState(false);
  const [entryFilterOnly, setEntryFilterOnly] = useState(false);
  const [categoryScope, setCategoryScope] = useState<'all' | 'fundamental'>('all');

  const handleSubmit = () => {
    if (!strategyName) return;
    const request: LabImproveRequest = {
      strategy_name: strategyName,
      auto_apply: autoApply,
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
      <div className="flex items-center justify-between">
        <Label htmlFor="improve-auto" className="text-xs">
          Auto Apply
        </Label>
        <Switch id="improve-auto" checked={autoApply} onCheckedChange={setAutoApply} disabled={disabled} />
      </div>
      <div className="flex items-center justify-between">
        <Label htmlFor="improve-entry-only" className="text-xs">
          Entry Filter Only
        </Label>
        <Switch
          id="improve-entry-only"
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
      <p className="text-xs text-muted-foreground">
        When enabled, suggested improvements will be automatically applied to the strategy.
      </p>

      <Button className="w-full" onClick={handleSubmit} disabled={disabled || !strategyName}>
        Analyze & Improve
      </Button>
    </div>
  );
}
