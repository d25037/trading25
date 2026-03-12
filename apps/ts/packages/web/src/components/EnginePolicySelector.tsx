import type { EnginePolicy, EnginePolicyMode } from '@/types/backtest';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';

interface EnginePolicySelectorProps {
  mode: EnginePolicyMode;
  onModeChange: (mode: EnginePolicyMode) => void;
  verificationTopK: string;
  onVerificationTopKChange: (value: string) => void;
  disabled?: boolean;
}

export function EnginePolicySelector({
  mode,
  onModeChange,
  verificationTopK,
  onVerificationTopKChange,
  disabled,
}: EnginePolicySelectorProps) {
  return (
    <div className="space-y-3">
      <div className="space-y-1.5">
        <Label className="text-xs">Engine Policy</Label>
        <Select value={mode} onValueChange={(value) => onModeChange(value as EnginePolicyMode)} disabled={disabled}>
          <SelectTrigger>
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="fast_only">Fast only</SelectItem>
            <SelectItem value="fast_then_verify">Fast + Nautilus verify</SelectItem>
          </SelectContent>
        </Select>
      </div>

      <div className="space-y-1.5">
        <Label className="text-xs">Top K</Label>
        <Input
          type="number"
          min={1}
          max={10}
          value={verificationTopK}
          onChange={(event) => onVerificationTopKChange(event.target.value)}
          disabled={disabled || mode === 'fast_only'}
        />
        <p className="text-[11px] text-muted-foreground">
          Only used when verification is enabled. Valid range is 1-10.
        </p>
      </div>
    </div>
  );
}

export function buildEnginePolicy(mode: EnginePolicyMode, verificationTopK: string): EnginePolicy {
  const policy: EnginePolicy = { mode };
  if (mode === 'fast_then_verify') {
    const parsedTopK = Number.parseInt(verificationTopK, 10);
    policy.verification_top_k = Number.isFinite(parsedTopK)
      ? Math.min(10, Math.max(1, parsedTopK))
      : 5;
  }
  return policy;
}
