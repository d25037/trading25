import { InfoPopover } from '@/components/ui/info-popover';
import { getRankingPresetDescription, RANKING_PRESET_OPTIONS } from './rankingState';

interface RankingPresetInfoButtonProps {
  className?: string;
  panelClassName?: string;
}

export function RankingPresetInfoButton({ className, panelClassName }: RankingPresetInfoButtonProps) {
  const presets = RANKING_PRESET_OPTIONS.filter((option) => option.value !== 'custom');

  return (
    <InfoPopover ariaLabel="Show preset conditions" className={className} contentClassName={panelClassName}>
      <div className="max-h-[22rem] space-y-2 overflow-auto pr-1">
        {presets.map((preset) => (
          <p key={preset.value} className="text-xs leading-snug text-muted-foreground">
            <span className="font-semibold text-foreground">{preset.label}:</span>{' '}
            {getRankingPresetDescription(preset.value).replace(`${preset.label}: `, '')}
          </p>
        ))}
      </div>
    </InfoPopover>
  );
}
