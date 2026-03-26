import { useState } from 'react';
import { SectionEyebrow, SegmentedTabs, Surface } from '@/components/Layout/Workspace';
import { HtmlFileBrowser } from './HtmlFileBrowser';
import { OptimizationHtmlFileBrowser } from './OptimizationHtmlFileBrowser';

type ResultTab = 'backtest' | 'optimization';

const resultTabs = [
  { value: 'backtest' as ResultTab, label: 'Backtest' },
  { value: 'optimization' as ResultTab, label: 'Optimization' },
];

export function BacktestResults() {
  const [activeTab, setActiveTab] = useState<ResultTab>('backtest');
  const description =
    activeTab === 'optimization'
      ? 'Review optimization artifacts and keep candidate analysis in the foreground.'
      : 'Review generated HTML reports and keep file history paired with the active preview.';

  return (
    <div className="space-y-3">
      <Surface className="px-4 py-4 sm:px-5">
        <div className="flex flex-col gap-4 xl:flex-row xl:items-end xl:justify-between">
          <div className="space-y-2">
            <SectionEyebrow>Result Workspace</SectionEyebrow>
            <div className="space-y-1">
              <h2 className="text-lg font-semibold tracking-tight text-foreground">Backtest Results</h2>
              <p className="max-w-2xl text-sm text-muted-foreground">{description}</p>
            </div>
          </div>
          <SegmentedTabs items={resultTabs} value={activeTab} onChange={setActiveTab} />
        </div>
      </Surface>

      {activeTab === 'backtest' ? <HtmlFileBrowser /> : <OptimizationHtmlFileBrowser />}
    </div>
  );
}
