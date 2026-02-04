import { useState } from 'react';
import { cn } from '@/lib/utils';
import { HtmlFileBrowser } from './HtmlFileBrowser';
import { OptimizationHtmlFileBrowser } from './OptimizationHtmlFileBrowser';

type ResultTab = 'backtest' | 'optimization';

export function BacktestResults() {
  const [activeTab, setActiveTab] = useState<ResultTab>('backtest');

  return (
    <div className="space-y-4">
      {/* Sub-tab navigation */}
      <div className="flex border-b">
        <button
          type="button"
          onClick={() => setActiveTab('backtest')}
          className={cn(
            'px-4 py-2 text-sm font-medium border-b-2 transition-colors',
            activeTab === 'backtest'
              ? 'border-primary text-primary'
              : 'border-transparent text-muted-foreground hover:text-foreground'
          )}
        >
          Backtest
        </button>
        <button
          type="button"
          onClick={() => setActiveTab('optimization')}
          className={cn(
            'px-4 py-2 text-sm font-medium border-b-2 transition-colors',
            activeTab === 'optimization'
              ? 'border-primary text-primary'
              : 'border-transparent text-muted-foreground hover:text-foreground'
          )}
        >
          Optimization
        </button>
      </div>

      {activeTab === 'backtest' ? <HtmlFileBrowser /> : <OptimizationHtmlFileBrowser />}
    </div>
  );
}
