import { Activity, BarChart3, Code, Database, FlaskConical, GitBranch, Play } from 'lucide-react';
import {
  BacktestAttribution,
  BacktestResults,
  BacktestRunner,
  BacktestStatus,
  BacktestStrategies,
  DatasetManager,
} from '@/components/Backtest';
import { LabPanel } from '@/components/Lab';
import { cn } from '@/lib/utils';
import { useBacktestStore } from '@/stores/backtestStore';
import type { BacktestSubTab } from '@/types/backtest';

const subTabs: { id: BacktestSubTab; label: string; icon: typeof Play }[] = [
  { id: 'runner', label: 'Runner', icon: Play },
  { id: 'results', label: 'Results', icon: BarChart3 },
  { id: 'attribution', label: 'Attribution', icon: GitBranch },
  { id: 'strategies', label: 'Strategies', icon: Code },
  { id: 'status', label: 'Status', icon: Activity },
  { id: 'dataset', label: 'Dataset', icon: Database },
  { id: 'lab', label: 'Lab', icon: FlaskConical },
];

export function BacktestPage() {
  const { activeSubTab, setActiveSubTab } = useBacktestStore();

  return (
    <div className="flex">
      {/* Sidebar */}
      <div className={cn('w-48 shrink-0 border-r border-border/30', 'glass-panel')}>
        <nav className="flex flex-col gap-1 p-3">
          {subTabs.map((tab) => {
            const Icon = tab.icon;
            const isActive = activeSubTab === tab.id;
            return (
              <button
                key={tab.id}
                type="button"
                className={cn(
                  'flex items-center gap-2 rounded-md px-3 py-2 text-sm transition-colors',
                  isActive
                    ? 'bg-primary/10 text-primary font-medium'
                    : 'text-muted-foreground hover:bg-accent hover:text-foreground'
                )}
                onClick={() => setActiveSubTab(tab.id)}
              >
                <Icon className="h-4 w-4 shrink-0" />
                {tab.label}
              </button>
            );
          })}
        </nav>
      </div>

      {/* Main Content */}
      <div className="flex-1 p-6">
        {activeSubTab === 'runner' && (
          <div className="max-w-xl">
            <BacktestRunner />
          </div>
        )}
        {activeSubTab === 'results' && <BacktestResults />}
        {activeSubTab === 'attribution' && <BacktestAttribution />}
        {activeSubTab === 'strategies' && <BacktestStrategies />}
        {activeSubTab === 'status' && <BacktestStatus />}
        {activeSubTab === 'dataset' && <DatasetManager />}
        {activeSubTab === 'lab' && <LabPanel />}
      </div>
    </div>
  );
}
