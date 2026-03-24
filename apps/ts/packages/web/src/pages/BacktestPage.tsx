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
import { NavRail, SplitLayout, SplitMain, SplitSidebar } from '@/components/Layout/Workspace';
import { useBacktestRouteState, useMigrateBacktestRouteState } from '@/hooks/usePageRouteState';
import type { BacktestSubTab } from '@/types/backtest';

const subTabs = [
  { value: 'runner' as BacktestSubTab, label: 'Runner', icon: Play },
  { value: 'results' as BacktestSubTab, label: 'Results', icon: BarChart3 },
  { value: 'attribution' as BacktestSubTab, label: 'Attribution', icon: GitBranch },
  { value: 'strategies' as BacktestSubTab, label: 'Strategies', icon: Code },
  { value: 'status' as BacktestSubTab, label: 'Status', icon: Activity },
  { value: 'dataset' as BacktestSubTab, label: 'Dataset', icon: Database },
  { value: 'lab' as BacktestSubTab, label: 'Lab', icon: FlaskConical },
];

export function BacktestPage() {
  useMigrateBacktestRouteState();
  const {
    activeSubTab,
    setActiveSubTab,
    selectedStrategy,
    setSelectedStrategy,
    setSelectedResultJobId,
    activeLabType,
    setActiveLabType,
  } = useBacktestRouteState();

  return (
    <SplitLayout className="h-full gap-0">
      <SplitSidebar className="w-48 border-r border-border/30 glass-panel">
        <NavRail items={subTabs} value={activeSubTab} onChange={setActiveSubTab} className="p-3" />
      </SplitSidebar>

      <SplitMain className="overflow-auto p-6">
        {activeSubTab === 'runner' && (
          <div className="max-w-xl">
            <BacktestRunner selectedStrategy={selectedStrategy} onSelectedStrategyChange={setSelectedStrategy} />
          </div>
        )}
        {activeSubTab === 'results' && <BacktestResults />}
        {activeSubTab === 'attribution' && (
          <BacktestAttribution selectedStrategy={selectedStrategy} onSelectedStrategyChange={setSelectedStrategy} />
        )}
        {activeSubTab === 'strategies' && <BacktestStrategies />}
        {activeSubTab === 'status' && (
          <BacktestStatus
            onViewJob={(jobId) => {
              setSelectedResultJobId(jobId);
              setActiveSubTab('results');
            }}
          />
        )}
        {activeSubTab === 'dataset' && <DatasetManager />}
        {activeSubTab === 'lab' && (
          <LabPanel
            selectedStrategy={selectedStrategy}
            onSelectedStrategyChange={setSelectedStrategy}
            operation={activeLabType ?? 'generate'}
            onOperationChange={setActiveLabType}
          />
        )}
      </SplitMain>
    </SplitLayout>
  );
}
