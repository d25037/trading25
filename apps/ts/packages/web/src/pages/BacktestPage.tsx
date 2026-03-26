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
import {
  NavRail,
  PageIntroMetaList,
  SectionEyebrow,
  SplitLayout,
  SplitMain,
  SplitSidebar,
  Surface,
} from '@/components/Layout/Workspace';
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

const BACKTEST_VIEW_META: Record<BacktestSubTab, { label: string; focus: string }> = {
  runner: { label: 'Runner', focus: 'Run status first' },
  results: { label: 'Results', focus: 'Report review' },
  attribution: { label: 'Attribution', focus: 'Signal diagnostics' },
  strategies: { label: 'Strategies', focus: 'Editor and config' },
  status: { label: 'Status', focus: 'Job monitoring' },
  dataset: { label: 'Dataset', focus: 'Snapshot operations' },
  lab: { label: 'Lab', focus: 'Generate and evolve' },
};

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

  const activeViewMeta = BACKTEST_VIEW_META[activeSubTab];
  const selectedStrategyLabel = selectedStrategy ? (selectedStrategy.split('/').pop() ?? selectedStrategy) : 'None selected';
  const introMetaItems = [
    { label: 'View', value: activeViewMeta.label },
    { label: 'Strategy', value: selectedStrategyLabel },
    { label: 'Focus', value: activeViewMeta.focus },
  ];

  return (
    <div className="flex min-h-0 flex-1 flex-col gap-3 overflow-y-auto p-4 lg:overflow-hidden">
      <Surface className="px-4 py-3">
        <div className="flex flex-col gap-4 xl:flex-row xl:items-end xl:justify-between">
          <div className="space-y-2">
            <SectionEyebrow>Analyst Desk</SectionEyebrow>
            <div className="space-y-1">
              <h1 className="text-2xl font-semibold tracking-tight text-foreground">Backtest</h1>
              <p className="max-w-2xl text-sm text-muted-foreground">
                Run, inspect, and refine strategies with execution status and result review kept ahead of setup chrome.
              </p>
            </div>
          </div>
          <PageIntroMetaList items={introMetaItems} className="gap-x-4 gap-y-2" />
        </div>
      </Surface>

      <SplitLayout className="min-h-0 flex-1 flex-col gap-3 lg:flex-row lg:items-stretch">
        <SplitSidebar className="w-full lg:h-full lg:w-44 lg:overflow-auto xl:w-48">
          <Surface className="flex h-full min-h-[20rem] flex-col overflow-hidden p-3">
            <div className="space-y-1 border-b border-border/70 pb-3">
              <SectionEyebrow>Workspace</SectionEyebrow>
              <h2 className="text-sm font-semibold text-foreground">Backtest Views</h2>
              <p className="text-xs text-muted-foreground">
                Switch between running, reviewing, editing, and monitoring without leaving the desk.
              </p>
            </div>

            <div className="min-h-0 flex-1 pt-3">
              <NavRail items={subTabs} value={activeSubTab} onChange={setActiveSubTab} className="h-full" />
            </div>
          </Surface>
        </SplitSidebar>

        <SplitMain className="gap-3 lg:overflow-y-auto lg:pr-1">
          {activeSubTab === 'runner' && (
            <BacktestRunner selectedStrategy={selectedStrategy} onSelectedStrategyChange={setSelectedStrategy} />
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
    </div>
  );
}
