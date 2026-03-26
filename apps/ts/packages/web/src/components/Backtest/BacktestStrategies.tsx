import { ArrowLeftRight, Code, Copy, Edit, FileText, Loader2, Lock, Pencil, Settings2, Trash2 } from 'lucide-react';
import { useState } from 'react';
import { SegmentedTabs, SectionEyebrow, Surface } from '@/components/Layout/Workspace';
import { Button } from '@/components/ui/button';
import { useStrategies, useStrategy } from '@/hooks/useBacktest';
import { cn } from '@/lib/utils';
import type { StrategyMetadata } from '@/types/backtest';
import { DeleteConfirmDialog } from './DeleteConfirmDialog';
import { DuplicateDialog } from './DuplicateDialog';
import { MoveGroupDialog } from './MoveGroupDialog';
import { OptimizationGridEditor } from './OptimizationGridEditor';
import { RenameDialog } from './RenameDialog';
import { StrategyEditor } from './StrategyEditor';
import { compareManagedStrategyCategory, isManagedStrategyCategory } from './strategyCategoryOrder';

function StrategyCard({
  strategy,
  isSelected,
  onClick,
}: {
  strategy: StrategyMetadata;
  isSelected: boolean;
  onClick: () => void;
}) {
  return (
    <button type="button" onClick={onClick} className="w-full text-left">
      <Surface
        className={cn(
          'w-full rounded-2xl border p-4 transition-colors',
          isSelected
            ? 'border-border/70 bg-[var(--app-surface-emphasis)] shadow-sm'
            : 'border-border/60 bg-[var(--app-surface-muted)] hover:bg-[var(--app-surface-emphasis)]'
        )}
      >
        <div className="space-y-3">
          <div className="space-y-2">
            <div className="flex items-center gap-2 text-foreground">
              <Code className="h-4 w-4" />
              <h3 className="truncate text-base font-semibold">{strategy.display_name || strategy.name}</h3>
            </div>
            <p className="text-xs uppercase tracking-[0.14em] text-muted-foreground">{strategy.category}</p>
          </div>
          {strategy.description ? <p className="line-clamp-2 text-sm text-muted-foreground">{strategy.description}</p> : null}
        </div>
      </Surface>
    </button>
  );
}

type DetailTab = 'detail' | 'optimize';

const detailTabs = [
  { value: 'detail' as const, label: 'Detail' },
  { value: 'optimize' as const, label: 'Optimize', icon: Settings2 },
];

function StrategyActionBar({
  canEditYaml,
  canRenameOrDelete,
  isMovable,
  onEdit,
  onRename,
  onDuplicate,
  onMove,
  onDelete,
}: {
  canEditYaml: boolean;
  canRenameOrDelete: boolean;
  isMovable: boolean;
  onEdit: () => void;
  onRename: () => void;
  onDuplicate: () => void;
  onMove: () => void;
  onDelete: () => void;
}) {
  return (
    <div className="flex flex-wrap gap-2">
      {canEditYaml ? (
        <Button variant="outline" size="sm" onClick={onEdit}>
          <Edit className="mr-1 h-4 w-4" />
          Edit
        </Button>
      ) : null}
      {canRenameOrDelete ? (
        <Button variant="outline" size="sm" onClick={onRename}>
          <Pencil className="mr-1 h-4 w-4" />
          Rename
        </Button>
      ) : null}
      <Button variant="outline" size="sm" onClick={onDuplicate}>
        <Copy className="mr-1 h-4 w-4" />
        Duplicate
      </Button>
      {isMovable ? (
        <Button variant="outline" size="sm" onClick={onMove}>
          <ArrowLeftRight className="mr-1 h-4 w-4" />
          Move Group
        </Button>
      ) : null}
      {canRenameOrDelete ? (
        <Button variant="outline" size="sm" className="text-destructive hover:text-destructive" onClick={onDelete}>
          <Trash2 className="mr-1 h-4 w-4" />
          Delete
        </Button>
      ) : null}
    </div>
  );
}

function StrategyMetadataCard({
  title,
  content,
}: {
  title: string;
  content: string | null;
}) {
  if (!content) {
    return null;
  }

  return (
    <div className="rounded-2xl border border-border/70 bg-[var(--app-surface-muted)] p-4">
      <h4 className="mb-1 text-sm font-medium">{title}</h4>
      <p className="text-sm text-muted-foreground whitespace-pre-wrap">{content}</p>
    </div>
  );
}

function StrategyJsonCard({
  title,
  value,
}: {
  title: string;
  value: unknown;
}) {
  if (!value || (typeof value === 'object' && Object.keys(value as Record<string, unknown>).length === 0)) {
    return null;
  }

  return (
    <div className="rounded-2xl border border-border/70 bg-[var(--app-surface-muted)] p-4">
      <h4 className="mb-1 text-sm font-medium">{title}</h4>
      <pre className="max-h-64 overflow-auto rounded-md bg-muted p-3 text-xs">{JSON.stringify(value, null, 2)}</pre>
    </div>
  );
}

function StrategyDetailContent({
  activeDetailTab,
  detail,
  strategyName,
}: {
  activeDetailTab: DetailTab;
  detail: NonNullable<ReturnType<typeof useStrategy>['data']>;
  strategyName: string;
}) {
  if (activeDetailTab === 'optimize') {
    return <OptimizationGridEditor strategyName={strategyName} />;
  }

  return (
    <div className="space-y-4">
      <StrategyMetadataCard title="Description" content={detail.description ?? null} />
      <StrategyJsonCard title="Configuration" value={detail.config} />
      <StrategyJsonCard title="Execution Info" value={detail.execution_info} />
      <div className="border-t border-border/70 pt-4">
        <OptimizationGridEditor strategyName={strategyName} />
      </div>
    </div>
  );
}

function StrategyDetailDialogs({
  strategyName,
  category,
  showEditor,
  setShowEditor,
  showDeleteDialog,
  setShowDeleteDialog,
  onDeleted,
  showDuplicateDialog,
  setShowDuplicateDialog,
  showRenameDialog,
  setShowRenameDialog,
  onRenamed,
  showMoveDialog,
  setShowMoveDialog,
  onMoved,
}: {
  strategyName: string;
  category: string;
  showEditor: boolean;
  setShowEditor: (open: boolean) => void;
  showDeleteDialog: boolean;
  setShowDeleteDialog: (open: boolean) => void;
  onDeleted?: () => void;
  showDuplicateDialog: boolean;
  setShowDuplicateDialog: (open: boolean) => void;
  showRenameDialog: boolean;
  setShowRenameDialog: (open: boolean) => void;
  onRenamed?: (newName: string) => void;
  showMoveDialog: boolean;
  setShowMoveDialog: (open: boolean) => void;
  onMoved?: (newName: string) => void;
}) {
  return (
    <>
      <StrategyEditor open={showEditor} onOpenChange={setShowEditor} strategyName={strategyName} />
      <DeleteConfirmDialog
        open={showDeleteDialog}
        onOpenChange={setShowDeleteDialog}
        strategyName={strategyName}
        onSuccess={onDeleted}
      />
      <DuplicateDialog open={showDuplicateDialog} onOpenChange={setShowDuplicateDialog} strategyName={strategyName} />
      <RenameDialog
        open={showRenameDialog}
        onOpenChange={setShowRenameDialog}
        strategyName={strategyName}
        onSuccess={onRenamed}
      />
      <MoveGroupDialog
        open={showMoveDialog}
        onOpenChange={setShowMoveDialog}
        strategyName={strategyName}
        currentCategory={category}
        onSuccess={onMoved}
      />
    </>
  );
}

function StrategyDetailPanel({
  strategyName,
  onDeleted,
  onRenamed,
  onMoved,
}: {
  strategyName: string;
  onDeleted?: () => void;
  onRenamed?: (newName: string) => void;
  onMoved?: (newName: string) => void;
}) {
  const { data: detail, isLoading } = useStrategy(strategyName);
  const [showEditor, setShowEditor] = useState(false);
  const [showDeleteDialog, setShowDeleteDialog] = useState(false);
  const [showDuplicateDialog, setShowDuplicateDialog] = useState(false);
  const [showMoveDialog, setShowMoveDialog] = useState(false);
  const [showRenameDialog, setShowRenameDialog] = useState(false);
  const [activeDetailTab, setActiveDetailTab] = useState<DetailTab>('detail');

  if (isLoading) {
    return (
      <div className="flex h-48 items-center justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    );
  }

  if (!detail) return null;

  const canEditYaml = detail.category === 'experimental' || detail.category === 'production';
  const canRenameOrDelete = detail.category === 'experimental';
  const isMovable = isManagedStrategyCategory(detail.category);

  return (
    <>
      <Surface className="p-4 sm:p-5">
        <div className="space-y-5">
          <div className="flex items-start justify-between">
            <div className="space-y-2">
              <SectionEyebrow>Strategy Workspace</SectionEyebrow>
              <h2 className="flex items-center gap-2 text-lg font-semibold tracking-tight text-foreground">
                <FileText className="h-5 w-5" />
                {detail.display_name || detail.name}
              </h2>
              <p className="flex items-center gap-2 text-sm text-muted-foreground">
                Category: {detail.category}
                {!canEditYaml ? <Lock className="h-3 w-3 text-muted-foreground" /> : null}
              </p>
            </div>
          </div>

          <StrategyActionBar
            canEditYaml={canEditYaml}
            canRenameOrDelete={canRenameOrDelete}
            isMovable={isMovable}
            onEdit={() => setShowEditor(true)}
            onRename={() => setShowRenameDialog(true)}
            onDuplicate={() => setShowDuplicateDialog(true)}
            onMove={() => setShowMoveDialog(true)}
            onDelete={() => setShowDeleteDialog(true)}
          />

          <SegmentedTabs items={detailTabs} value={activeDetailTab} onChange={setActiveDetailTab} />

          <StrategyDetailContent activeDetailTab={activeDetailTab} detail={detail} strategyName={strategyName} />
        </div>
      </Surface>

      <StrategyDetailDialogs
        strategyName={strategyName}
        category={detail.category}
        showEditor={showEditor}
        setShowEditor={setShowEditor}
        showDeleteDialog={showDeleteDialog}
        setShowDeleteDialog={setShowDeleteDialog}
        onDeleted={onDeleted}
        showDuplicateDialog={showDuplicateDialog}
        setShowDuplicateDialog={setShowDuplicateDialog}
        showRenameDialog={showRenameDialog}
        setShowRenameDialog={setShowRenameDialog}
        onRenamed={onRenamed}
        showMoveDialog={showMoveDialog}
        setShowMoveDialog={setShowMoveDialog}
        onMoved={onMoved}
      />
    </>
  );
}

export function BacktestStrategies() {
  const { data: strategiesData, isLoading } = useStrategies();
  const [selectedName, setSelectedName] = useState<string | null>(null);

  if (isLoading) {
    return (
      <div className="flex h-48 items-center justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    );
  }

  const strategies = strategiesData?.strategies ?? [];

  if (strategies.length === 0) {
    return <div className="flex h-48 items-center justify-center text-muted-foreground">No strategies available</div>;
  }

  const grouped = strategies.reduce(
    (acc, strategy) => {
      const category = strategy.category || 'other';
      if (!acc[category]) acc[category] = [];
      acc[category].push(strategy);
      return acc;
    },
    {} as Record<string, StrategyMetadata[]>
  );

  for (const category of Object.keys(grouped)) {
    grouped[category]?.sort((left, right) => {
      const leftTime = left.last_modified ? new Date(left.last_modified).getTime() : 0;
      const rightTime = right.last_modified ? new Date(right.last_modified).getTime() : 0;
      return rightTime - leftTime;
    });
  }

  const sortedCategories = Object.keys(grouped).sort(compareManagedStrategyCategory);

  return (
    <div className="grid grid-cols-1 gap-3 lg:grid-cols-[minmax(0,1fr)_minmax(24rem,30rem)] lg:items-start">
      <Surface className="p-4 sm:p-5">
        <div className="space-y-5">
          <div className="space-y-1 border-b border-border/70 pb-4">
            <SectionEyebrow>Strategy Library</SectionEyebrow>
            <h2 className="text-lg font-semibold tracking-tight text-foreground">Strategy Catalog</h2>
            <p className="text-sm text-muted-foreground">
              Production and experimental strategies stay grouped here so detail, editor, and optimization work remain in one workspace.
            </p>
          </div>

          {sortedCategories.map((category) => {
            const categoryStrategies = grouped[category] ?? [];
            return (
              <div key={category} className="space-y-3">
                <h3 className="text-xs font-medium uppercase tracking-[0.16em] text-muted-foreground">
                  {category} ({categoryStrategies.length})
                </h3>
                <div className="space-y-2">
                  {categoryStrategies.map((strategy) => (
                    <StrategyCard
                      key={strategy.name}
                      strategy={strategy}
                      isSelected={selectedName === strategy.name}
                      onClick={() => setSelectedName(strategy.name)}
                    />
                  ))}
                </div>
              </div>
            );
          })}
        </div>
      </Surface>

      <div className="lg:sticky lg:top-0 lg:self-start">
        {selectedName ? (
          <StrategyDetailPanel
            strategyName={selectedName}
            onDeleted={() => setSelectedName(null)}
            onRenamed={(newName) => setSelectedName(newName)}
            onMoved={(newName) => setSelectedName(newName)}
          />
        ) : (
          <Surface className="flex h-48 items-center justify-center p-6 text-muted-foreground">
            Select a strategy to view details
          </Surface>
        )}
      </div>
    </div>
  );
}
