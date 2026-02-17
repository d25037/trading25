import { ArrowLeftRight, Code, Copy, Edit, FileText, Loader2, Lock, Pencil, Settings2, Trash2 } from 'lucide-react';
import { useState } from 'react';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
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
    <Card
      className={cn('cursor-pointer transition-all hover:border-primary/50', isSelected && 'border-primary shadow-md')}
      onClick={onClick}
    >
      <CardHeader className="pb-2">
        <CardTitle className="text-base flex items-center gap-2">
          <Code className="h-4 w-4" />
          {strategy.display_name || strategy.name}
        </CardTitle>
        <CardDescription className="text-xs">
          <span className="inline-block px-2 py-0.5 rounded-full bg-secondary text-secondary-foreground capitalize">
            {strategy.category}
          </span>
        </CardDescription>
      </CardHeader>
      {strategy.description && (
        <CardContent className="pt-0">
          <p className="text-sm text-muted-foreground line-clamp-2">{strategy.description}</p>
        </CardContent>
      )}
    </Card>
  );
}

type DetailTab = 'detail' | 'optimize';

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
      <div className="flex items-center justify-center h-48">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    );
  }

  if (!detail) return null;

  const isEditable = detail.category === 'experimental';
  const isMovable = isManagedStrategyCategory(detail.category);

  return (
    <>
      <Card>
        <CardHeader>
          <div className="flex items-start justify-between">
            <div>
              <CardTitle className="flex items-center gap-2">
                <FileText className="h-5 w-5" />
                {detail.display_name || detail.name}
              </CardTitle>
              <CardDescription className="flex items-center gap-2 mt-1">
                Category: {detail.category}
                {!isEditable && <Lock className="h-3 w-3 text-muted-foreground" />}
              </CardDescription>
            </div>
          </div>
          {/* Action Buttons */}
          <div className="flex flex-wrap gap-2 pt-2">
            {isEditable && (
              <Button variant="outline" size="sm" onClick={() => setShowEditor(true)}>
                <Edit className="h-4 w-4 mr-1" />
                Edit
              </Button>
            )}
            {isEditable && (
              <Button variant="outline" size="sm" onClick={() => setShowRenameDialog(true)}>
                <Pencil className="h-4 w-4 mr-1" />
                Rename
              </Button>
            )}
            <Button variant="outline" size="sm" onClick={() => setShowDuplicateDialog(true)}>
              <Copy className="h-4 w-4 mr-1" />
              Duplicate
            </Button>
            {isMovable && (
              <Button variant="outline" size="sm" onClick={() => setShowMoveDialog(true)}>
                <ArrowLeftRight className="h-4 w-4 mr-1" />
                Move Group
              </Button>
            )}
            {isEditable && (
              <Button
                variant="outline"
                size="sm"
                className="text-destructive hover:text-destructive"
                onClick={() => setShowDeleteDialog(true)}
              >
                <Trash2 className="h-4 w-4 mr-1" />
                Delete
              </Button>
            )}
          </div>
          {/* Detail / Optimize tabs */}
          <div className="flex border-b mt-2">
            <button
              type="button"
              onClick={() => setActiveDetailTab('detail')}
              className={cn(
                'px-3 py-1.5 text-sm font-medium border-b-2 transition-colors',
                activeDetailTab === 'detail'
                  ? 'border-primary text-primary'
                  : 'border-transparent text-muted-foreground hover:text-foreground'
              )}
            >
              Detail
            </button>
            <button
              type="button"
              onClick={() => setActiveDetailTab('optimize')}
              className={cn(
                'px-3 py-1.5 text-sm font-medium border-b-2 transition-colors flex items-center gap-1',
                activeDetailTab === 'optimize'
                  ? 'border-primary text-primary'
                  : 'border-transparent text-muted-foreground hover:text-foreground'
              )}
            >
              <Settings2 className="h-3.5 w-3.5" />
              Optimize
            </button>
          </div>
        </CardHeader>
        <CardContent className="space-y-4">
          {activeDetailTab === 'detail' ? (
            <>
              {detail.description && (
                <div>
                  <h4 className="text-sm font-medium mb-1">Description</h4>
                  <p className="text-sm text-muted-foreground">{detail.description}</p>
                </div>
              )}
              {detail.config && Object.keys(detail.config).length > 0 && (
                <div>
                  <h4 className="text-sm font-medium mb-1">Configuration</h4>
                  <pre className="text-xs bg-muted p-3 rounded-md overflow-auto max-h-64">
                    {JSON.stringify(detail.config, null, 2)}
                  </pre>
                </div>
              )}
              {detail.execution_info && Object.keys(detail.execution_info).length > 0 && (
                <div>
                  <h4 className="text-sm font-medium mb-1">Execution Info</h4>
                  <pre className="text-xs bg-muted p-3 rounded-md overflow-auto max-h-64">
                    {JSON.stringify(detail.execution_info, null, 2)}
                  </pre>
                </div>
              )}
            </>
          ) : (
            <OptimizationGridEditor strategyName={strategyName} />
          )}
        </CardContent>
      </Card>

      {/* Dialogs */}
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
        currentCategory={detail.category}
        onSuccess={onMoved}
      />
    </>
  );
}

export function BacktestStrategies() {
  const { data: strategiesData, isLoading } = useStrategies();
  const [selectedName, setSelectedName] = useState<string | null>(null);

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-48">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    );
  }

  const strategies = strategiesData?.strategies ?? [];

  if (strategies.length === 0) {
    return <div className="flex items-center justify-center h-48 text-muted-foreground">No strategies available</div>;
  }

  // Group by category
  const grouped = strategies.reduce(
    (acc, s) => {
      const cat = s.category || 'other';
      if (!acc[cat]) acc[cat] = [];
      acc[cat].push(s);
      return acc;
    },
    {} as Record<string, StrategyMetadata[]>
  );

  // Sort strategies within each category by last_modified descending
  for (const cat of Object.keys(grouped)) {
    grouped[cat]?.sort((a, b) => {
      const aTime = a.last_modified ? new Date(a.last_modified).getTime() : 0;
      const bTime = b.last_modified ? new Date(b.last_modified).getTime() : 0;
      return bTime - aTime;
    });
  }

  const sortedCategories = Object.keys(grouped).sort(compareManagedStrategyCategory);

  return (
    <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
      {/* Strategy List */}
      <div className="space-y-6">
        {sortedCategories.map((category) => {
          const categoryStrategies = grouped[category] ?? [];
          return (
            <div key={category}>
              <h3 className="text-sm font-medium text-muted-foreground uppercase tracking-wider mb-3">
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

      {/* Detail Panel */}
      <div className="lg:sticky lg:top-4">
        {selectedName ? (
          <StrategyDetailPanel
            strategyName={selectedName}
            onDeleted={() => setSelectedName(null)}
            onRenamed={(newName) => setSelectedName(newName)}
            onMoved={(newName) => setSelectedName(newName)}
          />
        ) : (
          <Card className="flex items-center justify-center h-48 text-muted-foreground">
            Select a strategy to view details
          </Card>
        )}
      </div>
    </div>
  );
}
