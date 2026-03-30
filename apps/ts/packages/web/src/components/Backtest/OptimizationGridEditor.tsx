import { AlertCircle, CheckCircle2, Edit, Info, Loader2, RotateCcw, Save, Trash2 } from 'lucide-react';
import { useCallback, useEffect, useMemo, useState } from 'react';
import { MonacoYamlEditor } from '@/components/Editor/MonacoYamlEditor';
import { Button } from '@/components/ui/button';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import {
  useDeleteStrategyOptimization,
  useGenerateStrategyOptimizationDraft,
  useSaveStrategyOptimization,
  useStrategyOptimization,
} from '@/hooks/useOptimization';
import { cn } from '@/lib/utils';
import type {
  OptimizationDiagnosticResponse,
  StrategyOptimizationStateResponse,
} from '@/types/backtest';
import {
  analyzeGridParameters,
  type GridParameterAnalysis,
  type GridValidationIssue,
} from './optimizationGridParams';
import { SignalReferencePanel } from './SignalReferencePanel';

const EMPTY_OPTIMIZATION_YAML = `description: ""
parameter_ranges: {}
`;

type ValidationTone = 'error' | 'warning' | 'success';

interface ValidationState {
  tone: ValidationTone;
  message: string;
  details: string[];
}

interface SummaryCardsProps {
  currentParamCount: number;
  currentCombinations: number;
  savedInfo: string;
  stateLabel: string;
  readyLabel: string;
  driftCount: number;
}

interface EditorDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  strategyName: string;
  content: string;
  validationState: ValidationState;
  summary: SummaryCardsProps;
  hasPersistedSpec: boolean;
  isDirty: boolean;
  isSavePending: boolean;
  isDeletePending: boolean;
  isGeneratePending: boolean;
  saveErrorMessage: string | null;
  deleteErrorMessage: string | null;
  generateErrorMessage: string | null;
  onContentChange: (value: string) => void;
  onCopySnippet: (snippet: string) => void;
  onGenerateDraft: () => void;
  onReset: () => void;
  onDelete: () => void;
  onSave: () => void;
}

const VALIDATION_TONE_CLASS: Record<ValidationTone, string> = {
  error: 'bg-destructive/10 text-destructive',
  warning: 'bg-yellow-500/10 text-yellow-700',
  success: 'bg-green-500/10 text-green-700',
};

const VALIDATION_TONE_ICON = {
  error: AlertCircle,
  warning: Info,
  success: CheckCircle2,
} as const;

function formatValidationIssue(issue: GridValidationIssue | OptimizationDiagnosticResponse): string {
  return `${issue.path}: ${issue.message}`;
}

function buildEmptyState(strategyName: string): StrategyOptimizationStateResponse {
  return {
    strategy_name: strategyName,
    persisted: false,
    source: 'saved',
    optimization: null,
    yaml_content: '',
    valid: true,
    ready_to_run: false,
    param_count: 0,
    combinations: 0,
    errors: [],
    warnings: [],
    drift: [],
  };
}

function normalizeEditorContent(yamlContent: string): string {
  return yamlContent.trim() ? yamlContent : EMPTY_OPTIMIZATION_YAML;
}

function buildValidationState(
  analysis: GridParameterAnalysis,
  state: StrategyOptimizationStateResponse | null,
  isDirty: boolean
): ValidationState {
  if (analysis.parseError) {
    return { tone: 'error', message: analysis.parseError, details: [] };
  }

  if (analysis.errors.length > 0) {
    return {
      tone: 'error',
      message: `Validation failed: ${analysis.errors.length} structural issue(s) must be fixed before saving.`,
      details: analysis.errors.map(formatValidationIssue),
    };
  }

  if (isDirty) {
    const details = analysis.warnings.map(formatValidationIssue);
    return {
      tone: analysis.readyToRun ? 'success' : 'warning',
      message: analysis.readyToRun
        ? 'Draft is structurally valid. Strategy-linked validation and drift checks run on save.'
        : 'Draft is structurally valid, but candidate ranges are not ready to run yet.',
      details,
    };
  }

  if (state) {
    const errorDetails = state.errors.map(formatValidationIssue);
    if (errorDetails.length > 0) {
      return {
        tone: 'error',
        message: `Saved optimization has ${errorDetails.length} blocking issue(s). Fix the strategy-linked spec before running optimization.`,
        details: errorDetails,
      };
    }

    const warningDetails = [...state.warnings, ...state.drift].map(formatValidationIssue);
    if (!state.persisted) {
      return {
        tone: 'warning',
        message: 'No saved optimization spec. Generate a draft from the current strategy or author one manually.',
        details: warningDetails,
      };
    }
    if (warningDetails.length > 0) {
      return {
        tone: 'warning',
        message: state.ready_to_run
          ? 'Saved optimization is usable, but drift or non-blocking warnings were detected.'
          : 'Saved optimization is not ready to run yet.',
        details: warningDetails,
      };
    }
    return {
      tone: 'success',
      message: 'Saved optimization is ready to run.',
      details: [],
    };
  }

  return {
    tone: 'warning',
    message: 'Optimization state is not loaded yet.',
    details: [],
  };
}

function getSavedInfo(savedState: StrategyOptimizationStateResponse | null | undefined): string {
  if (!savedState?.persisted) {
    return 'Not saved';
  }
  return `${savedState.param_count} params, ${savedState.combinations} combinations`;
}

function getStateLabel(state: StrategyOptimizationStateResponse, isDirty: boolean): string {
  if (isDirty) {
    return state.source === 'draft' ? 'Generated draft (unsaved)' : 'Unsaved changes';
  }
  if (state.persisted) {
    return 'Saved';
  }
  return state.source === 'draft' ? 'Generated draft' : 'No saved spec';
}

function LoadingState() {
  return (
    <div className="flex h-32 items-center justify-center">
      <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
    </div>
  );
}

function ValidationBanner({ validationState }: { validationState: ValidationState }) {
  const Icon = VALIDATION_TONE_ICON[validationState.tone];

  return (
    <div className={cn('flex items-start gap-2 rounded-md p-3 text-sm', VALIDATION_TONE_CLASS[validationState.tone])}>
      <Icon className="mt-0.5 h-4 w-4 shrink-0" />
      <div className="space-y-1">
        <p>{validationState.message}</p>
        {validationState.details.length > 0 ? (
          <ul className="list-disc pl-5 text-xs leading-5">
            {validationState.details.map((detail) => (
              <li key={detail}>{detail}</li>
            ))}
          </ul>
        ) : null}
      </div>
    </div>
  );
}

function ErrorBanner({ message }: { message: string }) {
  return (
    <div className="flex items-start gap-2 rounded-md bg-destructive/10 p-3 text-sm text-destructive">
      <AlertCircle className="mt-0.5 h-4 w-4 shrink-0" />
      <span>{message}</span>
    </div>
  );
}

function SummaryCards({
  currentParamCount,
  currentCombinations,
  savedInfo,
  stateLabel,
  readyLabel,
  driftCount,
}: SummaryCardsProps) {
  return (
    <div className="grid gap-2 md:grid-cols-4">
      <div className="rounded-md border bg-muted/20 p-2">
        <p className="text-[11px] uppercase tracking-wide text-muted-foreground">Current</p>
        <p className="text-sm font-medium">
          {currentParamCount} params / {currentCombinations} combos
        </p>
      </div>
      <div className="rounded-md border bg-muted/20 p-2">
        <p className="text-[11px] uppercase tracking-wide text-muted-foreground">Saved</p>
        <p className="text-sm font-medium">{savedInfo}</p>
      </div>
      <div className="rounded-md border bg-muted/20 p-2">
        <p className="text-[11px] uppercase tracking-wide text-muted-foreground">State</p>
        <p className="text-sm font-medium">{stateLabel}</p>
      </div>
      <div className="rounded-md border bg-muted/20 p-2">
        <p className="text-[11px] uppercase tracking-wide text-muted-foreground">Run / Drift</p>
        <p className="text-sm font-medium">
          {readyLabel} / {driftCount} drift
        </p>
      </div>
    </div>
  );
}

function EditorDialog({
  open,
  onOpenChange,
  strategyName,
  content,
  validationState,
  summary,
  hasPersistedSpec,
  isDirty,
  isSavePending,
  isDeletePending,
  isGeneratePending,
  saveErrorMessage,
  deleteErrorMessage,
  generateErrorMessage,
  onContentChange,
  onCopySnippet,
  onGenerateDraft,
  onReset,
  onDelete,
  onSave,
}: EditorDialogProps) {
  const hasValidationError = validationState.tone === 'error';
  const isBusy = isSavePending || isDeletePending || isGeneratePending;

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="flex max-h-[92vh] max-w-7xl flex-col">
        <DialogHeader>
          <DialogTitle>Optimization Spec Editor: {strategyName}</DialogTitle>
          <DialogDescription>
            The optimization block is stored directly on the strategy YAML. Generate a draft from enabled signals, then
            edit ranges as needed.
          </DialogDescription>
        </DialogHeader>

        <div className="min-h-0 flex-1 space-y-3 overflow-hidden">
          <SummaryCards {...summary} />

          <div className="grid h-[560px] grid-cols-1 gap-4 lg:grid-cols-3">
            <div className="flex min-h-0 flex-col lg:col-span-2">
              <div className="mb-2 flex flex-wrap items-center justify-between gap-2">
                <p className="text-xs text-muted-foreground">
                  Save writes the top-level `optimization` block back into the current strategy YAML.
                </p>
                <Button variant="outline" size="sm" onClick={onGenerateDraft} disabled={isBusy}>
                  {isGeneratePending ? <Loader2 className="mr-1 h-4 w-4 animate-spin" /> : null}
                  Generate Draft from Strategy
                </Button>
              </div>

              <div className="min-h-0 flex-1">
                <MonacoYamlEditor value={content} onChange={onContentChange} height="460px" />
              </div>

              <div className="mt-3 space-y-2">
                <ValidationBanner validationState={validationState} />
                {generateErrorMessage ? <ErrorBanner message={generateErrorMessage} /> : null}
                {saveErrorMessage ? <ErrorBanner message={saveErrorMessage} /> : null}
                {deleteErrorMessage ? <ErrorBanner message={deleteErrorMessage} /> : null}
              </div>
            </div>

            <div className="min-h-0 overflow-hidden rounded-md border lg:col-span-1">
              <SignalReferencePanel onCopySnippet={onCopySnippet} />
            </div>
          </div>
        </div>

        <DialogFooter className="gap-2 sm:justify-between">
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            Close
          </Button>
          <div className="flex flex-wrap gap-2 justify-end">
            <Button variant="outline" size="sm" onClick={onReset} disabled={!isDirty || isBusy}>
              <RotateCcw className="mr-1 h-4 w-4" />
              Reset
            </Button>
            {hasPersistedSpec ? (
              <Button
                variant="outline"
                size="sm"
                onClick={onDelete}
                disabled={isBusy}
                className="text-destructive hover:text-destructive"
              >
                <Trash2 className="mr-1 h-4 w-4" />
                Delete
              </Button>
            ) : null}
            <Button size="sm" onClick={onSave} disabled={!isDirty || hasValidationError || isBusy}>
              {isSavePending ? <Loader2 className="mr-1 h-4 w-4 animate-spin" /> : <Save className="mr-1 h-4 w-4" />}
              Save
            </Button>
          </div>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

interface OptimizationSpecEditorProps {
  strategyName: string;
}

export function OptimizationSpecEditor({ strategyName }: OptimizationSpecEditorProps) {
  const { data: savedState, isLoading } = useStrategyOptimization(strategyName);
  const saveOptimization = useSaveStrategyOptimization();
  const deleteOptimization = useDeleteStrategyOptimization();
  const generateDraft = useGenerateStrategyOptimizationDraft();

  const [editorState, setEditorState] = useState<StrategyOptimizationStateResponse | null>(null);
  const [content, setContent] = useState(EMPTY_OPTIMIZATION_YAML);
  const [baselineContent, setBaselineContent] = useState(EMPTY_OPTIMIZATION_YAML);
  const [isDirty, setIsDirty] = useState(false);
  const [isEditorOpen, setIsEditorOpen] = useState(false);

  useEffect(() => {
    if (!savedState || isDirty) {
      return;
    }

    const normalizedContent = normalizeEditorContent(savedState.yaml_content);
    setEditorState(savedState);
    setContent(normalizedContent);
    setBaselineContent(normalizedContent);
  }, [savedState, isDirty]);

  const effectiveState = editorState ?? savedState ?? buildEmptyState(strategyName);
  const analysis = useMemo(() => analyzeGridParameters(content), [content]);
  const validationState = useMemo(
    () => buildValidationState(analysis, effectiveState, isDirty),
    [analysis, effectiveState, isDirty]
  );

  const applyContent = useCallback(
    (nextContent: string) => {
      setContent(nextContent);
      setIsDirty(nextContent !== baselineContent);
    },
    [baselineContent]
  );

  const handleCopySnippet = useCallback(
    (snippet: string) => {
      const nextContent = content.trim() ? `${content.trim()}\n\n${snippet}` : snippet;
      applyContent(nextContent);
    },
    [applyContent, content]
  );

  const handleGenerateDraft = useCallback(() => {
    generateDraft.mutate(strategyName, {
      onSuccess: (nextState) => {
        const nextContent = normalizeEditorContent(nextState.yaml_content);
        setEditorState(nextState);
        setContent(nextContent);
        setIsDirty(nextContent !== baselineContent);
      },
    });
  }, [baselineContent, generateDraft, strategyName]);

  const handleSave = useCallback(() => {
    saveOptimization.mutate(
      {
        strategy: strategyName,
        request: { yaml_content: content },
      },
      {
        onSuccess: (nextState) => {
          const nextContent = normalizeEditorContent(nextState.yaml_content);
          setEditorState(nextState);
          setContent(nextContent);
          setBaselineContent(nextContent);
          setIsDirty(false);
        },
      }
    );
  }, [content, saveOptimization, strategyName]);

  const handleDelete = useCallback(() => {
    deleteOptimization.mutate(strategyName, {
      onSuccess: () => {
        const emptyState = buildEmptyState(strategyName);
        setEditorState(emptyState);
        setContent(EMPTY_OPTIMIZATION_YAML);
        setBaselineContent(EMPTY_OPTIMIZATION_YAML);
        setIsDirty(false);
      },
    });
  }, [deleteOptimization, strategyName]);

  const handleReset = useCallback(() => {
    setContent(baselineContent);
    setEditorState(savedState ?? buildEmptyState(strategyName));
    setIsDirty(false);
  }, [baselineContent, savedState, strategyName]);

  if (isLoading && !savedState && !editorState) {
    return <LoadingState />;
  }

  const currentParamCount = isDirty ? analysis.paramCount : effectiveState.param_count;
  const currentCombinations = isDirty ? analysis.combinations : effectiveState.combinations;
  const savedInfo = getSavedInfo(savedState);
  const stateLabel = getStateLabel(effectiveState, isDirty);
  const readyLabel = effectiveState.ready_to_run ? 'Ready to Run' : 'Needs Update';
  const summary: SummaryCardsProps = {
    currentParamCount,
    currentCombinations,
    savedInfo,
    stateLabel,
    readyLabel,
    driftCount: effectiveState.drift.length,
  };

  return (
    <div className="space-y-4">
      <div className="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
        <div className="space-y-1">
          <h4 className="text-sm font-semibold">Optimization Spec</h4>
          <p className="text-xs text-muted-foreground">
            This strategy stores optimization ranges in its own top-level `optimization` block.
          </p>
          <p className="font-mono text-xs text-muted-foreground">strategy: {strategyName}</p>
        </div>

        <div className="flex flex-wrap gap-2">
          <Button size="sm" variant="outline" onClick={handleGenerateDraft} disabled={generateDraft.isPending}>
            {generateDraft.isPending ? <Loader2 className="mr-1 h-4 w-4 animate-spin" /> : null}
            Generate Draft from Strategy
          </Button>
          <Button size="sm" variant="outline" onClick={() => setIsEditorOpen(true)}>
            <Edit className="mr-1 h-4 w-4" />
            Open Editor
          </Button>
        </div>
      </div>

      <SummaryCards {...summary} />
      <ValidationBanner validationState={validationState} />

      <EditorDialog
        open={isEditorOpen}
        onOpenChange={setIsEditorOpen}
        strategyName={strategyName}
        content={content}
        validationState={validationState}
        summary={summary}
        hasPersistedSpec={effectiveState.persisted}
        isDirty={isDirty}
        isSavePending={saveOptimization.isPending}
        isDeletePending={deleteOptimization.isPending}
        isGeneratePending={generateDraft.isPending}
        saveErrorMessage={saveOptimization.isError ? saveOptimization.error.message : null}
        deleteErrorMessage={deleteOptimization.isError ? deleteOptimization.error.message : null}
        generateErrorMessage={generateDraft.isError ? generateDraft.error.message : null}
        onContentChange={applyContent}
        onCopySnippet={handleCopySnippet}
        onGenerateDraft={handleGenerateDraft}
        onReset={handleReset}
        onDelete={handleDelete}
        onSave={handleSave}
      />
    </div>
  );
}

export const OptimizationGridEditor = OptimizationSpecEditor;
