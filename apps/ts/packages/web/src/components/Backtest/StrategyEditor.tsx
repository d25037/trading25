import type {
  AuthoringFieldSchema,
  StrategyEditorReferenceResponse,
  StrategyValidationResponse,
} from '@trading25/api-clients/backtest';
import { AlertCircle, CheckCircle2, Eye, FileCode2, Loader2, PencilLine, Sparkles } from 'lucide-react';
import { type ReactNode, useCallback, useId, useMemo } from 'react';
import { MetadataFieldControl } from '@/components/Backtest/MetadataFieldControl';
import { MonacoYamlEditor } from '@/components/Editor/MonacoYamlEditor';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import {
  useSignalReference,
  useStrategyEditorContext,
  useStrategyEditorReference,
  useUpdateStrategy,
  useValidateStrategy,
} from '@/hooks/useBacktest';
import { useDatasetInfo, useDatasets } from '@/hooks/useDataset';
import { useIndicesList } from '@/hooks/useIndices';
import { cn } from '@/lib/utils';
import { buildVisualAdvancedOnlyPaths } from './authoringDocumentUtils';
import { getValueAtPath, hasValueAtPath, isPlainObject, normalizeSignalSection } from './authoringUtils';
import { SignalReferencePanel } from './SignalReferencePanel';
import { useStrategyEditorSharedConfigFields } from './StrategyEditorSharedConfig';
import {
  executionSemanticsLabels,
  type SignalSectionKey,
  useStrategyEditorSignalRenderer,
} from './StrategyEditorSignals';
import { type EditorTab, useStrategyEditorDraft, type VisualSectionKey } from './useStrategyEditorDraft';

interface StrategyEditorProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  strategyName: string;
  onSuccess?: () => void;
}

const VISUAL_TOP_LEVEL_KEYS = new Set([
  'display_name',
  'description',
  'shared_config',
  'entry_filter_params',
  'exit_trigger_params',
]);

const timingLabels: Record<string, string> = {
  prior_session_close: 'Prior Close',
  current_session_open: 'Current Open',
  current_session_close: 'Current Close',
  next_session_open: 'Next Open',
  current_session: 'Current Session',
  next_session: 'Next Session',
};

function formatTimingLabel(value: string) {
  return timingLabels[value] ?? value;
}

function resolveValidationViewState(validationResult: StrategyValidationResponse) {
  const hasValidationErrors = !validationResult.valid;
  const hasValidationWarnings = (validationResult.warnings?.length ?? 0) > 0;

  if (hasValidationErrors) {
    return {
      containerClass: 'bg-destructive/10 border-destructive/20',
      icon: <AlertCircle className="h-4 w-4 text-destructive" />,
      titleClass: 'text-destructive',
      title: 'Validation failed',
    };
  }
  if (hasValidationWarnings) {
    return {
      containerClass: 'bg-amber-500/10 border-amber-500/20',
      icon: <CheckCircle2 className="h-4 w-4 text-amber-600" />,
      titleClass: 'text-amber-700',
      title: 'Validation passed with warnings',
    };
  }
  return {
    containerClass: 'bg-emerald-500/10 border-emerald-500/20',
    icon: <CheckCircle2 className="h-4 w-4 text-emerald-600" />,
    titleClass: 'text-emerald-700',
    title: 'Validation passed',
  };
}

function EditorTabButton({
  active,
  icon,
  label,
  onClick,
}: {
  active: boolean;
  icon: ReactNode;
  label: string;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      role="tab"
      aria-selected={active}
      onClick={onClick}
      className={cn(
        'inline-flex items-center gap-2 rounded-md px-3 py-2 text-sm font-medium transition-colors',
        active ? 'bg-primary text-primary-foreground' : 'bg-muted text-muted-foreground hover:bg-muted/80'
      )}
    >
      {icon}
      <span>{label}</span>
    </button>
  );
}

function VisualSectionButton({
  active,
  label,
  description,
  onClick,
}: {
  active: boolean;
  label: string;
  description: string;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      aria-current={active ? 'page' : undefined}
      onClick={onClick}
      className={cn(
        'w-full rounded-lg border px-3 py-3 text-left transition-colors',
        active
          ? 'border-primary bg-primary/10 text-foreground'
          : 'border-border/60 bg-background/70 text-muted-foreground hover:border-border hover:bg-muted/40 hover:text-foreground'
      )}
    >
      <div className="text-sm font-medium">{label}</div>
      <div className="mt-1 text-xs text-muted-foreground">{description}</div>
    </button>
  );
}

function PreviewChipList({ items, emptyLabel }: { items: string[]; emptyLabel?: string }) {
  if (items.length === 0) {
    return <span className="text-xs text-muted-foreground">{emptyLabel ?? 'None'}</span>;
  }

  return (
    <div className="mt-1 flex flex-wrap gap-2">
      {items.map((item) => (
        <span key={item} className="rounded bg-muted px-2 py-1 text-xs">
          {item}
        </span>
      ))}
    </div>
  );
}

function PreviewValidationState({
  parseError,
  validationResult,
  previewDirty,
}: {
  parseError: string | null;
  validationResult: StrategyValidationResponse | null;
  previewDirty: boolean;
}) {
  const validationViewState = validationResult ? resolveValidationViewState(validationResult) : null;

  return (
    <>
      {parseError ? (
        <div className="rounded-lg border border-destructive/20 bg-destructive/10 p-4 text-sm text-destructive">
          {parseError}
        </div>
      ) : null}

      {validationResult ? (
        <div className={cn('rounded-lg border p-4', validationViewState?.containerClass)}>
          <div className="flex items-center gap-2">
            {validationViewState?.icon}
            <span className={cn('text-sm font-medium', validationViewState?.titleClass)}>
              {validationViewState?.title}
            </span>
            {previewDirty ? <span className="text-xs text-muted-foreground">(stale)</span> : null}
          </div>

          {(validationResult.errors?.length ?? 0) > 0 ? (
            <ul className="mt-3 list-disc space-y-1 pl-5 text-sm text-destructive">
              {validationResult.errors?.map((error) => (
                <li key={error}>{error}</li>
              ))}
            </ul>
          ) : null}

          {(validationResult.warnings?.length ?? 0) > 0 ? (
            <ul className="mt-3 list-disc space-y-1 pl-5 text-sm text-amber-700">
              {validationResult.warnings?.map((warning) => (
                <li key={warning}>{warning}</li>
              ))}
            </ul>
          ) : null}
        </div>
      ) : null}
    </>
  );
}

function PreviewCompiledStrategy({
  validationResult,
  effectiveExecution,
}: {
  validationResult: StrategyValidationResponse | null;
  effectiveExecution: Record<string, unknown>;
}) {
  const compiledStrategy = validationResult?.compiled_strategy;
  if (!compiledStrategy) {
    return null;
  }

  return (
    <div className="grid gap-4 lg:grid-cols-2">
      <Card>
        <CardHeader>
          <CardTitle className="text-base">Execution</CardTitle>
          <CardDescription>Compiled runtime behavior resolved by backend validation.</CardDescription>
        </CardHeader>
        <CardContent className="space-y-2 text-sm">
          <div>
            <span className="text-muted-foreground">Semantics:</span>{' '}
            <span className="font-medium">
              {executionSemanticsLabels[compiledStrategy.execution_semantics] ?? compiledStrategy.execution_semantics}
            </span>
          </div>
          <div>
            <span className="text-muted-foreground">Timeframe:</span>{' '}
            <span className="font-medium">{compiledStrategy.timeframe}</span>
          </div>
          <div>
            <span className="text-muted-foreground">Dataset:</span>{' '}
            <span className="font-medium">{compiledStrategy.dataset_name ?? 'inherited/default'}</span>
          </div>
          <div className="pt-2">
            <div className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">
              Effective execution
            </div>
            <pre className="mt-2 overflow-x-auto rounded-md bg-muted/40 p-3 text-xs">
              <code>{JSON.stringify(effectiveExecution, null, 2)}</code>
            </pre>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="text-base">Requirements</CardTitle>
          <CardDescription>
            Signals, data domains, and fundamental fields required by the compiled strategy.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-3 text-sm">
          <div>
            <div className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">Signal IDs</div>
            <PreviewChipList items={compiledStrategy.signal_ids ?? []} />
          </div>
          <div>
            <div className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">Data domains</div>
            <PreviewChipList items={compiledStrategy.required_data_domains ?? []} />
          </div>
          <div>
            <div className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">
              Fundamental fields
            </div>
            <PreviewChipList
              items={compiledStrategy.required_fundamental_fields ?? []}
              emptyLabel="No fundamental fields required."
            />
          </div>
        </CardContent>
      </Card>

      <Card className="lg:col-span-2">
        <CardHeader>
          <CardTitle className="text-base">Availability Timing</CardTitle>
          <CardDescription>Each enabled signal with backend-resolved observation and execution timing.</CardDescription>
        </CardHeader>
        <CardContent className="space-y-3">
          {(compiledStrategy.signals ?? []).map((signal) => (
            <div key={signal.signal_id} className="rounded-lg border border-border/60 p-3">
              <div className="flex flex-wrap items-center justify-between gap-2">
                <div>
                  <div className="font-medium">{signal.signal_name}</div>
                  <div className="text-xs text-muted-foreground">
                    {signal.signal_id} · {signal.scope} · {signal.category}
                  </div>
                </div>
                <div className="text-xs text-muted-foreground">
                  {(signal.data_requirements ?? []).join(', ') || 'market'}
                </div>
              </div>
              <div className="mt-3 grid gap-2 text-xs text-muted-foreground sm:grid-cols-2 lg:grid-cols-4">
                <span>Observe: {formatTimingLabel(signal.availability.observation_time)}</span>
                <span>Available: {formatTimingLabel(signal.availability.available_at)}</span>
                <span>Decision: {formatTimingLabel(signal.availability.decision_cutoff)}</span>
                <span>Execute: {formatTimingLabel(signal.availability.execution_session)}</span>
              </div>
            </div>
          ))}
        </CardContent>
      </Card>
    </div>
  );
}

function PreviewPanel({
  parseError,
  validationResult,
  updateErrorMessage,
  previewDirty,
  isRefreshing,
  onRefresh,
  effectiveExecution,
}: {
  parseError: string | null;
  validationResult: StrategyValidationResponse | null;
  updateErrorMessage: string | null;
  previewDirty: boolean;
  isRefreshing: boolean;
  onRefresh: () => void;
  effectiveExecution: Record<string, unknown>;
}) {
  return (
    <div className="space-y-4 overflow-y-auto pr-1">
      <div className="flex flex-wrap items-center justify-between gap-3 rounded-lg border border-border/60 bg-muted/30 p-4">
        <div className="space-y-1">
          <h3 className="text-sm font-semibold">Preview</h3>
          <p className="text-sm text-muted-foreground">
            Backend validation is the only authority. Refresh this tab to inspect the latest compiled strategy.
          </p>
        </div>
        <Button variant="outline" onClick={onRefresh} disabled={isRefreshing}>
          {isRefreshing ? (
            <>
              <Loader2 className="mr-2 h-4 w-4 animate-spin" />
              Refreshing...
            </>
          ) : previewDirty ? (
            'Refresh Preview'
          ) : (
            'Re-run Preview'
          )}
        </Button>
      </div>

      <PreviewValidationState parseError={parseError} validationResult={validationResult} previewDirty={previewDirty} />

      {updateErrorMessage ? (
        <div className="rounded-lg border border-destructive/20 bg-destructive/10 p-4 text-sm text-destructive">
          Error: {updateErrorMessage}
        </div>
      ) : null}

      <PreviewCompiledStrategy validationResult={validationResult} effectiveExecution={effectiveExecution} />
    </div>
  );
}

interface StrategyEditorDialogBodyProps {
  open: boolean;
  activeTab: EditorTab;
  activeVisualSection: VisualSectionKey;
  draftConfig: Record<string, unknown>;
  effectiveExecution: Record<string, unknown>;
  handleCopySnippet: (snippet: string) => void;
  handleOpenChange: (nextOpen: boolean) => void;
  handleSave: () => void;
  handleTabChange: (tab: EditorTab) => void;
  handleYamlChange: (value: string) => void;
  isLoading: boolean;
  parseError: string | null;
  previewDirty: boolean;
  reference?: StrategyEditorReferenceResponse;
  renderSharedField: (field: AuthoringFieldSchema) => ReactNode;
  renderSignalSection: (sectionKey: SignalSectionKey, title: string) => ReactNode;
  removeDraftPath: (path: string) => void;
  runBackendValidation: (switchToPreview?: boolean) => Promise<StrategyValidationResponse | null>;
  scrollToVisualSection: (sectionKey: VisualSectionKey) => void;
  strategyCategory: string;
  updateDraftAtPath: (path: string, value: unknown) => void;
  updateErrorMessage: string | null;
  updatePending: boolean;
  validatePending: boolean;
  validationResult: StrategyValidationResponse | null;
  visualAdvancedOnlyPaths: string[];
  visualSectionIdPrefix: string;
  visualSections: Array<{ key: VisualSectionKey; label: string; description: string }>;
  yamlContent: string;
  datasetInfo: { data?: { name: string; storage: { backend: string } } | null };
}

// biome-ignore lint/complexity/noExcessiveCognitiveComplexity: this view component centralizes the tabbed dialog layout while state orchestration stays in StrategyEditor.
function StrategyEditorDialogBody({
  open,
  activeTab,
  activeVisualSection,
  draftConfig,
  effectiveExecution,
  handleCopySnippet,
  handleOpenChange,
  handleSave,
  handleTabChange,
  handleYamlChange,
  isLoading,
  parseError,
  previewDirty,
  reference,
  renderSharedField,
  renderSignalSection,
  removeDraftPath,
  runBackendValidation,
  scrollToVisualSection,
  strategyCategory,
  updateDraftAtPath,
  updateErrorMessage,
  updatePending,
  validatePending,
  validationResult,
  visualAdvancedOnlyPaths,
  visualSectionIdPrefix,
  visualSections,
  yamlContent,
  datasetInfo,
}: StrategyEditorDialogBodyProps) {
  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogContent className="max-w-7xl max-h-[92vh] flex flex-col">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Sparkles className="h-5 w-5" />
            Strategy Editor
          </DialogTitle>
          <DialogDescription>
            Visual authoring is the default. Raw YAML remains available for advanced edits and unknown fields.
            <span className="ml-2 rounded bg-muted px-2 py-0.5 text-xs text-muted-foreground">{strategyCategory}</span>
          </DialogDescription>
        </DialogHeader>

        {isLoading ? (
          <div className="flex h-[640px] items-center justify-center">
            <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
          </div>
        ) : (
          <div className="flex min-h-0 flex-1 flex-col gap-4 py-2">
            <div className="flex flex-wrap items-center gap-2" role="tablist" aria-label="Strategy editor tabs">
              <EditorTabButton
                active={activeTab === 'visual'}
                icon={<PencilLine className="h-4 w-4" />}
                label="Visual"
                onClick={() => handleTabChange('visual')}
              />
              <EditorTabButton
                active={activeTab === 'advanced'}
                icon={<FileCode2 className="h-4 w-4" />}
                label="Advanced YAML"
                onClick={() => handleTabChange('advanced')}
              />
              <EditorTabButton
                active={activeTab === 'preview'}
                icon={<Eye className="h-4 w-4" />}
                label="Preview"
                onClick={() => handleTabChange('preview')}
              />
            </div>

            {activeTab === 'visual' ? (
              <div className="grid min-h-0 flex-1 gap-4 lg:grid-cols-[220px_minmax(0,1fr)]">
                <aside className="lg:min-h-0">
                  <Card className="border-border/60 lg:sticky lg:top-0">
                    <CardHeader className="pb-3">
                      <CardTitle className="text-base">Sections</CardTitle>
                      <CardDescription>
                        Jump between strategy metadata, shared config, and signal blocks.
                      </CardDescription>
                    </CardHeader>
                    <CardContent className="grid gap-2">
                      {visualSections.map((section) => (
                        <VisualSectionButton
                          key={section.key}
                          active={activeVisualSection === section.key}
                          label={section.label}
                          description={section.description}
                          onClick={() => scrollToVisualSection(section.key)}
                        />
                      ))}
                    </CardContent>
                  </Card>
                </aside>

                <div className="min-h-0 overflow-y-auto pr-1">
                  <div className="space-y-4">
                    <section id={`${visualSectionIdPrefix}-basics`} className="scroll-mt-4">
                      <Card className="border-border/60">
                        <CardHeader>
                          <CardTitle className="text-lg">Basics</CardTitle>
                          <CardDescription>Strategy metadata shown in the catalog and detail views.</CardDescription>
                        </CardHeader>
                        <CardContent className="grid gap-3 lg:grid-cols-2">
                          {(reference?.basics ?? []).map((field) => (
                            <MetadataFieldControl
                              key={field.path}
                              field={field}
                              value={getValueAtPath(draftConfig, field.path)}
                              overridden={hasValueAtPath(draftConfig, field.path) ? true : undefined}
                              onChange={(value) => updateDraftAtPath(field.path, value)}
                              onReset={() => removeDraftPath(field.path)}
                            />
                          ))}
                        </CardContent>
                      </Card>
                    </section>

                    <section id={`${visualSectionIdPrefix}-shared_config`} className="scroll-mt-4">
                      <Card className="border-border/60">
                        <CardHeader>
                          <CardTitle className="text-lg">Shared Config</CardTitle>
                          <CardDescription>
                            Visual controls are driven by backend metadata. Reset removes the local override rather than
                            copying the default.
                          </CardDescription>
                        </CardHeader>
                        <CardContent className="space-y-6">
                          {datasetInfo.data ? (
                            <div className="rounded-lg bg-muted/40 p-3 text-sm text-muted-foreground">
                              Dataset <strong>{datasetInfo.data.name}</strong> loaded from{' '}
                              {datasetInfo.data.storage.backend}.
                            </div>
                          ) : null}

                          {(reference?.shared_config_groups ?? []).map((group) => {
                            const groupFields = (reference?.shared_config_fields ?? []).filter(
                              (field) => field.group === group.key
                            );
                            if (groupFields.length === 0) return null;
                            return (
                              <div key={group.key} className="space-y-3">
                                <div>
                                  <h3 className="text-sm font-semibold">{group.label}</h3>
                                  {group.description ? (
                                    <p className="text-sm text-muted-foreground">{group.description}</p>
                                  ) : null}
                                </div>
                                <div className="grid gap-3 lg:grid-cols-2">{groupFields.map(renderSharedField)}</div>
                              </div>
                            );
                          })}
                        </CardContent>
                      </Card>
                    </section>

                    <section id={`${visualSectionIdPrefix}-entry_filter`} className="scroll-mt-4">
                      {renderSignalSection('entry_filter_params', 'Entry Signals')}
                    </section>

                    <section id={`${visualSectionIdPrefix}-exit_trigger`} className="scroll-mt-4">
                      {renderSignalSection('exit_trigger_params', 'Exit Signals')}
                    </section>

                    {visualAdvancedOnlyPaths.length > 0 ? (
                      <section id={`${visualSectionIdPrefix}-advanced_only`} className="scroll-mt-4">
                        <Card className="border-dashed border-border/60">
                          <CardHeader>
                            <CardTitle className="text-base">Advanced-only Content</CardTitle>
                            <CardDescription>
                              These paths are preserved on save but can only be edited in the Advanced YAML tab in v1.
                            </CardDescription>
                          </CardHeader>
                          <CardContent>
                            <ul className="list-disc space-y-1 pl-5 text-sm text-muted-foreground">
                              {visualAdvancedOnlyPaths.map((path) => (
                                <li key={path}>{path}</li>
                              ))}
                            </ul>
                          </CardContent>
                        </Card>
                      </section>
                    ) : null}
                  </div>
                </div>
              </div>
            ) : null}

            {activeTab === 'advanced' ? (
              <div className="grid min-h-0 flex-1 grid-cols-1 gap-4 lg:grid-cols-3">
                <div className="lg:col-span-2 min-h-0">
                  <MonacoYamlEditor value={yamlContent} onChange={handleYamlChange} height="620px" />
                  {parseError ? (
                    <div className="mt-3 rounded-lg border border-destructive/20 bg-destructive/10 p-3 text-sm text-destructive">
                      {parseError}
                    </div>
                  ) : null}
                </div>
                <div className="min-h-0 overflow-hidden rounded-lg border">
                  <SignalReferencePanel onCopySnippet={handleCopySnippet} />
                </div>
              </div>
            ) : null}

            {activeTab === 'preview' ? (
              <PreviewPanel
                parseError={parseError}
                validationResult={validationResult}
                updateErrorMessage={updateErrorMessage}
                previewDirty={previewDirty}
                isRefreshing={validatePending}
                onRefresh={() => {
                  void runBackendValidation(false);
                }}
                effectiveExecution={effectiveExecution}
              />
            ) : null}
          </div>
        )}

        <DialogFooter className="gap-2">
          <Button variant="outline" onClick={() => handleOpenChange(false)}>
            Cancel
          </Button>
          <Button
            variant="outline"
            onClick={() => {
              void runBackendValidation(true);
            }}
            disabled={validatePending || updatePending}
          >
            {validatePending ? (
              <>
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                Validating...
              </>
            ) : (
              'Validate'
            )}
          </Button>
          <Button onClick={handleSave} disabled={validatePending || updatePending || isLoading}>
            {updatePending ? (
              <>
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                Saving...
              </>
            ) : (
              'Save'
            )}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function resolveStrategyEditorLoading(...states: boolean[]) {
  return states.some(Boolean);
}

export function StrategyEditor({ open, onOpenChange, strategyName, onSuccess }: StrategyEditorProps) {
  const strategyContextQuery = useStrategyEditorContext(open ? strategyName : null);
  const referenceQuery = useStrategyEditorReference(open);
  const signalReferenceQuery = useSignalReference();
  const { data: datasets } = useDatasets();
  const { data: indices } = useIndicesList();
  const updateStrategy = useUpdateStrategy();
  const validateStrategy = useValidateStrategy();
  const visualSectionIdPrefix = useId();
  const {
    activeTab,
    activeVisualSection,
    draftConfig,
    handleCopySnippet,
    handleOpenChange,
    handleSave,
    handleTabChange,
    handleYamlChange,
    parseError,
    previewDirty,
    removeDraftPath,
    runBackendValidation,
    setActiveVisualSection,
    updateDraftAtPath,
    updateErrorMessage,
    updatePending,
    validatePending,
    validationResult,
    yamlContent,
  } = useStrategyEditorDraft({
    open,
    strategyName,
    strategyContextQuery,
    updateStrategy,
    validateStrategy,
    onOpenChange,
    onSuccess,
  });

  const definitions = signalReferenceQuery.data?.signals ?? [];
  const categories = signalReferenceQuery.data?.categories ?? [];

  const reference = referenceQuery.data;
  const context = strategyContextQuery.data;
  const defaultSharedConfig = useMemo(
    () =>
      context?.default_shared_config && isPlainObject(context.default_shared_config)
        ? context.default_shared_config
        : {},
    [context?.default_shared_config]
  );
  const effectiveExecution = useMemo(
    () =>
      context?.effective_execution && isPlainObject(context.effective_execution) ? context.effective_execution : {},
    [context?.effective_execution]
  );

  const rawSharedConfig = normalizeSignalSection(draftConfig.shared_config);
  const { definitionsByType, fundamentalDefinitionsByType, fundamentalParentFieldNames, renderSignalSection } =
    useStrategyEditorSignalRenderer({
      categories,
      defaultSharedConfig,
      definitions,
      draftConfig,
      rawSharedConfig,
      removeDraftPath,
      updateDraftAtPath,
    });

  const visualAdvancedOnlyPaths = useMemo(
    () =>
      buildVisualAdvancedOnlyPaths(
        draftConfig,
        definitionsByType,
        fundamentalDefinitionsByType,
        fundamentalParentFieldNames,
        VISUAL_TOP_LEVEL_KEYS
      ),
    [definitionsByType, draftConfig, fundamentalDefinitionsByType, fundamentalParentFieldNames]
  );

  const { datasetSnapshotName, renderSharedField } = useStrategyEditorSharedConfigFields({
    contextReady: Boolean(context),
    datasets,
    defaultSharedConfig,
    indices,
    rawSharedConfig,
    removeDraftPath,
    updateDraftAtPath,
  });

  const visualSections = useMemo(() => {
    const sections: Array<{ key: VisualSectionKey; label: string; description: string }> = [
      {
        key: 'basics',
        label: 'Basics',
        description: 'Display name and strategy summary.',
      },
      {
        key: 'shared_config',
        label: 'Shared Settings',
        description: 'Dataset, execution, portfolio, and optimization defaults.',
      },
      {
        key: 'entry_filter',
        label: 'Entry Filters',
        description: 'Signals that gate entries.',
      },
      {
        key: 'exit_trigger',
        label: 'Exit Triggers',
        description: 'Signals used only in standard execution mode.',
      },
    ];

    if (visualAdvancedOnlyPaths.length > 0) {
      sections.push({
        key: 'advanced_only',
        label: 'Advanced Fields',
        description: 'Paths preserved on save but edited only in YAML.',
      });
    }

    return sections;
  }, [visualAdvancedOnlyPaths.length]);

  const scrollToVisualSection = useCallback(
    (sectionKey: VisualSectionKey) => {
      setActiveVisualSection(sectionKey);
      document.getElementById(`${visualSectionIdPrefix}-${sectionKey}`)?.scrollIntoView({
        behavior: 'smooth',
        block: 'start',
      });
    },
    [setActiveVisualSection, visualSectionIdPrefix]
  );

  const isLoading = resolveStrategyEditorLoading(
    strategyContextQuery.isLoading,
    referenceQuery.isLoading,
    signalReferenceQuery.isLoading
  );
  const strategyCategory = context?.category ?? 'unknown';
  const datasetInfo = useDatasetInfo(open ? datasetSnapshotName : null);

  return (
    <StrategyEditorDialogBody
      open={open}
      activeTab={activeTab}
      activeVisualSection={activeVisualSection}
      draftConfig={draftConfig}
      effectiveExecution={effectiveExecution}
      handleCopySnippet={handleCopySnippet}
      handleOpenChange={handleOpenChange}
      handleSave={handleSave}
      handleTabChange={handleTabChange}
      handleYamlChange={handleYamlChange}
      isLoading={isLoading}
      parseError={parseError}
      previewDirty={previewDirty}
      reference={reference}
      renderSharedField={renderSharedField}
      renderSignalSection={renderSignalSection}
      removeDraftPath={removeDraftPath}
      runBackendValidation={runBackendValidation}
      scrollToVisualSection={scrollToVisualSection}
      strategyCategory={strategyCategory}
      updateDraftAtPath={updateDraftAtPath}
      updateErrorMessage={updateErrorMessage}
      updatePending={updatePending}
      validatePending={validatePending}
      validationResult={validationResult}
      visualAdvancedOnlyPaths={visualAdvancedOnlyPaths}
      visualSectionIdPrefix={visualSectionIdPrefix}
      visualSections={visualSections}
      yamlContent={yamlContent}
      datasetInfo={datasetInfo}
    />
  );
}
