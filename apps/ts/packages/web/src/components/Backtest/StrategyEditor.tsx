import { AlertCircle, CheckCircle2, Eye, FileCode2, Loader2, PencilLine, Sparkles } from 'lucide-react';
import { type ReactNode, useCallback, useEffect, useId, useMemo, useState } from 'react';
import { MetadataFieldControl } from '@/components/Backtest/MetadataFieldControl';
import { ReferenceSelectFieldCard } from '@/components/Backtest/ReferenceSelectFieldCard';
import { buildDefaultSignalParams, SignalFieldInputs } from '@/components/Backtest/SignalFieldInputs';
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
import { Label } from '@/components/ui/label';
import { Textarea } from '@/components/ui/textarea';
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
import type {
  AuthoringFieldSchema,
  SignalDefinition,
  SignalFieldDefinition,
  StrategyValidationResponse,
} from '@/types/backtest';
import {
  addFundamentalSignalConfig,
  asStringArray,
  buildDefaultFundamentalConfig as buildDefaultFundamentalConfigFromFields,
  buildSignalOptions,
  buildVisualAdvancedOnlyPaths,
  canVisualizeStrategyConfig,
  deriveFundamentalParentFieldNames,
  getValueAtPath,
  hasValueAtPath,
  isPlainObject,
  normalizeSignalSection,
  parseYamlObject,
  removeFundamentalChildConfig,
  removeValueAtPath,
  safeDumpYaml,
  setValueAtPath,
  updateFundamentalChildConfig,
  updateFundamentalParentConfig,
  updateRegularSignalConfig,
} from './authoringUtils';
import { SignalReferencePanel } from './SignalReferencePanel';

interface StrategyEditorProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  strategyName: string;
  onSuccess?: () => void;
}

type EditorTab = 'visual' | 'advanced' | 'preview';
type SignalSectionKey = 'entry_filter_params' | 'exit_trigger_params';
type VisualSectionKey = 'basics' | 'shared_config' | 'entry_filter' | 'exit_trigger' | 'advanced_only';

const VISUAL_TOP_LEVEL_KEYS = new Set([
  'display_name',
  'description',
  'shared_config',
  'entry_filter_params',
  'exit_trigger_params',
]);

const FUNDAMENTAL_PARENT_FIELD_FALLBACK = ['enabled', 'period_type', 'use_adjusted'];

const executionSemanticsLabels: Record<string, string> = {
  standard: 'Standard',
  next_session_round_trip: 'Next Session Round Trip',
  current_session_round_trip: 'Current Session Round Trip',
  overnight_round_trip: 'Overnight Round Trip',
};

const timingLabels: Record<string, string> = {
  prior_session_close: 'Prior Close',
  current_session_open: 'Current Open',
  current_session_close: 'Current Close',
  next_session_open: 'Next Open',
  current_session: 'Current Session',
  next_session: 'Next Session',
};

function isReferenceSelectField(path: string) {
  return path === 'dataset' || path === 'benchmark_table';
}

function getReferenceSelectCopy(path: string) {
  return path === 'dataset'
    ? { chooserLabel: 'Choose available dataset', placeholderLabel: 'Select a dataset' }
    : { chooserLabel: 'Choose available benchmark', placeholderLabel: 'Select a benchmark' };
}

function formatTimingLabel(value: string) {
  return timingLabels[value] ?? value;
}

function resolveValidationViewState(validationResult: StrategyValidationResponse) {
  const hasValidationErrors = !validationResult.valid;
  const hasValidationWarnings = validationResult.warnings.length > 0;

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

          {validationResult.errors.length > 0 ? (
            <ul className="mt-3 list-disc space-y-1 pl-5 text-sm text-destructive">
              {validationResult.errors.map((error) => (
                <li key={error}>{error}</li>
              ))}
            </ul>
          ) : null}

          {validationResult.warnings.length > 0 ? (
            <ul className="mt-3 list-disc space-y-1 pl-5 text-sm text-amber-700">
              {validationResult.warnings.map((warning) => (
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

function SignalCard({
  definition,
  signalConfig,
  disabled,
  onToggleEnabled,
  onFieldChange,
  onRemove,
}: {
  definition: SignalDefinition;
  signalConfig: Record<string, unknown>;
  disabled?: boolean;
  onToggleEnabled: (enabled: boolean) => void;
  onFieldChange: (field: SignalFieldDefinition, value: unknown) => void;
  onRemove: () => void;
}) {
  const enabledId = useId();
  const enabled = Boolean(
    signalConfig.enabled ?? definition.fields.find((field) => field.name === 'enabled')?.default ?? true
  );

  return (
    <Card className="border-border/60">
      <CardHeader className="pb-4">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div className="space-y-1">
            <CardTitle className="text-base">{definition.name}</CardTitle>
            <CardDescription>{definition.summary ?? definition.description}</CardDescription>
          </div>
          <div className="flex items-center gap-2">
            <Label htmlFor={enabledId} className="text-xs text-muted-foreground">
              Enabled
            </Label>
            <input
              id={enabledId}
              type="checkbox"
              aria-label={`${definition.name} enabled`}
              checked={enabled}
              disabled={disabled}
              onChange={(event) => onToggleEnabled(event.target.checked)}
            />
          </div>
        </div>
        {definition.when_to_use && definition.when_to_use.length > 0 ? (
          <ul className="list-disc space-y-1 pl-5 text-xs text-muted-foreground">
            {definition.when_to_use.map((item) => (
              <li key={item}>{item}</li>
            ))}
          </ul>
        ) : null}
      </CardHeader>
      <CardContent className="space-y-4">
        <SignalFieldInputs
          fields={definition.fields}
          values={signalConfig}
          excludeFields={['enabled']}
          disabled={disabled}
          onFieldChange={onFieldChange}
        />

        {definition.data_requirements.length > 0 ? (
          <div className="text-xs text-muted-foreground">
            Data requirements: {definition.data_requirements.join(', ')}
          </div>
        ) : null}

        {definition.pitfalls && definition.pitfalls.length > 0 ? (
          <div className="rounded-md bg-amber-500/5 p-3 text-xs text-amber-800">{definition.pitfalls.join(' ')}</div>
        ) : null}

        <div className="flex justify-end">
          <Button
            variant="ghost"
            className="text-destructive hover:text-destructive"
            onClick={onRemove}
            disabled={disabled}
          >
            Remove
          </Button>
        </div>
      </CardContent>
    </Card>
  );
}

function buildSignalParentFieldSchema(field: SignalFieldDefinition): AuthoringFieldSchema {
  return {
    path: field.name,
    section: 'shared_config',
    label: field.label ?? field.name,
    group: null,
    type: field.type === 'select' ? 'select' : field.type,
    widget:
      field.type === 'boolean'
        ? 'switch'
        : field.type === 'select'
          ? 'select'
          : field.type === 'number'
            ? 'number'
            : 'text',
    description: field.description,
    summary: null,
    default: field.default,
    options: field.options ?? [],
    constraints: field.constraints,
    placeholder: field.placeholder ?? null,
    unit: field.unit ?? null,
    examples: [],
    required: false,
    advanced_only: false,
  };
}

function StockCodesFieldCard({
  field,
  value,
  overridden,
  onModeChange,
  onChange,
  onReset,
}: {
  field: AuthoringFieldSchema;
  value: unknown;
  overridden: boolean;
  onModeChange: (mode: 'all' | 'custom') => void;
  onChange: (value: string) => void;
  onReset: () => void;
}) {
  const stockCodes = asStringArray(value);
  const customCodes = stockCodes.filter((code) => code !== 'all');
  const stockCodeMode = stockCodes.includes('all') ? 'all' : 'custom';

  return (
    <Card key={field.path} className="border-border/60">
      <CardHeader className="pb-3">
        <CardTitle className="text-base">{field.label}</CardTitle>
        <CardDescription>{field.summary ?? field.description}</CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="flex gap-2">
          <Button variant={stockCodeMode === 'all' ? 'default' : 'outline'} onClick={() => onModeChange('all')}>
            All
          </Button>
          <Button variant={stockCodeMode === 'custom' ? 'default' : 'outline'} onClick={() => onModeChange('custom')}>
            Custom
          </Button>
          <Button variant="outline" onClick={onReset} disabled={!overridden}>
            Reset
          </Button>
        </div>
        {stockCodeMode === 'custom' ? (
          <Textarea
            value={customCodes.join('\n')}
            placeholder="7203\n6758\n9984"
            onChange={(event) => onChange(event.target.value)}
          />
        ) : (
          <div className="rounded-md bg-muted/40 p-3 text-sm text-muted-foreground">
            Entire dataset universe is selected.
          </div>
        )}
      </CardContent>
    </Card>
  );
}

function NativeSelectField({
  label,
  ariaLabel,
  placeholder,
  disabled,
  optionGroups,
  options,
  onSelect,
}: {
  label: string;
  ariaLabel: string;
  placeholder: string;
  disabled?: boolean;
  optionGroups?: Array<{ key: string; label: string; options: Array<{ value: string; label: string }> }>;
  options?: Array<{ value: string; label: string }>;
  onSelect: (value: string) => void;
}) {
  const selectId = useId();

  return (
    <div className="min-w-64">
      <Label htmlFor={selectId} className="mb-1 block text-xs font-medium text-muted-foreground">
        {label}
      </Label>
      <select
        id={selectId}
        aria-label={ariaLabel}
        className="h-10 w-full rounded-md border border-input bg-background px-3 text-sm"
        defaultValue=""
        disabled={disabled}
        onChange={(event) => {
          const selectedValue = event.target.value;
          if (!selectedValue) {
            return;
          }
          onSelect(selectedValue);
          event.currentTarget.value = '';
        }}
      >
        <option value="">{placeholder}</option>
        {optionGroups?.map((group) => (
          <optgroup key={group.key} label={group.label}>
            {group.options.map((option) => (
              <option key={option.value} value={option.value}>
                {option.label}
              </option>
            ))}
          </optgroup>
        ))}
        {options?.map((option) => (
          <option key={option.value} value={option.value}>
            {option.label}
          </option>
        ))}
      </select>
    </div>
  );
}

function FundamentalParentSettingsGrid({
  sectionKey,
  fields,
  values,
  onChange,
}: {
  sectionKey: SignalSectionKey;
  fields: SignalFieldDefinition[];
  values: Record<string, unknown>;
  onChange: (field: SignalFieldDefinition, value: unknown) => void;
}) {
  return (
    <div className="grid gap-3 lg:grid-cols-3">
      {fields.map((field) => (
        <MetadataFieldControl
          key={`${sectionKey}.fundamental.${field.name}`}
          field={buildSignalParentFieldSchema(field)}
          value={values[field.name] ?? field.default}
          onChange={(value) => onChange(field, value)}
        />
      ))}
    </div>
  );
}

// biome-ignore lint/complexity/noExcessiveCognitiveComplexity: this dialog coordinates YAML round-trip, backend validation, and visual editor state in one place.
export function StrategyEditor({ open, onOpenChange, strategyName, onSuccess }: StrategyEditorProps) {
  const [activeTab, setActiveTab] = useState<EditorTab>('visual');
  const [activeVisualSection, setActiveVisualSection] = useState<VisualSectionKey>('basics');
  const [draftConfig, setDraftConfig] = useState<Record<string, unknown>>({});
  const [yamlContent, setYamlContent] = useState('');
  const [parseError, setParseError] = useState<string | null>(null);
  const [validationResult, setValidationResult] = useState<StrategyValidationResponse | null>(null);
  const [previewDirty, setPreviewDirty] = useState(true);

  const strategyContextQuery = useStrategyEditorContext(open ? strategyName : null);
  const referenceQuery = useStrategyEditorReference(open);
  const signalReferenceQuery = useSignalReference();
  const { data: datasets } = useDatasets();
  const { data: indices } = useIndicesList();
  const updateStrategy = useUpdateStrategy();
  const validateStrategy = useValidateStrategy();
  const visualSectionIdPrefix = useId();

  const applyDraftConfig = useCallback((nextConfig: Record<string, unknown>) => {
    setDraftConfig(nextConfig);
    setYamlContent(safeDumpYaml(nextConfig));
    setParseError(null);
    setPreviewDirty(true);
  }, []);

  useEffect(() => {
    if (!open || !strategyContextQuery.data) {
      return;
    }
    setActiveTab('visual');
    setActiveVisualSection('basics');
    setValidationResult(null);
    setPreviewDirty(true);
    applyDraftConfig(strategyContextQuery.data.raw_config);
  }, [applyDraftConfig, open, strategyContextQuery.data]);

  const definitions = signalReferenceQuery.data?.signals ?? [];
  const categories = signalReferenceQuery.data?.categories ?? [];

  const definitionsByType = useMemo(
    () => new Map(definitions.map((definition) => [definition.signal_type, definition])),
    [definitions]
  );
  const regularDefinitions = useMemo(
    () => definitions.filter((definition) => !definition.key.startsWith('fundamental_')),
    [definitions]
  );
  const fundamentalDefinitions = useMemo(
    () => definitions.filter((definition) => definition.key.startsWith('fundamental_')),
    [definitions]
  );
  const fundamentalDefinitionsByType = useMemo(
    () => new Map(fundamentalDefinitions.map((definition) => [definition.signal_type, definition])),
    [fundamentalDefinitions]
  );
  const fundamentalParentFieldNames = useMemo(
    () => deriveFundamentalParentFieldNames(fundamentalDefinitions, FUNDAMENTAL_PARENT_FIELD_FALLBACK),
    [fundamentalDefinitions]
  );

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
  const exitSignals = normalizeSignalSection(draftConfig.exit_trigger_params);
  const currentExecutionMode = String(
    hasValueAtPath(rawSharedConfig, 'execution_policy.mode')
      ? getValueAtPath(rawSharedConfig, 'execution_policy.mode')
      : (getValueAtPath(defaultSharedConfig, 'execution_policy.mode') ?? 'standard')
  );
  const exitSectionDisabled = currentExecutionMode !== 'standard';

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

  const datasetOptionValues = useMemo(() => {
    const values = new Set<string>();
    const inheritedValue = hasValueAtPath(rawSharedConfig, 'dataset')
      ? getValueAtPath(rawSharedConfig, 'dataset')
      : getValueAtPath(defaultSharedConfig, 'dataset');
    if (typeof inheritedValue === 'string' && inheritedValue.length > 0) {
      values.add(inheritedValue);
    }
    for (const dataset of datasets ?? []) {
      values.add(dataset.name);
    }
    return Array.from(values);
  }, [datasets, defaultSharedConfig, rawSharedConfig]);

  const benchmarkOptionValues = useMemo(() => {
    const values = new Set<string>();
    const inheritedValue = hasValueAtPath(rawSharedConfig, 'benchmark_table')
      ? getValueAtPath(rawSharedConfig, 'benchmark_table')
      : getValueAtPath(defaultSharedConfig, 'benchmark_table');
    if (typeof inheritedValue === 'string' && inheritedValue.length > 0) {
      values.add(inheritedValue);
    }
    for (const item of indices?.indices ?? []) {
      values.add(item.code);
    }
    return Array.from(values);
  }, [defaultSharedConfig, indices?.indices, rawSharedConfig]);

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
    [visualSectionIdPrefix]
  );

  const updateDraftAtPath = useCallback(
    (path: string, value: unknown) => {
      applyDraftConfig(setValueAtPath(draftConfig, path, value));
    },
    [applyDraftConfig, draftConfig]
  );

  const removeDraftPath = useCallback(
    (path: string) => {
      applyDraftConfig(removeValueAtPath(draftConfig, path));
    },
    [applyDraftConfig, draftConfig]
  );

  const updateSharedConfigField = useCallback(
    (field: AuthoringFieldSchema, value: unknown) => {
      updateDraftAtPath(`shared_config.${field.path}`, value);
    },
    [updateDraftAtPath]
  );

  const handleStockCodesModeChange = useCallback(
    (mode: 'all' | 'custom') => {
      if (mode === 'all') {
        updateDraftAtPath('shared_config.stock_codes', ['all']);
        return;
      }
      const current = asStringArray(getValueAtPath(rawSharedConfig, 'stock_codes'));
      const customCodes = current.filter((code) => code !== 'all');
      updateDraftAtPath('shared_config.stock_codes', customCodes.length > 0 ? customCodes : []);
    },
    [rawSharedConfig, updateDraftAtPath]
  );

  const buildSelectableSignalOptions = useCallback(
    (sectionKey: SignalSectionKey) => {
      return buildSignalOptions(
        normalizeSignalSection(draftConfig[sectionKey]),
        categories,
        regularDefinitions,
        sectionKey
      );
    },
    [categories, draftConfig, regularDefinitions]
  );

  const addRegularSignal = useCallback(
    (sectionKey: SignalSectionKey, signalType: string) => {
      const definition = definitionsByType.get(signalType);
      if (!definition) return;
      updateDraftAtPath(`${sectionKey}.${signalType}`, buildDefaultSignalParams(definition));
    },
    [definitionsByType, updateDraftAtPath]
  );

  const updateRegularSignalField = useCallback(
    (sectionKey: SignalSectionKey, signalType: string, field: SignalFieldDefinition, value: unknown) => {
      const nextSignal = updateRegularSignalConfig(
        normalizeSignalSection(draftConfig[sectionKey]),
        signalType,
        field,
        value
      );
      updateDraftAtPath(`${sectionKey}.${signalType}`, nextSignal);
    },
    [draftConfig, updateDraftAtPath]
  );

  const removeRegularSignal = useCallback(
    (sectionKey: SignalSectionKey, signalType: string) => {
      removeDraftPath(`${sectionKey}.${signalType}`);
    },
    [removeDraftPath]
  );

  const fundamentalParentFields = useMemo(() => {
    const firstDefinition = fundamentalDefinitions[0];
    if (!firstDefinition) return [];
    return firstDefinition.fields.filter((field) => fundamentalParentFieldNames.includes(field.name));
  }, [fundamentalDefinitions, fundamentalParentFieldNames]);

  const buildDefaultFundamentalConfig = useCallback(() => {
    return buildDefaultFundamentalConfigFromFields(fundamentalParentFields);
  }, [fundamentalParentFields]);

  const addFundamentalSignal = useCallback(
    (sectionKey: SignalSectionKey, childKey: string) => {
      const definition = fundamentalDefinitionsByType.get(childKey);
      if (!definition) return;

      const currentFundamental = normalizeSignalSection(normalizeSignalSection(draftConfig[sectionKey]).fundamental);
      const nextFundamental = addFundamentalSignalConfig(
        currentFundamental,
        childKey,
        definition,
        fundamentalParentFieldNames,
        buildDefaultFundamentalConfig()
      );
      updateDraftAtPath(`${sectionKey}.fundamental`, nextFundamental);
    },
    [
      buildDefaultFundamentalConfig,
      draftConfig,
      fundamentalDefinitionsByType,
      fundamentalParentFieldNames,
      updateDraftAtPath,
    ]
  );

  const updateFundamentalParentField = useCallback(
    (sectionKey: SignalSectionKey, field: SignalFieldDefinition, value: unknown) => {
      const currentFundamental = normalizeSignalSection(normalizeSignalSection(draftConfig[sectionKey]).fundamental);
      const nextFundamental = updateFundamentalParentConfig(
        currentFundamental,
        field,
        value,
        buildDefaultFundamentalConfig()
      );
      updateDraftAtPath(`${sectionKey}.fundamental`, nextFundamental);
    },
    [buildDefaultFundamentalConfig, draftConfig, updateDraftAtPath]
  );

  const updateFundamentalChildField = useCallback(
    (sectionKey: SignalSectionKey, childKey: string, field: SignalFieldDefinition, value: unknown) => {
      const currentFundamental = normalizeSignalSection(normalizeSignalSection(draftConfig[sectionKey]).fundamental);
      const nextFundamental = updateFundamentalChildConfig(
        currentFundamental,
        childKey,
        field,
        value,
        buildDefaultFundamentalConfig()
      );
      updateDraftAtPath(`${sectionKey}.fundamental`, nextFundamental);
    },
    [buildDefaultFundamentalConfig, draftConfig, updateDraftAtPath]
  );

  const removeFundamentalChild = useCallback(
    (sectionKey: SignalSectionKey, childKey: string) => {
      const currentFundamental = normalizeSignalSection(normalizeSignalSection(draftConfig[sectionKey]).fundamental);
      const { nextFundamental, shouldRemoveSection } = removeFundamentalChildConfig(
        currentFundamental,
        childKey,
        fundamentalParentFieldNames
      );

      if (shouldRemoveSection) {
        removeDraftPath(`${sectionKey}.fundamental`);
        return;
      }

      updateDraftAtPath(`${sectionKey}.fundamental`, nextFundamental);
    },
    [draftConfig, fundamentalParentFieldNames, removeDraftPath, updateDraftAtPath]
  );

  const resolveCurrentConfig = useCallback(() => {
    if (activeTab === 'advanced') {
      const parsed = parseYamlObject(yamlContent);
      setParseError(parsed.error);
      return parsed.value;
    }
    return draftConfig;
  }, [activeTab, draftConfig, yamlContent]);

  const runBackendValidation = useCallback(
    async (switchToPreview = false) => {
      const config = resolveCurrentConfig();
      if (!config) {
        return null;
      }
      try {
        const result = await validateStrategy.mutateAsync({
          name: strategyName,
          request: { config },
        });
        setValidationResult(result);
        setPreviewDirty(false);
        if (switchToPreview) {
          setActiveTab('preview');
        }
        return result;
      } catch (error) {
        const message = error instanceof Error ? error.message : 'Unknown validation error';
        const failedResult: StrategyValidationResponse = {
          valid: false,
          errors: [`Validation request failed: ${message}`],
          warnings: [],
        };
        setValidationResult(failedResult);
        setPreviewDirty(false);
        if (switchToPreview) {
          setActiveTab('preview');
        }
        return failedResult;
      }
    },
    [resolveCurrentConfig, strategyName, validateStrategy]
  );

  const handleSave = useCallback(async () => {
    const config = resolveCurrentConfig();
    if (!config) return;

    const result = await runBackendValidation();
    if (!result?.valid) {
      return;
    }

    updateStrategy.mutate(
      { name: strategyName, request: { config } },
      {
        onSuccess: () => {
          onOpenChange(false);
          onSuccess?.();
        },
      }
    );
  }, [onOpenChange, onSuccess, resolveCurrentConfig, runBackendValidation, strategyName, updateStrategy]);

  const handleOpenChange = useCallback(
    (nextOpen: boolean) => {
      if (!nextOpen) {
        updateStrategy.reset();
        validateStrategy.reset();
        setParseError(null);
        setValidationResult(null);
        setPreviewDirty(true);
        setActiveTab('visual');
      }
      onOpenChange(nextOpen);
    },
    [onOpenChange, updateStrategy, validateStrategy]
  );

  const handleYamlChange = useCallback((value: string) => {
    setYamlContent(value);
    setPreviewDirty(true);
    const parsed = parseYamlObject(value);
    setParseError(parsed.error);
    if (parsed.value) {
      setDraftConfig(parsed.value);
    }
  }, []);

  const handleTabChange = useCallback(
    (tab: EditorTab) => {
      if (tab === 'visual') {
        const parsed = parseYamlObject(yamlContent);
        if (!parsed.value) {
          setParseError(parsed.error);
          return;
        }
        const compatibilityError = canVisualizeStrategyConfig(parsed.value);
        if (compatibilityError) {
          setParseError(compatibilityError);
          return;
        }
        setParseError(null);
        setDraftConfig(parsed.value);
        setActiveTab('visual');
        return;
      }

      if (tab === 'preview') {
        setActiveTab('preview');
        void runBackendValidation(false);
        return;
      }

      setActiveTab(tab);
    },
    [runBackendValidation, yamlContent]
  );

  const handleCopySnippet = useCallback(
    (snippet: string) => {
      const nextContent = yamlContent.trim() ? `${yamlContent.trimEnd()}\n\n${snippet}` : snippet;
      setActiveTab('advanced');
      handleYamlChange(nextContent);
    },
    [handleYamlChange, yamlContent]
  );

  const isLoading = strategyContextQuery.isLoading || referenceQuery.isLoading || signalReferenceQuery.isLoading;
  const strategyCategory = context?.category ?? 'unknown';
  const updateErrorMessage = updateStrategy.isError ? updateStrategy.error.message : null;
  const datasetName =
    typeof getValueAtPath(rawSharedConfig, 'dataset') === 'string'
      ? (getValueAtPath(rawSharedConfig, 'dataset') as string)
      : typeof getValueAtPath(defaultSharedConfig, 'dataset') === 'string'
        ? (getValueAtPath(defaultSharedConfig, 'dataset') as string)
        : null;
  const datasetInfo = useDatasetInfo(open ? datasetName : null);

  const getSharedFieldOptionValues = useCallback(
    (path: string) => {
      if (path === 'dataset') {
        return datasetOptionValues;
      }
      if (path === 'benchmark_table') {
        return benchmarkOptionValues;
      }
      return undefined;
    },
    [benchmarkOptionValues, datasetOptionValues]
  );

  const renderStockCodesField = useCallback(
    (field: AuthoringFieldSchema, value: unknown, overridden: boolean) => (
      <StockCodesFieldCard
        field={field}
        value={value}
        overridden={overridden}
        onModeChange={handleStockCodesModeChange}
        onChange={(nextValue) =>
          updateDraftAtPath(
            'shared_config.stock_codes',
            nextValue
              .split(/[\n,]/)
              .map((item) => item.trim())
              .filter((item) => item.length > 0)
          )
        }
        onReset={() => removeDraftPath('shared_config.stock_codes')}
      />
    ),
    [handleStockCodesModeChange, removeDraftPath, updateDraftAtPath]
  );

  const renderReferenceSharedField = useCallback(
    (field: AuthoringFieldSchema, value: unknown, overridden: boolean, optionValues: string[]) => {
      const copy = getReferenceSelectCopy(field.path);
      return (
        <ReferenceSelectFieldCard
          field={field}
          value={value}
          effectiveValue={value}
          overridden={overridden}
          optionValues={optionValues}
          chooserLabel={copy.chooserLabel}
          placeholderLabel={copy.placeholderLabel}
          onChange={(nextValue) => updateSharedConfigField(field, nextValue)}
          onReset={() => removeDraftPath(`shared_config.${field.path}`)}
        />
      );
    },
    [removeDraftPath, updateSharedConfigField]
  );

  const renderSharedField = (field: AuthoringFieldSchema) => {
    if (!context) return null;

    const overridden = hasValueAtPath(rawSharedConfig, field.path);
    const value = overridden
      ? getValueAtPath(rawSharedConfig, field.path)
      : getValueAtPath(defaultSharedConfig, field.path);

    const optionValues = getSharedFieldOptionValues(field.path);

    if (field.path === 'stock_codes') {
      return renderStockCodesField(field, value, overridden);
    }

    if (isReferenceSelectField(field.path)) {
      return renderReferenceSharedField(field, value, overridden, optionValues ?? []);
    }

    return (
      <MetadataFieldControl
        key={field.path}
        field={field}
        value={value}
        effectiveValue={value}
        overridden={overridden}
        optionValues={optionValues}
        onChange={(nextValue) => updateSharedConfigField(field, nextValue)}
        onReset={() => removeDraftPath(`shared_config.${field.path}`)}
      />
    );
  };

  const renderSignalSection = (sectionKey: SignalSectionKey, title: string) => {
    const section = normalizeSignalSection(draftConfig[sectionKey]);
    const regularSignalEntries = Object.entries(section).filter(
      ([signalKey, signalValue]) =>
        signalKey !== 'fundamental' && definitionsByType.has(signalKey) && isPlainObject(signalValue)
    );
    const availableOptions = buildSelectableSignalOptions(sectionKey);
    const fundamentalSection = normalizeSignalSection(section.fundamental);
    const configuredFundamentalChildren = Object.keys(fundamentalSection).filter(
      (key) => !fundamentalParentFieldNames.includes(key) && fundamentalDefinitionsByType.has(key)
    );
    const availableFundamentalSignals = fundamentalDefinitions.filter(
      (definition) =>
        !configuredFundamentalChildren.includes(definition.signal_type) &&
        (sectionKey === 'entry_filter_params' || !definition.exit_disabled)
    );
    const sectionDisabled = sectionKey === 'exit_trigger_params' && exitSectionDisabled;

    return (
      <Card className="border-border/60">
        <CardHeader className="pb-4">
          <div className="flex flex-wrap items-start justify-between gap-3">
            <div className="space-y-1">
              <CardTitle className="text-lg">{title}</CardTitle>
              <CardDescription>
                {sectionKey === 'entry_filter_params'
                  ? 'Add signals from grouped categories and edit them visually.'
                  : 'Exit signals are available only in standard execution mode.'}
              </CardDescription>
            </div>
            <NativeSelectField
              label="Add signal"
              ariaLabel={`Add ${title}`}
              placeholder="Select a signal…"
              disabled={sectionDisabled}
              optionGroups={availableOptions.map(({ category, signals }) => ({
                key: category.key,
                label: category.label,
                options: signals.map((definition) => ({
                  value: definition.signal_type,
                  label: definition.name,
                })),
              }))}
              onSelect={(signalType) => addRegularSignal(sectionKey, signalType)}
            />
          </div>
        </CardHeader>
        <CardContent className="space-y-4">
          {sectionDisabled ? (
            <div className="rounded-lg border border-amber-500/20 bg-amber-500/10 p-4 text-sm text-amber-800">
              Execution policy <strong>{executionSemanticsLabels[currentExecutionMode] ?? currentExecutionMode}</strong>{' '}
              disables exit triggers. Save an empty object for <code>exit_trigger_params</code> or clear it now.
              {Object.keys(exitSignals).length > 0 ? (
                <div className="mt-3">
                  <Button variant="outline" onClick={() => updateDraftAtPath('exit_trigger_params', {})}>
                    Clear Exit Config
                  </Button>
                </div>
              ) : null}
            </div>
          ) : null}

          {regularSignalEntries.map(([signalKey, signalValue]) => {
            const definition = definitionsByType.get(signalKey);
            if (!definition || !isPlainObject(signalValue)) return null;
            return (
              <SignalCard
                key={signalKey}
                definition={definition}
                signalConfig={signalValue}
                disabled={sectionDisabled}
                onToggleEnabled={(enabled) =>
                  updateRegularSignalField(
                    sectionKey,
                    signalKey,
                    { name: 'enabled', type: 'boolean', description: '' },
                    enabled
                  )
                }
                onFieldChange={(field, value) => updateRegularSignalField(sectionKey, signalKey, field, value)}
                onRemove={() => removeRegularSignal(sectionKey, signalKey)}
              />
            );
          })}

          <Card className="border-dashed border-border/60">
            <CardHeader className="pb-4">
              <div className="flex flex-wrap items-start justify-between gap-3">
                <div className="space-y-1">
                  <CardTitle className="text-base">Fundamental Signals</CardTitle>
                  <CardDescription>Parent settings are shared. Child cards control individual factors.</CardDescription>
                </div>
                <NativeSelectField
                  label="Add fundamental factor"
                  ariaLabel={`Add ${title} fundamental signal`}
                  placeholder="Select a factor…"
                  disabled={sectionDisabled || availableFundamentalSignals.length === 0}
                  options={availableFundamentalSignals.map((definition) => ({
                    value: definition.signal_type,
                    label: definition.name,
                  }))}
                  onSelect={(childKey) => addFundamentalSignal(sectionKey, childKey)}
                />
              </div>
            </CardHeader>
            <CardContent className="space-y-4">
              {Object.keys(fundamentalSection).length > 0 ? (
                <>
                  <FundamentalParentSettingsGrid
                    sectionKey={sectionKey}
                    fields={fundamentalParentFields}
                    values={fundamentalSection}
                    onChange={(field, value) => updateFundamentalParentField(sectionKey, field, value)}
                  />

                  {configuredFundamentalChildren.map((childKey) => {
                    const definition = fundamentalDefinitionsByType.get(childKey);
                    const childConfig = normalizeSignalSection(fundamentalSection[childKey]);
                    if (!definition) return null;

                    return (
                      <SignalCard
                        key={`${sectionKey}.fundamental.${childKey}`}
                        definition={definition}
                        signalConfig={childConfig}
                        disabled={sectionDisabled}
                        onToggleEnabled={(enabled) =>
                          updateFundamentalChildField(
                            sectionKey,
                            childKey,
                            { name: 'enabled', type: 'boolean', description: '' },
                            enabled
                          )
                        }
                        onFieldChange={(field, value) =>
                          updateFundamentalChildField(sectionKey, childKey, field, value)
                        }
                        onRemove={() => removeFundamentalChild(sectionKey, childKey)}
                      />
                    );
                  })}
                </>
              ) : (
                <div className="rounded-lg bg-muted/40 p-4 text-sm text-muted-foreground">
                  No fundamental filters configured in this section.
                </div>
              )}
            </CardContent>
          </Card>

          {regularSignalEntries.length === 0 && Object.keys(fundamentalSection).length === 0 ? (
            <div className="rounded-lg bg-muted/40 p-4 text-sm text-muted-foreground">
              No signals configured yet. Add a signal from the dropdown above.
            </div>
          ) : null}
        </CardContent>
      </Card>
    );
  };

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
                isRefreshing={validateStrategy.isPending}
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
            disabled={validateStrategy.isPending || updateStrategy.isPending}
          >
            {validateStrategy.isPending ? (
              <>
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                Validating...
              </>
            ) : (
              'Validate'
            )}
          </Button>
          <Button onClick={handleSave} disabled={validateStrategy.isPending || updateStrategy.isPending || isLoading}>
            {updateStrategy.isPending ? (
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
