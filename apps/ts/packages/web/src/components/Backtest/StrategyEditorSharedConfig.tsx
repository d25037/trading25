import type { AuthoringFieldSchema } from '@trading25/api-clients/backtest';
import { useCallback, useMemo } from 'react';
import { MetadataFieldControl } from '@/components/Backtest/MetadataFieldControl';
import { ReferenceSelectFieldCard } from '@/components/Backtest/ReferenceSelectFieldCard';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Textarea } from '@/components/ui/textarea';
import { asStringArray, getValueAtPath, hasValueAtPath } from './authoringUtils';

function isReferenceSelectField(path: string) {
  return path === 'universe_preset' || path === 'dataset_snapshot' || path === 'benchmark_table';
}

function getReferenceSelectCopy(path: string) {
  return path === 'universe_preset'
    ? { chooserLabel: 'Choose universe preset', placeholderLabel: 'Select a universe preset' }
    : path === 'dataset_snapshot'
      ? { chooserLabel: 'Choose archived dataset snapshot', placeholderLabel: 'Select a dataset snapshot' }
      : { chooserLabel: 'Choose available benchmark', placeholderLabel: 'Select a benchmark' };
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
            Entire PIT universe is selected.
          </div>
        )}
      </CardContent>
    </Card>
  );
}

interface SharedFieldRenderContext {
  contextReady: boolean;
  rawSharedConfig: Record<string, unknown>;
  defaultSharedConfig: Record<string, unknown>;
  benchmarkOptionValues: string[];
  datasetSnapshotOptionValues: string[];
  universePresetOptionValues: string[];
  handleStockCodesModeChange: (mode: 'all' | 'custom') => void;
  removeDraftPath: (path: string) => void;
  updateDraftAtPath: (path: string, value: unknown) => void;
  updateSharedConfigField: (field: AuthoringFieldSchema, value: unknown) => void;
}

function getSharedFieldOptionValues(context: SharedFieldRenderContext, path: string) {
  if (path === 'universe_preset') {
    return context.universePresetOptionValues;
  }
  if (path === 'dataset_snapshot') {
    return context.datasetSnapshotOptionValues;
  }
  if (path === 'benchmark_table') {
    return context.benchmarkOptionValues;
  }
  return undefined;
}

function renderStockCodesSharedField(
  field: AuthoringFieldSchema,
  value: unknown,
  overridden: boolean,
  context: SharedFieldRenderContext
) {
  return (
    <StockCodesFieldCard
      field={field}
      value={value}
      overridden={overridden}
      onModeChange={context.handleStockCodesModeChange}
      onChange={(nextValue) =>
        context.updateDraftAtPath(
          'shared_config.stock_codes',
          nextValue
            .split(/[\n,]/)
            .map((item) => item.trim())
            .filter((item) => item.length > 0)
        )
      }
      onReset={() => context.removeDraftPath('shared_config.stock_codes')}
    />
  );
}

function renderReferenceSharedField(
  field: AuthoringFieldSchema,
  value: unknown,
  overridden: boolean,
  optionValues: string[],
  context: SharedFieldRenderContext
) {
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
      onChange={(nextValue) => context.updateSharedConfigField(field, nextValue)}
      onReset={() => context.removeDraftPath(`shared_config.${field.path}`)}
    />
  );
}

function renderSharedFieldControl(field: AuthoringFieldSchema, context: SharedFieldRenderContext) {
  if (!context.contextReady) return null;

  const overridden = hasValueAtPath(context.rawSharedConfig, field.path);
  const value = overridden
    ? getValueAtPath(context.rawSharedConfig, field.path)
    : getValueAtPath(context.defaultSharedConfig, field.path);
  const optionValues = getSharedFieldOptionValues(context, field.path);

  if (field.path === 'stock_codes') {
    return renderStockCodesSharedField(field, value, overridden, context);
  }

  if (isReferenceSelectField(field.path)) {
    return renderReferenceSharedField(field, value, overridden, optionValues ?? [], context);
  }

  return (
    <MetadataFieldControl
      key={field.path}
      field={field}
      value={value}
      effectiveValue={value}
      overridden={overridden}
      optionValues={optionValues}
      onChange={(nextValue) => context.updateSharedConfigField(field, nextValue)}
      onReset={() => context.removeDraftPath(`shared_config.${field.path}`)}
    />
  );
}

function resolveDatasetSnapshotName(
  rawSharedConfig: Record<string, unknown>,
  defaultSharedConfig: Record<string, unknown>
): string | null {
  const explicitDataset = getValueAtPath(rawSharedConfig, 'dataset_snapshot');
  if (typeof explicitDataset === 'string') {
    return explicitDataset;
  }
  const defaultDataset = getValueAtPath(defaultSharedConfig, 'dataset_snapshot');
  return typeof defaultDataset === 'string' ? defaultDataset : null;
}

interface UseStrategyEditorSharedConfigFieldsParams {
  contextReady: boolean;
  datasets?: Array<{ name: string }>;
  defaultSharedConfig: Record<string, unknown>;
  indices?: { indices?: Array<{ code: string }> };
  rawSharedConfig: Record<string, unknown>;
  removeDraftPath: (path: string) => void;
  updateDraftAtPath: (path: string, value: unknown) => void;
}

export function useStrategyEditorSharedConfigFields({
  contextReady,
  datasets,
  defaultSharedConfig,
  indices,
  rawSharedConfig,
  removeDraftPath,
  updateDraftAtPath,
}: UseStrategyEditorSharedConfigFieldsParams) {
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

  const universePresetOptionValues = useMemo(() => {
    const values = new Set<string>();
    const inheritedValue = hasValueAtPath(rawSharedConfig, 'universe_preset')
      ? getValueAtPath(rawSharedConfig, 'universe_preset')
      : getValueAtPath(defaultSharedConfig, 'universe_preset');
    if (typeof inheritedValue === 'string' && inheritedValue.length > 0) {
      values.add(inheritedValue);
    }
    for (const preset of ['prime', 'standard', 'growth', 'topix100', 'primeExTopix500']) {
      values.add(preset);
    }
    return Array.from(values);
  }, [defaultSharedConfig, rawSharedConfig]);

  const datasetSnapshotOptionValues = useMemo(() => {
    const values = new Set<string>();
    const inheritedValue = hasValueAtPath(rawSharedConfig, 'dataset_snapshot')
      ? getValueAtPath(rawSharedConfig, 'dataset_snapshot')
      : getValueAtPath(defaultSharedConfig, 'dataset_snapshot');
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

  const renderSharedField = useCallback(
    (field: AuthoringFieldSchema) =>
      renderSharedFieldControl(field, {
        contextReady,
        rawSharedConfig,
        defaultSharedConfig,
        benchmarkOptionValues,
        datasetSnapshotOptionValues,
        universePresetOptionValues,
        handleStockCodesModeChange,
        removeDraftPath,
        updateDraftAtPath,
        updateSharedConfigField,
      }),
    [
      benchmarkOptionValues,
      contextReady,
      datasetSnapshotOptionValues,
      defaultSharedConfig,
      handleStockCodesModeChange,
      rawSharedConfig,
      removeDraftPath,
      universePresetOptionValues,
      updateDraftAtPath,
      updateSharedConfigField,
    ]
  );

  return {
    datasetSnapshotName: resolveDatasetSnapshotName(rawSharedConfig, defaultSharedConfig),
    renderSharedField,
  };
}
