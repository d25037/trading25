import { FileCode2, Loader2, PencilLine, Settings } from 'lucide-react';
import { type ReactNode, useCallback, useEffect, useMemo, useState } from 'react';
import { MetadataFieldControl } from '@/components/Backtest/MetadataFieldControl';
import { ReferenceSelectFieldCard } from '@/components/Backtest/ReferenceSelectFieldCard';
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
  useDefaultConfigEditorContext,
  useStrategyEditorReference,
  useUpdateDefaultConfig,
  useUpdateDefaultConfigStructured,
} from '@/hooks/useBacktest';
import { useDatasets } from '@/hooks/useDataset';
import { useIndicesList } from '@/hooks/useIndices';
import { cn } from '@/lib/utils';
import type { AuthoringFieldSchema } from '@/types/backtest';
import { buildDefaultDocumentAdvancedOnlyPaths, canVisualizeDefaultDocument } from './authoringDocumentUtils';
import {
  getValueAtPath,
  hasValueAtPath,
  isPlainObject,
  parseYamlObject,
  removeValueAtPath,
  safeDumpYaml,
  setValueAtPath,
} from './authoringUtils';

interface DefaultConfigEditorProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

type DefaultEditorTab = 'visual' | 'advanced';

function isReferenceSelectField(path: string) {
  return path === 'dataset' || path === 'benchmark_table';
}

function getReferenceSelectCopy(path: string) {
  return path === 'dataset'
    ? { chooserLabel: 'Choose available dataset', placeholderLabel: 'Select a dataset' }
    : { chooserLabel: 'Choose available benchmark', placeholderLabel: 'Select a benchmark' };
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

// biome-ignore lint/complexity/noExcessiveCognitiveComplexity: the dialog coordinates raw YAML and structured default-config editing against two save paths.
export function DefaultConfigEditor({ open, onOpenChange }: DefaultConfigEditorProps) {
  const [activeTab, setActiveTab] = useState<DefaultEditorTab>('visual');
  const [draftDocument, setDraftDocument] = useState<Record<string, unknown>>({});
  const [yamlContent, setYamlContent] = useState('');
  const [parseError, setParseError] = useState<string | null>(null);

  const contextQuery = useDefaultConfigEditorContext(open);
  const referenceQuery = useStrategyEditorReference(open);
  const { data: datasets } = useDatasets();
  const { data: indices } = useIndicesList();
  const updateDefaultConfig = useUpdateDefaultConfig();
  const updateDefaultConfigStructured = useUpdateDefaultConfigStructured();

  const applyDraftDocument = useCallback((nextDocument: Record<string, unknown>) => {
    setDraftDocument(nextDocument);
    setYamlContent(safeDumpYaml(nextDocument));
    setParseError(null);
  }, []);

  useEffect(() => {
    if (!open || !contextQuery.data) return;
    setActiveTab('visual');
    setParseError(null);
    setDraftDocument(contextQuery.data.raw_document);
    setYamlContent(contextQuery.data.raw_yaml);
  }, [contextQuery.data, open]);

  const reference = referenceQuery.data;
  const defaultSection = isPlainObject(draftDocument.default) ? draftDocument.default : {};
  const execution = isPlainObject(defaultSection.execution) ? defaultSection.execution : {};
  const parameters = isPlainObject(defaultSection.parameters) ? defaultSection.parameters : {};
  const sharedConfig = isPlainObject(parameters.shared_config) ? parameters.shared_config : {};

  const advancedOnlyPaths = useMemo(() => buildDefaultDocumentAdvancedOnlyPaths(draftDocument), [draftDocument]);

  const datasetOptionValues = useMemo(() => {
    const values = new Set<string>();
    const currentValue = getValueAtPath(sharedConfig, 'dataset');
    if (typeof currentValue === 'string' && currentValue.length > 0) {
      values.add(currentValue);
    }
    for (const dataset of datasets ?? []) {
      values.add(dataset.name);
    }
    return Array.from(values);
  }, [datasets, sharedConfig]);

  const benchmarkOptionValues = useMemo(() => {
    const values = new Set<string>();
    const currentValue = getValueAtPath(sharedConfig, 'benchmark_table');
    if (typeof currentValue === 'string' && currentValue.length > 0) {
      values.add(currentValue);
    }
    for (const item of indices?.indices ?? []) {
      values.add(item.code);
    }
    return Array.from(values);
  }, [indices?.indices, sharedConfig]);

  const updateDraftAtPath = useCallback(
    (path: string, value: unknown) => {
      applyDraftDocument(setValueAtPath(draftDocument, path, value));
    },
    [applyDraftDocument, draftDocument]
  );

  const removeDraftPath = useCallback(
    (path: string) => {
      applyDraftDocument(removeValueAtPath(draftDocument, path));
    },
    [applyDraftDocument, draftDocument]
  );

  const handleYamlChange = useCallback((value: string) => {
    setYamlContent(value);
    const parsed = parseYamlObject(value);
    setParseError(parsed.error);
    if (parsed.value) {
      setDraftDocument(parsed.value);
    }
  }, []);

  const handleTabChange = useCallback(
    (tab: DefaultEditorTab) => {
      if (tab === 'visual') {
        const parsed = parseYamlObject(yamlContent);
        if (!parsed.value) {
          setParseError(parsed.error);
          return;
        }
        const compatibilityError = canVisualizeDefaultDocument(parsed.value);
        if (compatibilityError) {
          setParseError(compatibilityError);
          return;
        }
        setParseError(null);
        setDraftDocument(parsed.value);
      }

      setActiveTab(tab);
    },
    [yamlContent]
  );

  const handleOpenChange = useCallback(
    (nextOpen: boolean) => {
      if (!nextOpen) {
        updateDefaultConfig.reset();
        updateDefaultConfigStructured.reset();
        setParseError(null);
        setActiveTab('visual');
      }
      onOpenChange(nextOpen);
    },
    [onOpenChange, updateDefaultConfig, updateDefaultConfigStructured]
  );

  const getFieldOptionValues = useCallback(
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

  const renderReferenceField = useCallback(
    (field: AuthoringFieldSchema, pathPrefix: 'default.execution' | 'default.parameters.shared_config') => {
      const scopedPath = `${pathPrefix}.${field.path}`;
      const copy = getReferenceSelectCopy(field.path);

      return (
        <ReferenceSelectFieldCard
          key={scopedPath}
          field={field}
          value={getValueAtPath(draftDocument, scopedPath) ?? field.default}
          effectiveValue={field.default}
          overridden={hasValueAtPath(draftDocument, scopedPath)}
          optionValues={getFieldOptionValues(field.path) ?? []}
          chooserLabel={copy.chooserLabel}
          placeholderLabel={copy.placeholderLabel}
          onChange={(value) => updateDraftAtPath(scopedPath, value)}
          onReset={() => removeDraftPath(scopedPath)}
        />
      );
    },
    [draftDocument, getFieldOptionValues, removeDraftPath, updateDraftAtPath]
  );

  const renderField = useCallback(
    (field: AuthoringFieldSchema, pathPrefix: 'default.execution' | 'default.parameters.shared_config') => {
      const scopedPath = `${pathPrefix}.${field.path}`;
      const scopedValue = getValueAtPath(draftDocument, scopedPath);
      const optionValues = getFieldOptionValues(field.path);

      if (isReferenceSelectField(field.path)) {
        return renderReferenceField(field, pathPrefix);
      }

      return (
        <MetadataFieldControl
          key={scopedPath}
          field={field}
          value={scopedValue ?? field.default}
          effectiveValue={field.default}
          overridden={hasValueAtPath(draftDocument, scopedPath)}
          optionValues={optionValues}
          onChange={(value) => updateDraftAtPath(scopedPath, value)}
          onReset={() => removeDraftPath(scopedPath)}
        />
      );
    },
    [draftDocument, getFieldOptionValues, removeDraftPath, renderReferenceField, updateDraftAtPath]
  );

  const handleSave = useCallback(() => {
    if (activeTab === 'advanced') {
      const parsed = parseYamlObject(yamlContent);
      if (!parsed.value) {
        setParseError(parsed.error);
        return;
      }

      updateDefaultConfig.mutate(
        { content: yamlContent },
        {
          onSuccess: () => {
            onOpenChange(false);
          },
        }
      );
      return;
    }

    updateDefaultConfigStructured.mutate(
      {
        execution,
        shared_config: sharedConfig,
      },
      {
        onSuccess: () => {
          onOpenChange(false);
        },
      }
    );
  }, [
    activeTab,
    execution,
    onOpenChange,
    sharedConfig,
    updateDefaultConfig,
    updateDefaultConfigStructured,
    yamlContent,
  ]);

  const isLoading = contextQuery.isLoading || referenceQuery.isLoading;
  const saveError = updateDefaultConfig.isError
    ? updateDefaultConfig.error.message
    : updateDefaultConfigStructured.isError
      ? updateDefaultConfigStructured.error.message
      : null;

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogContent className="max-w-6xl max-h-[92vh] flex flex-col">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Settings className="h-5 w-5" />
            Default Config
          </DialogTitle>
          <DialogDescription>
            Visual mode edits <code>default.execution</code> and <code>default.parameters.shared_config</code>. Advanced
            YAML remains available for everything else.
          </DialogDescription>
        </DialogHeader>

        {isLoading ? (
          <div className="flex h-[560px] items-center justify-center">
            <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
          </div>
        ) : (
          <div className="flex min-h-0 flex-1 flex-col gap-4 py-2">
            <div className="flex flex-wrap items-center gap-2" role="tablist" aria-label="Default config editor tabs">
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
            </div>

            {activeTab === 'visual' ? (
              <div className="min-h-0 flex-1 overflow-y-auto pr-1">
                <div className="space-y-6">
                  <Card className="border-border/60">
                    <CardHeader>
                      <CardTitle className="text-lg">Execution Defaults</CardTitle>
                      <CardDescription>Applies to report generation and artifact output behavior.</CardDescription>
                    </CardHeader>
                    <CardContent className="grid gap-3 lg:grid-cols-2">
                      {(reference?.execution_fields ?? []).map((field) => renderField(field, 'default.execution'))}
                    </CardContent>
                  </Card>

                  <Card className="border-border/60">
                    <CardHeader>
                      <CardTitle className="text-lg">Shared Config Defaults</CardTitle>
                      <CardDescription>These defaults feed inherited strategy shared_config values.</CardDescription>
                    </CardHeader>
                    <CardContent className="space-y-6">
                      {(reference?.shared_config_groups ?? []).map((group) => {
                        const fields = (reference?.shared_config_fields ?? []).filter(
                          (field) => field.group === group.key
                        );
                        if (fields.length === 0) return null;
                        return (
                          <div key={group.key} className="space-y-3">
                            <div>
                              <h3 className="text-sm font-semibold">{group.label}</h3>
                              {group.description ? (
                                <p className="text-sm text-muted-foreground">{group.description}</p>
                              ) : null}
                            </div>
                            <div className="grid gap-3 lg:grid-cols-2">
                              {fields.map((field) => renderField(field, 'default.parameters.shared_config'))}
                            </div>
                          </div>
                        );
                      })}
                    </CardContent>
                  </Card>

                  {advancedOnlyPaths.length > 0 ? (
                    <Card className="border-dashed border-border/60">
                      <CardHeader>
                        <CardTitle className="text-base">Advanced-only Content</CardTitle>
                        <CardDescription>
                          These paths remain intact on structured save but are only editable in Advanced YAML mode.
                        </CardDescription>
                      </CardHeader>
                      <CardContent>
                        <ul className="list-disc space-y-1 pl-5 text-sm text-muted-foreground">
                          {advancedOnlyPaths.map((path) => (
                            <li key={path}>{path}</li>
                          ))}
                        </ul>
                      </CardContent>
                    </Card>
                  ) : null}
                </div>
              </div>
            ) : null}

            {activeTab === 'advanced' ? (
              <div className="min-h-0 flex-1">
                <MonacoYamlEditor value={yamlContent} onChange={handleYamlChange} height="620px" />
                {parseError ? (
                  <div className="mt-3 rounded-lg border border-destructive/20 bg-destructive/10 p-3 text-sm text-destructive">
                    {parseError}
                  </div>
                ) : null}
              </div>
            ) : null}

            {saveError ? (
              <div className="rounded-lg border border-destructive/20 bg-destructive/10 p-3 text-sm text-destructive">
                Error: {saveError}
              </div>
            ) : null}
          </div>
        )}

        <DialogFooter className="gap-2">
          <Button variant="outline" onClick={() => handleOpenChange(false)}>
            Cancel
          </Button>
          <Button
            onClick={handleSave}
            disabled={isLoading || updateDefaultConfig.isPending || updateDefaultConfigStructured.isPending}
          >
            {updateDefaultConfig.isPending || updateDefaultConfigStructured.isPending ? (
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
