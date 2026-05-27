import type { StrategyValidationResponse } from '@trading25/api-clients/backtest';
import { useCallback, useEffect, useState } from 'react';
import { canVisualizeStrategyConfig } from './authoringDocumentUtils';
import { parseYamlObject, removeValueAtPath, safeDumpYaml, setValueAtPath } from './authoringUtils';

export type EditorTab = 'visual' | 'advanced' | 'preview';
export type VisualSectionKey = 'basics' | 'shared_config' | 'entry_filter' | 'exit_trigger' | 'advanced_only';

interface StrategyEditorContextLike {
  data?: {
    raw_config: Record<string, unknown>;
  };
}

interface StrategyUpdateMutationLike {
  mutate: (
    args: { name: string; request: { config: Record<string, unknown> } },
    options?: { onSuccess?: () => void }
  ) => void;
  reset: () => void;
  isPending: boolean;
  isError: boolean;
  error: Error | null;
}

interface StrategyValidateMutationLike {
  mutateAsync: (args: {
    name: string;
    request: { config: Record<string, unknown> };
  }) => Promise<StrategyValidationResponse>;
  reset: () => void;
  isPending: boolean;
}

interface UseStrategyEditorDraftParams {
  open: boolean;
  strategyName: string;
  strategyContextQuery: StrategyEditorContextLike;
  updateStrategy: StrategyUpdateMutationLike;
  validateStrategy: StrategyValidateMutationLike;
  onOpenChange: (open: boolean) => void;
  onSuccess?: () => void;
}

export function useStrategyEditorDraft({
  open,
  strategyName,
  strategyContextQuery,
  updateStrategy,
  validateStrategy,
  onOpenChange,
  onSuccess,
}: UseStrategyEditorDraftParams) {
  const [activeTab, setActiveTab] = useState<EditorTab>('visual');
  const [activeVisualSection, setActiveVisualSection] = useState<VisualSectionKey>('basics');
  const [draftConfig, setDraftConfig] = useState<Record<string, unknown>>({});
  const [yamlContent, setYamlContent] = useState('');
  const [parseError, setParseError] = useState<string | null>(null);
  const [validationResult, setValidationResult] = useState<StrategyValidationResponse | null>(null);
  const [previewDirty, setPreviewDirty] = useState(true);

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

  return {
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
    updateErrorMessage: updateStrategy.isError ? (updateStrategy.error?.message ?? 'Unknown update error') : null,
    updatePending: updateStrategy.isPending,
    validatePending: validateStrategy.isPending,
    validationResult,
    yamlContent,
  };
}
