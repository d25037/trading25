import yaml from 'js-yaml';
import { AlertCircle, CheckCircle2, Edit, Loader2 } from 'lucide-react';
import { useCallback, useEffect, useState } from 'react';
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
import { useStrategy, useUpdateStrategy, useValidateStrategy } from '@/hooks/useBacktest';
import { cn } from '@/lib/utils';
import { SignalReferencePanel } from './SignalReferencePanel';

interface StrategyEditorProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  strategyName: string;
  onSuccess?: () => void;
}

// biome-ignore lint/complexity/noExcessiveCognitiveComplexity: Complex component with multiple validation states
export function StrategyEditor({ open, onOpenChange, strategyName, onSuccess }: StrategyEditorProps) {
  const [yamlContent, setYamlContent] = useState('');
  const [parseError, setParseError] = useState<string | null>(null);
  const { data: strategyDetail, isLoading: isLoadingStrategy } = useStrategy(open ? strategyName : null);
  const updateStrategy = useUpdateStrategy();
  const validateStrategy = useValidateStrategy();

  useEffect(() => {
    if (strategyDetail?.config) {
      try {
        setYamlContent(yaml.dump(strategyDetail.config, { indent: 2 }));
        setParseError(null);
      } catch {
        setYamlContent(JSON.stringify(strategyDetail.config, null, 2));
      }
    }
  }, [strategyDetail]);

  const parseYaml = useCallback((): Record<string, unknown> | null => {
    try {
      const parsed = yaml.load(yamlContent);
      if (typeof parsed !== 'object' || parsed === null) {
        setParseError('Invalid YAML: Must be an object');
        return null;
      }
      setParseError(null);
      return parsed as Record<string, unknown>;
    } catch (e) {
      setParseError(`YAML parse error: ${e instanceof Error ? e.message : 'Unknown error'}`);
      return null;
    }
  }, [yamlContent]);

  const handleValidate = useCallback(() => {
    const config = parseYaml();
    if (!config) return;

    validateStrategy.mutate({
      name: strategyName,
      request: { config },
    });
  }, [parseYaml, strategyName, validateStrategy]);

  const handleSave = useCallback(() => {
    const config = parseYaml();
    if (!config) return;

    updateStrategy.mutate(
      { name: strategyName, request: { config } },
      {
        onSuccess: () => {
          onOpenChange(false);
          onSuccess?.();
        },
      }
    );
  }, [parseYaml, strategyName, updateStrategy, onOpenChange, onSuccess]);

  const handleOpenChange = useCallback(
    (open: boolean) => {
      if (!open) {
        validateStrategy.reset();
        updateStrategy.reset();
        setParseError(null);
      }
      onOpenChange(open);
    },
    [onOpenChange, validateStrategy, updateStrategy]
  );

  const handleYamlChange = useCallback((value: string) => {
    setYamlContent(value);
    setParseError(null);
  }, []);

  const handleCopySnippet = useCallback(
    (snippet: string) => {
      // Insert snippet at the end of current YAML content
      const newContent = yamlContent.trim() ? `${yamlContent.trim()}\n\n${snippet}` : snippet;
      setYamlContent(newContent);
    },
    [yamlContent]
  );

  const validationResult = validateStrategy.data;
  const hasValidationErrors = validationResult && !validationResult.valid;
  const hasValidationWarnings = validationResult?.warnings && validationResult.warnings.length > 0;

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogContent className="max-w-6xl max-h-[90vh] flex flex-col">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Edit className="h-5 w-5" />
            Edit Strategy: {strategyName}
          </DialogTitle>
          <DialogDescription>
            Edit the YAML configuration for this strategy. Only experimental strategies can be modified.
          </DialogDescription>
        </DialogHeader>

        <div className="flex-1 min-h-0 py-4 overflow-hidden">
          {isLoadingStrategy ? (
            <div className="flex items-center justify-center h-[500px]">
              <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
            </div>
          ) : (
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-4 h-[500px]">
              {/* Monaco Editor */}
              <div className="lg:col-span-2 flex flex-col min-h-0">
                <div className="flex-1 min-h-0">
                  <MonacoYamlEditor value={yamlContent} onChange={handleYamlChange} height="400px" />
                </div>

                {/* Error/Validation Messages */}
                <div className="mt-4 space-y-2">
                  {parseError && (
                    <div className="flex items-start gap-2 p-3 rounded-md bg-destructive/10 text-destructive">
                      <AlertCircle className="h-4 w-4 mt-0.5 shrink-0" />
                      <span className="text-sm">{parseError}</span>
                    </div>
                  )}

                  {validationResult && (
                    <div
                      className={cn(
                        'p-3 rounded-md space-y-2',
                        hasValidationErrors
                          ? 'bg-destructive/10'
                          : hasValidationWarnings
                            ? 'bg-yellow-500/10'
                            : 'bg-green-500/10'
                      )}
                    >
                      <div className="flex items-center gap-2">
                        {hasValidationErrors ? (
                          <AlertCircle className="h-4 w-4 text-destructive" />
                        ) : (
                          <CheckCircle2
                            className={cn('h-4 w-4', hasValidationWarnings ? 'text-yellow-500' : 'text-green-500')}
                          />
                        )}
                        <span
                          className={cn(
                            'text-sm font-medium',
                            hasValidationErrors
                              ? 'text-destructive'
                              : hasValidationWarnings
                                ? 'text-yellow-500'
                                : 'text-green-500'
                          )}
                        >
                          {hasValidationErrors
                            ? 'Validation failed'
                            : hasValidationWarnings
                              ? 'Validation passed with warnings'
                              : 'Validation passed'}
                        </span>
                      </div>
                      {validationResult.errors.length > 0 && (
                        <ul className="list-disc list-inside text-sm text-destructive space-y-1">
                          {validationResult.errors.map((error) => (
                            <li key={error}>{error}</li>
                          ))}
                        </ul>
                      )}
                      {validationResult.warnings.length > 0 && (
                        <ul className="list-disc list-inside text-sm text-yellow-600 space-y-1">
                          {validationResult.warnings.map((warning) => (
                            <li key={warning}>{warning}</li>
                          ))}
                        </ul>
                      )}
                    </div>
                  )}

                  {updateStrategy.isError && (
                    <div className="flex items-start gap-2 p-3 rounded-md bg-destructive/10 text-destructive">
                      <AlertCircle className="h-4 w-4 mt-0.5 shrink-0" />
                      <span className="text-sm">Error: {updateStrategy.error.message}</span>
                    </div>
                  )}
                </div>
              </div>

              {/* Signal Reference Panel */}
              <div className="lg:col-span-1 border rounded-md overflow-hidden min-h-0">
                <SignalReferencePanel onCopySnippet={handleCopySnippet} />
              </div>
            </div>
          )}
        </div>

        <DialogFooter className="gap-2">
          <Button variant="outline" onClick={() => handleOpenChange(false)}>
            Cancel
          </Button>
          <Button
            variant="secondary"
            onClick={handleValidate}
            disabled={validateStrategy.isPending || isLoadingStrategy}
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
          <Button onClick={handleSave} disabled={updateStrategy.isPending || isLoadingStrategy || !!parseError}>
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
