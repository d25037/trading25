import yaml from 'js-yaml';
import { AlertCircle, Loader2, Settings } from 'lucide-react';
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
import { useDefaultConfig, useUpdateDefaultConfig } from '@/hooks/useBacktest';

interface DefaultConfigEditorProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

export function DefaultConfigEditor({ open, onOpenChange }: DefaultConfigEditorProps) {
  const [yamlContent, setYamlContent] = useState('');
  const [parseError, setParseError] = useState<string | null>(null);
  const { data: configData, isLoading } = useDefaultConfig(open);
  const updateConfig = useUpdateDefaultConfig();

  useEffect(() => {
    if (configData?.content) {
      setYamlContent(configData.content);
      setParseError(null);
    }
  }, [configData]);

  const validateYaml = useCallback((): boolean => {
    try {
      const parsed = yaml.load(yamlContent);
      if (typeof parsed !== 'object' || parsed === null) {
        setParseError('Invalid YAML: Must be an object');
        return false;
      }
      setParseError(null);
      return true;
    } catch (e) {
      setParseError(`YAML parse error: ${e instanceof Error ? e.message : 'Unknown error'}`);
      return false;
    }
  }, [yamlContent]);

  const handleSave = useCallback(() => {
    if (!validateYaml()) return;

    updateConfig.mutate(
      { content: yamlContent },
      {
        onSuccess: () => {
          onOpenChange(false);
        },
      }
    );
  }, [validateYaml, yamlContent, updateConfig, onOpenChange]);

  const handleOpenChange = useCallback(
    (nextOpen: boolean) => {
      if (!nextOpen) {
        updateConfig.reset();
        setParseError(null);
      }
      onOpenChange(nextOpen);
    },
    [onOpenChange, updateConfig]
  );

  const handleYamlChange = useCallback((value: string) => {
    setYamlContent(value);
    setParseError(null);
  }, []);

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogContent className="max-w-4xl max-h-[90vh] flex flex-col">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Settings className="h-5 w-5" />
            Default Config
          </DialogTitle>
          <DialogDescription>Edit default.yaml — shared parameters applied to all strategies.</DialogDescription>
        </DialogHeader>

        <div className="flex-1 min-h-0 py-4 overflow-hidden">
          {isLoading ? (
            <div className="flex items-center justify-center h-[500px]">
              <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
            </div>
          ) : (
            <div className="flex flex-col h-[500px]">
              <div className="flex-1 min-h-0">
                <MonacoYamlEditor value={yamlContent} onChange={handleYamlChange} height="450px" />
              </div>

              <div className="mt-3 space-y-2">
                {parseError && (
                  <div className="flex items-start gap-2 p-3 rounded-md bg-destructive/10 text-destructive">
                    <AlertCircle className="h-4 w-4 mt-0.5 shrink-0" />
                    <span className="text-sm">{parseError}</span>
                  </div>
                )}

                {updateConfig.isError && (
                  <div className="flex items-start gap-2 p-3 rounded-md bg-destructive/10 text-destructive">
                    <AlertCircle className="h-4 w-4 mt-0.5 shrink-0" />
                    <span className="text-sm">Error: {updateConfig.error.message}</span>
                  </div>
                )}
              </div>
            </div>
          )}
        </div>

        <DialogFooter className="gap-2">
          <Button variant="outline" onClick={() => handleOpenChange(false)}>
            Cancel
          </Button>
          <Button onClick={handleSave} disabled={updateConfig.isPending || isLoading || !!parseError}>
            {updateConfig.isPending ? (
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
