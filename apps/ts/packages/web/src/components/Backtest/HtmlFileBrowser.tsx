import { Check, ExternalLink, File, Loader2, Pencil, Search, Trash2, X } from 'lucide-react';
import { useEffect, useMemo, useRef, useState } from 'react';
import { Button } from '@/components/ui/button';
import { Card, CardContent } from '@/components/ui/card';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Input } from '@/components/ui/input';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { useDeleteHtmlFile, useHtmlFileContent, useHtmlFiles, useRenameHtmlFile } from '@/hooks/useBacktest';
import type { HtmlFileInfo, HtmlFileMetrics } from '@/types/backtest';
import { ResultHtmlViewer } from './ResultHtmlViewer';

function safeAtob(base64: string): string | null {
  try {
    return atob(base64);
  } catch {
    console.error('Failed to decode base64 HTML content');
    return null;
  }
}

function formatDate(dateStr: string): string {
  return new Date(dateStr).toLocaleString('ja-JP', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  });
}

interface FileListItemProps {
  file: HtmlFileInfo;
  isSelected: boolean;
  onSelect: () => void;
}

function FileListItem({ file, isSelected, onSelect }: FileListItemProps) {
  return (
    <button
      type="button"
      onClick={onSelect}
      className={`w-full flex items-center gap-3 p-3 text-left rounded-md transition-colors ${
        isSelected ? 'bg-primary/10 border border-primary/30' : 'hover:bg-muted'
      }`}
    >
      <File className="h-4 w-4 shrink-0 text-muted-foreground" />
      <div className="flex-1 min-w-0">
        <p className="text-sm font-medium truncate">{file.filename}</p>
        <div className="flex items-center gap-2 text-xs text-muted-foreground">
          <span>{file.strategy_name}</span>
          <span>|</span>
          <span>{formatDate(file.created_at)}</span>
        </div>
      </div>
    </button>
  );
}

function formatMetricValue(value: number | null, format: 'percent' | 'decimal' | 'integer'): string {
  if (value === null || value === undefined) return '—';
  switch (format) {
    case 'percent':
      return `${value.toFixed(2)}%`;
    case 'decimal':
      return value.toFixed(2);
    case 'integer':
      return String(Math.round(value));
  }
}

function MetricItem({
  label,
  value,
  format,
}: {
  label: string;
  value: number | null;
  format: 'percent' | 'decimal' | 'integer';
}) {
  const isReturnMetric = label === 'Total Return';
  const colorClass =
    isReturnMetric && value !== null
      ? value >= 0
        ? 'text-green-600 dark:text-green-400'
        : 'text-red-600 dark:text-red-400'
      : '';

  return (
    <div className="rounded-lg border bg-card p-3 text-center">
      <p className="text-xs text-muted-foreground mb-1">{label}</p>
      <p className={`text-sm font-semibold ${colorClass}`}>{formatMetricValue(value, format)}</p>
    </div>
  );
}

function MetricsGrid({ metrics }: { metrics: HtmlFileMetrics }) {
  return (
    <div className="grid grid-cols-3 md:grid-cols-6 gap-3">
      <MetricItem label="Total Return" value={metrics.total_return} format="percent" />
      <MetricItem label="Max DD" value={metrics.max_drawdown} format="percent" />
      <MetricItem label="Sharpe" value={metrics.sharpe_ratio} format="decimal" />
      <MetricItem label="Sortino" value={metrics.sortino_ratio} format="decimal" />
      <MetricItem label="Win Rate" value={metrics.win_rate} format="percent" />
      <MetricItem label="Trades" value={metrics.total_trades} format="integer" />
    </div>
  );
}

// biome-ignore lint/complexity/noExcessiveCognitiveComplexity: UI component with file list, preview, rename/delete dialogs
export function HtmlFileBrowser() {
  const [selectedStrategy, setSelectedStrategy] = useState<string>('all');
  const [searchQuery, setSearchQuery] = useState('');
  const [selectedFile, setSelectedFile] = useState<HtmlFileInfo | null>(null);
  const [isRenaming, setIsRenaming] = useState(false);
  const [renameValue, setRenameValue] = useState('');
  const renameInputRef = useRef<HTMLInputElement>(null);

  const { data: htmlFilesData, isLoading: isLoadingFiles } = useHtmlFiles(
    selectedStrategy === 'all' ? undefined : selectedStrategy
  );
  const { data: htmlContent, isLoading: isLoadingContent } = useHtmlFileContent(
    selectedFile?.strategy_name ?? null,
    selectedFile?.filename ?? null
  );
  const renameHtmlFile = useRenameHtmlFile();
  const deleteHtmlFile = useDeleteHtmlFile();
  const [isDeleteDialogOpen, setIsDeleteDialogOpen] = useState(false);

  // Get unique strategies
  const strategies = useMemo(() => {
    if (!htmlFilesData?.files) return [];
    const strategySet = new Set(htmlFilesData.files.map((f) => f.strategy_name));
    return Array.from(strategySet).sort();
  }, [htmlFilesData]);

  // Filter files by search query
  const filteredFiles = useMemo(() => {
    if (!htmlFilesData?.files) return [];
    if (!searchQuery) return htmlFilesData.files;
    const query = searchQuery.toLowerCase();
    return htmlFilesData.files.filter(
      (f) =>
        f.filename.toLowerCase().includes(query) ||
        f.dataset_name.toLowerCase().includes(query) ||
        f.strategy_name.toLowerCase().includes(query)
    );
  }, [htmlFilesData, searchQuery]);

  // Sort files by created_at descending
  const sortedFiles = useMemo(() => {
    return [...filteredFiles].sort((a, b) => new Date(b.created_at).getTime() - new Date(a.created_at).getTime());
  }, [filteredFiles]);

  const handleStartRename = () => {
    if (!selectedFile) return;
    setRenameValue(selectedFile.filename);
    setIsRenaming(true);
  };

  useEffect(() => {
    if (isRenaming && renameInputRef.current) {
      const input = renameInputRef.current;
      input.focus();
      // .html の前にカーソルを置く
      const extIndex = input.value.lastIndexOf('.html');
      if (extIndex > 0) {
        input.setSelectionRange(0, extIndex);
      }
    }
  }, [isRenaming]);

  const handleCancelRename = () => {
    setIsRenaming(false);
    setRenameValue('');
  };

  const handleConfirmRename = () => {
    if (!selectedFile || !renameValue.trim()) return;
    const newFilename = renameValue.trim().endsWith('.html') ? renameValue.trim() : `${renameValue.trim()}.html`;
    if (newFilename === selectedFile.filename) {
      handleCancelRename();
      return;
    }
    renameHtmlFile.mutate(
      {
        strategy: selectedFile.strategy_name,
        filename: selectedFile.filename,
        request: { new_filename: newFilename },
      },
      {
        onSuccess: (data) => {
          setSelectedFile({
            ...selectedFile,
            filename: data.new_filename,
          });
          setIsRenaming(false);
          setRenameValue('');
        },
      }
    );
  };

  const handleOpenInNewTab = () => {
    if (!selectedFile || !htmlContent?.html_content) return;
    const htmlString = safeAtob(htmlContent.html_content);
    if (!htmlString) return;
    const blob = new Blob([htmlString], { type: 'text/html' });
    const url = URL.createObjectURL(blob);
    window.open(url, '_blank', 'noopener,noreferrer');
    setTimeout(() => URL.revokeObjectURL(url), 60000);
  };

  const handleDeleteFile = () => {
    if (!selectedFile) return;
    deleteHtmlFile.mutate(
      {
        strategy: selectedFile.strategy_name,
        filename: selectedFile.filename,
      },
      {
        onSuccess: () => {
          setIsDeleteDialogOpen(false);
          setSelectedFile(null);
        },
      }
    );
  };

  return (
    <div className="space-y-4">
      {/* Filters */}
      <div className="flex items-center gap-4">
        <div className="flex-1">
          <div className="relative">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
            <Input
              placeholder="Search files..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="pl-10"
            />
          </div>
        </div>
        <Select value={selectedStrategy} onValueChange={setSelectedStrategy}>
          <SelectTrigger className="w-[200px]">
            <SelectValue placeholder="All strategies" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All strategies</SelectItem>
            {strategies.map((strategy) => (
              <SelectItem key={strategy} value={strategy}>
                {strategy}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        {/* File List */}
        <Card className="lg:col-span-1">
          <CardContent className="pt-4">
            {isLoadingFiles ? (
              <div className="flex items-center justify-center h-48">
                <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
              </div>
            ) : filteredFiles.length === 0 ? (
              <div className="flex flex-col items-center justify-center h-48 text-muted-foreground">
                <File className="h-12 w-12 mb-2" />
                <p className="text-sm">No HTML files found</p>
              </div>
            ) : (
              <div className="space-y-4 max-h-[600px] overflow-y-auto">
                <p className="text-sm text-muted-foreground">
                  {filteredFiles.length} files{' '}
                  {htmlFilesData?.total && htmlFilesData.total > filteredFiles.length
                    ? `(${htmlFilesData.total} total)`
                    : ''}
                </p>

                {sortedFiles.map((file) => (
                  <FileListItem
                    key={`${file.strategy_name}/${file.filename}`}
                    file={file}
                    isSelected={
                      selectedFile?.strategy_name === file.strategy_name && selectedFile?.filename === file.filename
                    }
                    onSelect={() => setSelectedFile(file)}
                  />
                ))}
              </div>
            )}
          </CardContent>
        </Card>

        {/* Preview */}
        <Card className="lg:col-span-2">
          <CardContent className="pt-4">
            {selectedFile ? (
              <div className="space-y-4">
                <div className="flex items-center justify-between">
                  <div className="min-w-0 flex-1">
                    {isRenaming ? (
                      <div className="flex items-center gap-2">
                        <Input
                          ref={renameInputRef}
                          value={renameValue}
                          onChange={(e) => setRenameValue(e.target.value)}
                          onKeyDown={(e) => {
                            if (e.key === 'Enter') handleConfirmRename();
                            if (e.key === 'Escape') handleCancelRename();
                          }}
                          className="h-8 text-sm"
                          disabled={renameHtmlFile.isPending}
                        />
                        <Button
                          variant="ghost"
                          size="icon"
                          className="h-7 w-7 shrink-0"
                          onClick={handleConfirmRename}
                          disabled={renameHtmlFile.isPending}
                        >
                          <Check className="h-4 w-4" />
                        </Button>
                        <Button
                          variant="ghost"
                          size="icon"
                          className="h-7 w-7 shrink-0"
                          onClick={handleCancelRename}
                          disabled={renameHtmlFile.isPending}
                        >
                          <X className="h-4 w-4" />
                        </Button>
                      </div>
                    ) : (
                      <div className="flex items-center gap-2">
                        <h3 className="font-medium truncate">{selectedFile.filename}</h3>
                        <Button variant="ghost" size="icon" className="h-7 w-7 shrink-0" onClick={handleStartRename}>
                          <Pencil className="h-3.5 w-3.5" />
                        </Button>
                        <Button
                          variant="ghost"
                          size="icon"
                          className="h-7 w-7 shrink-0 text-destructive hover:text-destructive"
                          onClick={() => setIsDeleteDialogOpen(true)}
                        >
                          <Trash2 className="h-3.5 w-3.5" />
                        </Button>
                      </div>
                    )}
                    <p className="text-sm text-muted-foreground">
                      {selectedFile.strategy_name} | {selectedFile.dataset_name} | {formatDate(selectedFile.created_at)}
                    </p>
                  </div>
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={handleOpenInNewTab}
                    disabled={!htmlContent?.html_content}
                    className="shrink-0 ml-2"
                  >
                    <ExternalLink className="h-4 w-4 mr-2" />
                    Open in new tab
                  </Button>
                </div>
                {renameHtmlFile.isError && (
                  <div className="rounded-md bg-red-500/10 p-2 text-sm text-red-500">
                    {renameHtmlFile.error.message}
                  </div>
                )}
                {htmlContent?.metrics && <MetricsGrid metrics={htmlContent.metrics} />}
                <ResultHtmlViewer
                  htmlContent={htmlContent?.html_content ? safeAtob(htmlContent.html_content) : null}
                  isLoading={isLoadingContent}
                />
              </div>
            ) : (
              <div className="flex flex-col items-center justify-center h-[400px] text-muted-foreground">
                <File className="h-12 w-12 mb-2" />
                <p className="text-sm">Select a file to preview</p>
              </div>
            )}
          </CardContent>
        </Card>
      </div>

      {/* Delete Confirm Dialog */}
      <Dialog open={isDeleteDialogOpen} onOpenChange={setIsDeleteDialogOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              <Trash2 className="h-5 w-5 text-destructive" />
              Delete HTML File
            </DialogTitle>
            <DialogDescription>
              Are you sure you want to delete{' '}
              <span className="font-semibold text-foreground">{selectedFile?.filename}</span>? This action cannot be
              undone.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setIsDeleteDialogOpen(false)}>
              Cancel
            </Button>
            <Button variant="destructive" onClick={handleDeleteFile} disabled={deleteHtmlFile.isPending}>
              {deleteHtmlFile.isPending ? (
                <>
                  <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                  Deleting...
                </>
              ) : (
                'Delete'
              )}
            </Button>
          </DialogFooter>
          {deleteHtmlFile.isError && <p className="text-sm text-destructive">Error: {deleteHtmlFile.error.message}</p>}
        </DialogContent>
      </Dialog>
    </div>
  );
}
