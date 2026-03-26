import { Check, ExternalLink, File, Loader2, Pencil, Search, Trash2, X } from 'lucide-react';
import { type RefObject, useEffect, useMemo, useRef, useState } from 'react';
import { SectionEyebrow, Surface } from '@/components/Layout/Workspace';
import { Button } from '@/components/ui/button';
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

function resolveStrategies(files: HtmlFileInfo[] | undefined): string[] {
  if (!files) return [];
  return Array.from(new Set(files.map((file) => file.strategy_name))).sort();
}

function resolveFilteredFiles(files: HtmlFileInfo[] | undefined, searchQuery: string): HtmlFileInfo[] {
  if (!files) return [];
  if (!searchQuery) return files;
  const query = searchQuery.toLowerCase();
  return files.filter(
    (file) =>
      file.filename.toLowerCase().includes(query) ||
      file.dataset_name.toLowerCase().includes(query) ||
      file.strategy_name.toLowerCase().includes(query)
  );
}

function resolveSortedFiles(files: HtmlFileInfo[]): HtmlFileInfo[] {
  return [...files].sort((left, right) => new Date(right.created_at).getTime() - new Date(left.created_at).getTime());
}

function openHtmlInNewTab(decodedHtml: string): void {
  const blob = new Blob([decodedHtml], { type: 'text/html' });
  const url = URL.createObjectURL(blob);
  window.open(url, '_blank', 'noopener,noreferrer');
  setTimeout(() => URL.revokeObjectURL(url), 60000);
}

type RenameAction =
  | { type: 'ignore' }
  | { type: 'cancel' }
  | {
      type: 'rename';
      newFilename: string;
    };

function resolveRenameAction(selectedFile: HtmlFileInfo | null, renameValue: string): RenameAction {
  if (!selectedFile) {
    return { type: 'ignore' };
  }

  const trimmed = renameValue.trim();
  if (!trimmed) {
    return { type: 'ignore' };
  }

  const newFilename = trimmed.endsWith('.html') ? trimmed : `${trimmed}.html`;
  if (newFilename === selectedFile.filename) {
    return { type: 'cancel' };
  }

  return { type: 'rename', newFilename };
}

interface FileListCardProps {
  isLoadingFiles: boolean;
  filteredFiles: HtmlFileInfo[];
  sortedFiles: HtmlFileInfo[];
  totalFiles: number | undefined;
  selectedFile: HtmlFileInfo | null;
  onSelectFile: (file: HtmlFileInfo) => void;
}

function FileListCard({
  isLoadingFiles,
  filteredFiles,
  sortedFiles,
  totalFiles,
  selectedFile,
  onSelectFile,
}: FileListCardProps) {
  if (isLoadingFiles) {
    return (
      <Surface className="lg:col-span-1 flex min-h-[24rem] items-center justify-center px-4 py-4">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </Surface>
    );
  }

  if (filteredFiles.length === 0) {
    return (
      <Surface className="lg:col-span-1 flex min-h-[24rem] flex-col items-center justify-center px-4 py-4 text-muted-foreground">
        <File className="mb-2 h-12 w-12" />
        <p className="text-sm">No HTML files found</p>
      </Surface>
    );
  }

  return (
    <Surface className="lg:col-span-1 flex min-h-[32rem] flex-col px-4 py-4">
      <SectionEyebrow>History</SectionEyebrow>
      <p className="mt-2 text-sm text-muted-foreground">
        {filteredFiles.length} files {totalFiles && totalFiles > filteredFiles.length ? `(${totalFiles} total)` : ''}
      </p>

      <div className="mt-4 flex-1 space-y-2 overflow-y-auto pr-1">
        {sortedFiles.map((file) => (
          <FileListItem
            key={`${file.strategy_name}/${file.filename}`}
            file={file}
            isSelected={selectedFile?.strategy_name === file.strategy_name && selectedFile?.filename === file.filename}
            onSelect={() => onSelectFile(file)}
          />
        ))}
      </div>
    </Surface>
  );
}

interface PreviewCardProps {
  selectedFile: HtmlFileInfo | null;
  isRenaming: boolean;
  renameValue: string;
  setRenameValue: (value: string) => void;
  renameInputRef: RefObject<HTMLInputElement | null>;
  onConfirmRename: () => void;
  onCancelRename: () => void;
  onStartRename: () => void;
  onOpenDeleteDialog: () => void;
  onOpenInNewTab: () => void;
  isRenamePending: boolean;
  renameErrorMessage: string | null;
  htmlContentBase64: string | null | undefined;
  metrics: HtmlFileMetrics | null | undefined;
  isLoadingContent: boolean;
}

function PreviewCard({
  selectedFile,
  isRenaming,
  renameValue,
  setRenameValue,
  renameInputRef,
  onConfirmRename,
  onCancelRename,
  onStartRename,
  onOpenDeleteDialog,
  onOpenInNewTab,
  isRenamePending,
  renameErrorMessage,
  htmlContentBase64,
  metrics,
  isLoadingContent,
}: PreviewCardProps) {
  if (!selectedFile) {
    return (
      <Surface className="lg:col-span-2 flex min-h-[32rem] flex-col items-center justify-center px-4 py-4 text-muted-foreground">
        <File className="mb-2 h-12 w-12" />
        <p className="text-sm">Select a file to preview</p>
      </Surface>
    );
  }

  const decodedHtmlContent = htmlContentBase64 ? safeAtob(htmlContentBase64) : null;

  return (
    <Surface className="lg:col-span-2 flex min-h-[32rem] flex-col px-4 py-4">
      <SectionEyebrow>Selected Report</SectionEyebrow>
      <div className="mt-3 space-y-4">
        <div className="flex items-center justify-between gap-3">
          <div className="min-w-0 flex-1">
            {isRenaming ? (
              <div className="flex items-center gap-2">
                <Input
                  ref={renameInputRef}
                  value={renameValue}
                  onChange={(event) => setRenameValue(event.target.value)}
                  onKeyDown={(event) => {
                    if (event.key === 'Enter') onConfirmRename();
                    if (event.key === 'Escape') onCancelRename();
                  }}
                  className="h-8 text-sm"
                  disabled={isRenamePending}
                />
                <Button
                  variant="ghost"
                  size="icon"
                  className="h-7 w-7 shrink-0"
                  onClick={onConfirmRename}
                  disabled={isRenamePending}
                >
                  <Check className="h-4 w-4" />
                </Button>
                <Button
                  variant="ghost"
                  size="icon"
                  className="h-7 w-7 shrink-0"
                  onClick={onCancelRename}
                  disabled={isRenamePending}
                >
                  <X className="h-4 w-4" />
                </Button>
              </div>
            ) : (
              <div className="flex items-center gap-2">
                <h3 className="truncate text-base font-semibold text-foreground">{selectedFile.filename}</h3>
                <Button variant="ghost" size="icon" className="h-7 w-7 shrink-0" onClick={onStartRename}>
                  <Pencil className="h-3.5 w-3.5" />
                </Button>
                <Button
                  variant="ghost"
                  size="icon"
                  className="h-7 w-7 shrink-0 text-destructive hover:text-destructive"
                  onClick={onOpenDeleteDialog}
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
            onClick={onOpenInNewTab}
            disabled={!htmlContentBase64}
            className="ml-2 shrink-0"
          >
            <ExternalLink className="mr-2 h-4 w-4" />
            Open in new tab
          </Button>
        </div>
        {renameErrorMessage && <div className="rounded-md bg-red-500/10 p-2 text-sm text-red-500">{renameErrorMessage}</div>}
        {metrics && <MetricsGrid metrics={metrics} />}
        <ResultHtmlViewer htmlContent={decodedHtmlContent} isLoading={isLoadingContent} />
      </div>
    </Surface>
  );
}

interface DeleteDialogProps {
  open: boolean;
  selectedFilename: string | undefined;
  isPending: boolean;
  errorMessage: string | null;
  onOpenChange: (open: boolean) => void;
  onDelete: () => void;
}

function DeleteDialog({ open, selectedFilename, isPending, errorMessage, onOpenChange, onDelete }: DeleteDialogProps) {
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Trash2 className="h-5 w-5 text-destructive" />
            Delete HTML File
          </DialogTitle>
          <DialogDescription>
            Are you sure you want to delete <span className="font-semibold text-foreground">{selectedFilename}</span>?
            This action cannot be undone.
          </DialogDescription>
        </DialogHeader>
        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            Cancel
          </Button>
          <Button variant="destructive" onClick={onDelete} disabled={isPending}>
            {isPending ? (
              <>
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                Deleting...
              </>
            ) : (
              'Delete'
            )}
          </Button>
        </DialogFooter>
        {errorMessage && <p className="text-sm text-destructive">Error: {errorMessage}</p>}
      </DialogContent>
    </Dialog>
  );
}

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

  const strategies = useMemo(() => resolveStrategies(htmlFilesData?.files), [htmlFilesData?.files]);
  const filteredFiles = useMemo(
    () => resolveFilteredFiles(htmlFilesData?.files, searchQuery),
    [htmlFilesData?.files, searchQuery]
  );
  const sortedFiles = useMemo(() => resolveSortedFiles(filteredFiles), [filteredFiles]);

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
    const action = resolveRenameAction(selectedFile, renameValue);
    if (action.type === 'ignore') {
      return;
    }
    if (action.type === 'cancel') {
      handleCancelRename();
      return;
    }
    if (!selectedFile) return;

    renameHtmlFile.mutate(
      {
        strategy: selectedFile.strategy_name,
        filename: selectedFile.filename,
        request: { new_filename: action.newFilename },
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
    const decodedHtml = htmlContent?.html_content ? safeAtob(htmlContent.html_content) : null;
    if (!decodedHtml) return;
    openHtmlInNewTab(decodedHtml);
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
    <div className="flex min-h-0 flex-1 flex-col gap-3">
      <Surface className="px-4 py-3">
        <SectionEyebrow className="mb-3">Filters</SectionEyebrow>
        <div className="flex flex-col gap-3 lg:flex-row lg:items-center">
          <div className="flex-1">
            <div className="relative">
              <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
              <Input
                placeholder="Search files..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                className="pl-10"
              />
            </div>
          </div>
          <Select value={selectedStrategy} onValueChange={setSelectedStrategy}>
            <SelectTrigger className="w-full lg:w-[220px]">
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
      </Surface>

      <div className="grid min-h-0 flex-1 grid-cols-1 gap-3 lg:grid-cols-3">
        <FileListCard
          isLoadingFiles={isLoadingFiles}
          filteredFiles={filteredFiles}
          sortedFiles={sortedFiles}
          totalFiles={htmlFilesData?.total}
          selectedFile={selectedFile}
          onSelectFile={setSelectedFile}
        />
        <PreviewCard
          selectedFile={selectedFile}
          isRenaming={isRenaming}
          renameValue={renameValue}
          setRenameValue={setRenameValue}
          renameInputRef={renameInputRef}
          onConfirmRename={handleConfirmRename}
          onCancelRename={handleCancelRename}
          onStartRename={handleStartRename}
          onOpenDeleteDialog={() => setIsDeleteDialogOpen(true)}
          onOpenInNewTab={handleOpenInNewTab}
          isRenamePending={renameHtmlFile.isPending}
          renameErrorMessage={renameHtmlFile.isError ? renameHtmlFile.error.message : null}
          htmlContentBase64={htmlContent?.html_content}
          metrics={htmlContent?.metrics}
          isLoadingContent={isLoadingContent}
        />
      </div>

      <DeleteDialog
        open={isDeleteDialogOpen}
        selectedFilename={selectedFile?.filename}
        isPending={deleteHtmlFile.isPending}
        errorMessage={deleteHtmlFile.isError ? deleteHtmlFile.error.message : null}
        onOpenChange={setIsDeleteDialogOpen}
        onDelete={handleDeleteFile}
      />
    </div>
  );
}
