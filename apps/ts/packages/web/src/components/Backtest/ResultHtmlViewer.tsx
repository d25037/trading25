import { Loader2 } from 'lucide-react';

interface ResultHtmlViewerProps {
  htmlContent: string | null | undefined;
  isLoading: boolean;
}

export function ResultHtmlViewer({ htmlContent, isLoading }: ResultHtmlViewerProps) {
  if (isLoading) {
    return (
      <div className="mt-4 flex h-96 items-center justify-center rounded-2xl border border-border/70 bg-[var(--app-surface-muted)]">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    );
  }

  if (!htmlContent) {
    return (
      <div className="mt-4 flex h-48 items-center justify-center rounded-2xl border border-dashed border-border/70 bg-[var(--app-surface-muted)] text-sm text-muted-foreground">
        No HTML report available
      </div>
    );
  }

  return (
    <iframe
      srcDoc={htmlContent}
      className="mt-4 h-[640px] w-full rounded-2xl border border-border/70 bg-white"
      title="Backtest Report"
      sandbox="allow-scripts"
    />
  );
}
