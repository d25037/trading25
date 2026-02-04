import { ExternalLink, Loader2 } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';

interface ResultHtmlViewerProps {
  htmlContent: string | null | undefined;
  isLoading: boolean;
}

export function ResultHtmlViewer({ htmlContent, isLoading }: ResultHtmlViewerProps) {
  if (isLoading) {
    return (
      <Card className="mt-4">
        <CardContent className="flex items-center justify-center h-96">
          <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
        </CardContent>
      </Card>
    );
  }

  if (!htmlContent) {
    return (
      <Card className="mt-4">
        <CardContent className="flex items-center justify-center h-48 text-muted-foreground">
          No HTML report available
        </CardContent>
      </Card>
    );
  }

  const openInNewTab = () => {
    const blob = new Blob([htmlContent], { type: 'text/html' });
    const url = URL.createObjectURL(blob);
    window.open(url, '_blank');
    setTimeout(() => URL.revokeObjectURL(url), 60000);
  };

  return (
    <Card className="mt-4">
      <CardHeader className="pb-2 flex flex-row items-center justify-between">
        <CardTitle className="text-base">Backtest Report</CardTitle>
        <Button variant="outline" size="sm" onClick={openInNewTab}>
          <ExternalLink className="h-4 w-4 mr-2" />
          Open in New Tab
        </Button>
      </CardHeader>
      <CardContent className="p-0">
        <iframe
          srcDoc={htmlContent}
          className="w-full h-[600px] border-0 rounded-b-lg"
          title="Backtest Report"
          sandbox="allow-scripts"
        />
      </CardContent>
    </Card>
  );
}
