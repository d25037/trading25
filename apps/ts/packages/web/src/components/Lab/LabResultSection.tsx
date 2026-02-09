import { validateLabResultData } from '@trading25/clients-ts/backtest';
import { AlertTriangle, RefreshCw } from 'lucide-react';
import { useRef, useState } from 'react';
import { ErrorBoundary } from '@/components/ErrorBoundary';
import type { LabResultData } from '@/types/backtest';
import { LabEvolveResults } from './LabEvolveResults';
import { LabGenerateResults } from './LabGenerateResults';
import { LabImproveResults } from './LabImproveResults';
import { LabOptimizeResults } from './LabOptimizeResults';

interface ValidationErrorProps {
  error: string;
}

function ValidationErrorUI({ error }: ValidationErrorProps) {
  return (
    <div className="mt-4 rounded-md border border-yellow-500/30 bg-yellow-500/10 p-4">
      <div className="flex items-center gap-2 text-sm font-medium text-yellow-600 dark:text-yellow-400">
        <AlertTriangle className="h-4 w-4" />
        結果データの形式が不正です
      </div>
      <p className="mt-1 text-xs text-muted-foreground">{error}</p>
    </div>
  );
}

interface ErrorFallbackProps {
  onReset: () => void;
}

function LabResultErrorFallback({ onReset }: ErrorFallbackProps) {
  return (
    <div className="mt-4 rounded-md border border-red-500/30 bg-red-500/10 p-4">
      <div className="flex items-center gap-2 text-sm font-medium text-red-500">
        <AlertTriangle className="h-4 w-4" />
        結果の表示に失敗しました
      </div>
      <p className="mt-1 text-xs text-muted-foreground">予期しないエラーが発生しました。</p>
      <button
        type="button"
        onClick={onReset}
        className="mt-2 inline-flex items-center gap-1.5 rounded-md bg-primary px-3 py-1.5 text-xs text-primary-foreground hover:bg-primary/90"
      >
        <RefreshCw className="h-3 w-3" />
        再試行
      </button>
    </div>
  );
}

interface LabResultContentProps {
  resultData: LabResultData;
}

function LabResultContent({ resultData }: LabResultContentProps) {
  const validation = validateLabResultData(resultData);

  if (!validation.success) {
    return <ValidationErrorUI error={validation.error} />;
  }

  const { data } = validation;
  switch (data.lab_type) {
    case 'generate':
      return <LabGenerateResults result={data} />;
    case 'evolve':
      return <LabEvolveResults result={data} />;
    case 'optimize':
      return <LabOptimizeResults result={data} />;
    case 'improve':
      return <LabImproveResults result={data} />;
  }
}

interface LabResultSectionProps {
  resultData: LabResultData;
}

export function LabResultSection({ resultData }: LabResultSectionProps) {
  const [manualResetKey, setManualResetKey] = useState(0);
  const prevDataRef = useRef(resultData);
  const dataChangeCountRef = useRef(0);

  // Auto-reset ErrorBoundary when resultData changes (no useEffect needed)
  if (prevDataRef.current !== resultData) {
    prevDataRef.current = resultData;
    dataChangeCountRef.current += 1;
  }

  const boundaryKey = `${dataChangeCountRef.current}-${manualResetKey}`;

  return (
    <ErrorBoundary
      key={boundaryKey}
      fallback={<LabResultErrorFallback onReset={() => setManualResetKey((k) => k + 1)} />}
    >
      <div className="mt-4">
        <LabResultContent resultData={resultData} />
      </div>
    </ErrorBoundary>
  );
}
