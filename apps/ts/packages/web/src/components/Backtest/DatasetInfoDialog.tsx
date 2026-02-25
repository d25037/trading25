import { AlertCircle, AlertTriangle, CheckCircle2, Database, Loader2 } from 'lucide-react';
import { Button } from '@/components/ui/button';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { useDatasetInfo } from '@/hooks/useDataset';
import type { DatasetInfoResponse } from '@/types/dataset';
import { formatBytes } from '@/utils/formatters';

interface DatasetInfoDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  datasetName: string | null;
}

function DataCoverageSection({ info }: { info: DatasetInfoResponse }) {
  const coverage = info.validation.details?.dataCoverage;
  if (!coverage) return null;

  return (
    <div>
      <h4 className="font-medium mb-2">データカバレッジ</h4>
      <div className="grid grid-cols-2 gap-2">
        <div className="text-muted-foreground">Quotes</div>
        <div>
          {coverage.stocksWithQuotes} / {coverage.totalStocks}
        </div>
        {info.stats.hasStatementsData && (
          <>
            <div className="text-muted-foreground">Statements</div>
            <div>
              {coverage.stocksWithStatements} / {coverage.totalStocks}
            </div>
          </>
        )}
        {info.stats.hasMarginData && (
          <>
            <div className="text-muted-foreground">Margin</div>
            <div>
              {coverage.stocksWithMargin} / {coverage.totalStocks}
            </div>
          </>
        )}
      </div>
    </div>
  );
}

function StatementsSchemaSection({ info }: { info: DatasetInfoResponse }) {
  const fc = info.stats.statementsFieldCoverage;
  if (!fc) return null;

  return (
    <div>
      <h4 className="font-medium mb-2">Statementsスキーマ</h4>
      <div className="flex flex-wrap gap-2">
        <span
          className={`rounded px-2 py-0.5 text-xs ${
            fc.hasExtendedFields ? 'bg-green-500/10 text-green-500' : 'bg-red-500/10 text-red-500'
          }`}
        >
          Extended: {fc.hasExtendedFields ? 'OK' : 'N/A'}
        </span>
        <span
          className={`rounded px-2 py-0.5 text-xs ${
            fc.hasCashFlowFields ? 'bg-green-500/10 text-green-500' : 'bg-yellow-500/10 text-yellow-500'
          }`}
        >
          {fc.hasCashFlowFields ? (
            'CF: OK'
          ) : (
            <span className="flex items-center gap-1">
              <AlertTriangle className="h-3 w-3" />
              CF: N/A (v1)
            </span>
          )}
        </span>
      </div>
    </div>
  );
}

function ValidationSection({ info }: { info: DatasetInfoResponse }) {
  const details = info.validation.details;
  const hasStockCountValidation = !!details?.stockCountValidation;
  const hasDateGapCount = typeof details?.dateGapsCount === 'number';
  const hasOrphanStocksCount = typeof details?.orphanStocksCount === 'number';
  const hasFkIntegrity = !!details?.fkIntegrity;

  return (
    <div>
      <h4 className="font-medium mb-2 flex items-center gap-1">
        Validation
        {info.validation.isValid ? (
          <CheckCircle2 className="h-4 w-4 text-green-500" />
        ) : (
          <AlertCircle className="h-4 w-4 text-red-500" />
        )}
      </h4>
      {info.validation.errors.length > 0 && (
        <ul className="space-y-1">
          {info.validation.errors.map((err) => (
            <li key={err} className="text-xs text-destructive">
              {err}
            </li>
          ))}
        </ul>
      )}
      {info.validation.warnings.length > 0 && (
        <ul className="space-y-1 mt-1">
          {info.validation.warnings.map((warn) => (
            <li key={warn} className="text-xs text-yellow-500">
              {warn}
            </li>
          ))}
        </ul>
      )}
      {(hasStockCountValidation || hasDateGapCount || hasOrphanStocksCount || hasFkIntegrity) && (
        <div className="mt-2 grid grid-cols-2 gap-1 text-xs">
          {hasStockCountValidation && details?.stockCountValidation && (
            <>
              <div className="text-muted-foreground">Stock count</div>
              <div>
                {details.stockCountValidation.actual.toLocaleString()} /{' '}
                {details.stockCountValidation.expected
                  ? `${details.stockCountValidation.expected.min.toLocaleString()}-${details.stockCountValidation.expected.max.toLocaleString()}`
                  : 'N/A'}
              </div>
            </>
          )}
          {hasDateGapCount && (
            <>
              <div className="text-muted-foreground">Date gaps</div>
              <div>{details?.dateGapsCount}</div>
            </>
          )}
          {hasOrphanStocksCount && (
            <>
              <div className="text-muted-foreground">Stocks without quotes</div>
              <div>{details?.orphanStocksCount?.toLocaleString() ?? 0}</div>
            </>
          )}
          {hasFkIntegrity && details?.fkIntegrity && (
            <>
              <div className="text-muted-foreground">FK integrity</div>
              <div>
                stock:{details.fkIntegrity.stockDataOrphans} / margin:{details.fkIntegrity.marginDataOrphans} /
                statements:{details.fkIntegrity.statementsOrphans}
              </div>
            </>
          )}
        </div>
      )}
      {info.validation.isValid && info.validation.warnings.length === 0 && (
        <p className="text-xs text-muted-foreground">問題なし</p>
      )}
    </div>
  );
}

export function DatasetInfoDialog({ open, onOpenChange, datasetName }: DatasetInfoDialogProps) {
  const { data: info, isLoading, isError, error } = useDatasetInfo(open ? datasetName : null);

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-lg">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Database className="h-5 w-5" />
            {datasetName}
          </DialogTitle>
          <DialogDescription>データセット詳細情報</DialogDescription>
        </DialogHeader>

        {isLoading && (
          <div className="flex items-center justify-center py-8">
            <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
          </div>
        )}

        {isError && <p className="text-sm text-destructive">Error: {error.message}</p>}

        {info && (
          <div className="space-y-4 text-sm">
            <div className="grid grid-cols-2 gap-2">
              <div className="text-muted-foreground">Preset</div>
              <div>{info.snapshot.preset ?? 'N/A'}</div>
              <div className="text-muted-foreground">ファイルサイズ</div>
              <div>{formatBytes(info.fileSize)}</div>
              <div className="text-muted-foreground">作成日時</div>
              <div>{info.snapshot.createdAt ? new Date(info.snapshot.createdAt).toLocaleString('ja-JP') : 'N/A'}</div>
              <div className="text-muted-foreground">更新日時</div>
              <div>{new Date(info.lastModified).toLocaleString('ja-JP')}</div>
            </div>

            <div>
              <h4 className="font-medium mb-2">統計情報</h4>
              <div className="grid grid-cols-2 gap-2">
                <div className="text-muted-foreground">銘柄数</div>
                <div>{info.stats.totalStocks.toLocaleString()}</div>
                <div className="text-muted-foreground">株価レコード数</div>
                <div>{info.stats.totalQuotes.toLocaleString()}</div>
                <div className="text-muted-foreground">日付範囲</div>
                <div>
                  {info.stats.dateRange.from} ~ {info.stats.dateRange.to}
                </div>
              </div>
              <div className="flex flex-wrap gap-2 mt-2">
                {info.stats.hasMarginData && (
                  <span className="rounded bg-blue-500/10 px-2 py-0.5 text-xs text-blue-500">Margin</span>
                )}
                {info.stats.hasTOPIXData && (
                  <span className="rounded bg-green-500/10 px-2 py-0.5 text-xs text-green-500">TOPIX</span>
                )}
                {info.stats.hasSectorData && (
                  <span className="rounded bg-purple-500/10 px-2 py-0.5 text-xs text-purple-500">Sector</span>
                )}
                {info.stats.hasStatementsData && (
                  <span className="rounded bg-orange-500/10 px-2 py-0.5 text-xs text-orange-500">Statements</span>
                )}
              </div>
            </div>

            <DataCoverageSection info={info} />
            <StatementsSchemaSection info={info} />
            <ValidationSection info={info} />
          </div>
        )}

        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            閉じる
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
