import { ArrowDown, ArrowUp, ArrowUpDown, Database, Info, RefreshCw, Trash2 } from 'lucide-react';
import { useMemo, useState } from 'react';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { useDatasets, useResumeDataset } from '@/hooks/useDataset';
import { useBacktestStore } from '@/stores/backtestStore';
import type { DatasetListItem } from '@/types/dataset';
import { formatBytes } from '@/utils/formatters';
import { DatasetDeleteDialog } from './DatasetDeleteDialog';
import { DatasetInfoDialog } from './DatasetInfoDialog';

type SortKey = 'name' | 'preset' | 'fileSize' | 'lastModified';
type SortDir = 'asc' | 'desc';

function compareItems(a: DatasetListItem, b: DatasetListItem, key: SortKey, dir: SortDir): number {
  let cmp: number;
  switch (key) {
    case 'name':
      cmp = a.name.localeCompare(b.name);
      break;
    case 'preset':
      cmp = (a.preset ?? '').localeCompare(b.preset ?? '');
      break;
    case 'fileSize':
      cmp = a.fileSize - b.fileSize;
      break;
    case 'lastModified':
      cmp = new Date(a.lastModified).getTime() - new Date(b.lastModified).getTime();
      break;
  }
  return dir === 'asc' ? cmp : -cmp;
}

function SortIcon({ column, sortKey, sortDir }: { column: SortKey; sortKey: SortKey; sortDir: SortDir }) {
  if (column !== sortKey) return <ArrowUpDown className="ml-1 h-3 w-3 opacity-40" />;
  return sortDir === 'asc' ? <ArrowUp className="ml-1 h-3 w-3" /> : <ArrowDown className="ml-1 h-3 w-3" />;
}

export function DatasetList() {
  const { data, isLoading, isError, error, refetch } = useDatasets();
  const { setActiveDatasetJobId } = useBacktestStore();
  const resumeDataset = useResumeDataset();

  const [infoDataset, setInfoDataset] = useState<string | null>(null);
  const [deleteDataset, setDeleteDataset] = useState<string | null>(null);
  const [sortKey, setSortKey] = useState<SortKey>('lastModified');
  const [sortDir, setSortDir] = useState<SortDir>('desc');

  const datasets = data ?? [];

  const sortedDatasets = useMemo(() => {
    const list = data ?? [];
    return [...list].sort((a, b) => compareItems(a, b, sortKey, sortDir));
  }, [data, sortKey, sortDir]);

  const toggleSort = (key: SortKey) => {
    if (sortKey === key) {
      setSortDir((d) => (d === 'asc' ? 'desc' : 'asc'));
    } else {
      setSortKey(key);
      setSortDir(key === 'name' || key === 'preset' ? 'asc' : 'desc');
    }
  };

  const handleResume = (item: DatasetListItem) => {
    if (!item.preset) return;
    resumeDataset.mutate(
      { name: item.name, preset: item.preset, timeoutMinutes: 30 },
      {
        onSuccess: (resp) => {
          setActiveDatasetJobId(resp.jobId);
        },
      }
    );
  };

  const headerClass = 'cursor-pointer select-none hover:text-foreground';

  return (
    <>
      <Card>
        <CardHeader className="pb-3">
          <div className="flex items-center justify-between">
            <CardTitle className="text-base flex items-center gap-2">
              <Database className="h-4 w-4" />
              データセット一覧
            </CardTitle>
            <Button variant="ghost" size="sm" onClick={() => refetch()}>
              <RefreshCw className="h-4 w-4" />
            </Button>
          </div>
        </CardHeader>
        <CardContent>
          {isLoading && <p className="text-sm text-muted-foreground">読み込み中...</p>}
          {isError && <p className="text-sm text-destructive">Error: {error.message}</p>}

          {data && datasets.length === 0 && (
            <p className="text-sm text-muted-foreground py-4 text-center">データセットがありません</p>
          )}

          {sortedDatasets.length > 0 && (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead className={headerClass} onClick={() => toggleSort('name')}>
                    <span className="inline-flex items-center">
                      Name
                      <SortIcon column="name" sortKey={sortKey} sortDir={sortDir} />
                    </span>
                  </TableHead>
                  <TableHead className={headerClass} onClick={() => toggleSort('preset')}>
                    <span className="inline-flex items-center">
                      Preset
                      <SortIcon column="preset" sortKey={sortKey} sortDir={sortDir} />
                    </span>
                  </TableHead>
                  <TableHead className={`text-right ${headerClass}`} onClick={() => toggleSort('fileSize')}>
                    <span className="inline-flex items-center justify-end w-full">
                      Size
                      <SortIcon column="fileSize" sortKey={sortKey} sortDir={sortDir} />
                    </span>
                  </TableHead>
                  <TableHead className={headerClass} onClick={() => toggleSort('lastModified')}>
                    <span className="inline-flex items-center">
                      Modified
                      <SortIcon column="lastModified" sortKey={sortKey} sortDir={sortDir} />
                    </span>
                  </TableHead>
                  <TableHead className="text-right">Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {sortedDatasets.map((item) => (
                  <TableRow key={item.name}>
                    <TableCell className="font-medium">{item.name}</TableCell>
                    <TableCell>{item.preset ?? '-'}</TableCell>
                    <TableCell className="text-right">{formatBytes(item.fileSize)}</TableCell>
                    <TableCell>{new Date(item.lastModified).toLocaleDateString('ja-JP')}</TableCell>
                    <TableCell className="text-right">
                      <div className="flex items-center justify-end gap-1">
                        <Button variant="ghost" size="sm" onClick={() => setInfoDataset(item.name)} title="詳細">
                          <Info className="h-4 w-4" />
                        </Button>
                        {item.preset && (
                          <Button
                            variant="ghost"
                            size="sm"
                            onClick={() => handleResume(item)}
                            disabled={resumeDataset.isPending}
                            title="レジューム"
                          >
                            <RefreshCw className="h-4 w-4" />
                          </Button>
                        )}
                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={() => setDeleteDataset(item.name)}
                          title="削除"
                          className="text-destructive hover:text-destructive"
                        >
                          <Trash2 className="h-4 w-4" />
                        </Button>
                      </div>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}

          {resumeDataset.isError && (
            <p className="text-sm text-destructive mt-2">Resume Error: {resumeDataset.error.message}</p>
          )}
        </CardContent>
      </Card>

      <DatasetInfoDialog
        open={!!infoDataset}
        onOpenChange={(open) => !open && setInfoDataset(null)}
        datasetName={infoDataset}
      />

      {deleteDataset && (
        <DatasetDeleteDialog
          open={!!deleteDataset}
          onOpenChange={(open) => !open && setDeleteDataset(null)}
          datasetName={deleteDataset}
        />
      )}
    </>
  );
}
