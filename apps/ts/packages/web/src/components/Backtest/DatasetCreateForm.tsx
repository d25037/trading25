import { Loader2, Plus, RefreshCw } from 'lucide-react';
import { useEffect, useState } from 'react';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { useCreateDataset, useResumeDataset } from '@/hooks/useDataset';
import { useBacktestStore } from '@/stores/backtestStore';
import { DATASET_PRESETS } from '@/types/dataset';
import { DatasetJobProgress } from './DatasetJobProgress';

export function DatasetCreateForm() {
  const [selectedPreset, setSelectedPreset] = useState('quickTesting');
  const [datasetName, setDatasetName] = useState('quickTesting');
  const [overwrite, setOverwrite] = useState(false);
  const [timeoutMinutes, setTimeoutMinutes] = useState(35);

  const { activeDatasetJobId, setActiveDatasetJobId } = useBacktestStore();
  const createDataset = useCreateDataset();
  const resumeDataset = useResumeDataset();

  const isJobActive = !!activeDatasetJobId;

  // Keep the default dataset name aligned with the selected preset.
  useEffect(() => {
    setDatasetName(selectedPreset);
  }, [selectedPreset]);

  const presetInfo = DATASET_PRESETS.find((p) => p.value === selectedPreset);

  const normalizedDatasetName = datasetName.trim().replace(/\.db$/i, '');

  const handleCreate = () => {
    createDataset.mutate(
      { name: normalizedDatasetName, preset: selectedPreset, overwrite, timeoutMinutes },
      {
        onSuccess: (data) => {
          setActiveDatasetJobId(data.jobId);
        },
      }
    );
  };

  const handleResume = () => {
    resumeDataset.mutate(
      { name: normalizedDatasetName, preset: selectedPreset, timeoutMinutes },
      {
        onSuccess: (data) => {
          setActiveDatasetJobId(data.jobId);
        },
      }
    );
  };

  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle className="text-base">DuckDB データセット作成</CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <p className="text-xs text-muted-foreground">
          新規 dataset は snapshot として保存されます。出力は <code>dataset.duckdb</code> と
          <code>parquet/</code> が正本で、<code>dataset.db</code> は互換用です。
        </p>
        <p className="text-xs text-amber-700">
          <code>作成</code> と <code>レジューム</code> は <code>market.duckdb</code> を source of truth として
          dataset snapshot を作成します。J-Quants へは fetch しません。
        </p>

        <div className="space-y-2">
          <Label htmlFor="preset">プリセット</Label>
          <Select value={selectedPreset} onValueChange={setSelectedPreset}>
            <SelectTrigger id="preset">
              <SelectValue placeholder="プリセットを選択" />
            </SelectTrigger>
            <SelectContent>
              {DATASET_PRESETS.map((preset) => (
                <SelectItem key={preset.value} value={preset.value}>
                  {preset.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          {presetInfo && (
            <p className="text-xs text-muted-foreground">
              {presetInfo.description} (推定: {presetInfo.estimatedTime})
            </p>
          )}
        </div>

        <div className="space-y-2">
          <Label htmlFor="dataset-name">データセット名</Label>
          <Input
            id="dataset-name"
            value={datasetName}
            onChange={(e) => setDatasetName(e.target.value)}
            placeholder="quickTesting"
          />
          <p className="text-xs text-muted-foreground">
            出力先: <code>{normalizedDatasetName || '<name>'}/dataset.duckdb</code> と
            <code>{normalizedDatasetName || '<name>'}/parquet/</code>
          </p>
        </div>

        <div className="space-y-2">
          <Label htmlFor="timeout">タイムアウト(分)</Label>
          <Input
            id="timeout"
            type="number"
            min={1}
            max={120}
            value={timeoutMinutes}
            onChange={(e) => setTimeoutMinutes(Number(e.target.value))}
          />
        </div>

        <label className="flex items-center gap-2 text-sm">
          <input type="checkbox" checked={overwrite} onChange={(e) => setOverwrite(e.target.checked)} />
          既存ファイルを上書き
        </label>

        <div className="flex gap-2">
          <Button onClick={handleCreate} disabled={isJobActive || createDataset.isPending || !normalizedDatasetName}>
            {createDataset.isPending ? (
              <Loader2 className="mr-2 h-4 w-4 animate-spin" />
            ) : (
              <Plus className="mr-2 h-4 w-4" />
            )}
            作成
          </Button>
          <Button
            variant="outline"
            onClick={handleResume}
            disabled={isJobActive || resumeDataset.isPending || !normalizedDatasetName}
          >
            {resumeDataset.isPending ? (
              <Loader2 className="mr-2 h-4 w-4 animate-spin" />
            ) : (
              <RefreshCw className="mr-2 h-4 w-4" />
            )}
            レジューム
          </Button>
        </div>

        {createDataset.isError && <p className="text-sm text-destructive">Error: {createDataset.error.message}</p>}
        {resumeDataset.isError && <p className="text-sm text-destructive">Error: {resumeDataset.error.message}</p>}

        <DatasetJobProgress />
      </CardContent>
    </Card>
  );
}
