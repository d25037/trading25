import { Loader2, Plus } from 'lucide-react';
import { useEffect, useState } from 'react';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { useCreateDataset } from '@/hooks/useDataset';
import { useBacktestStore } from '@/stores/backtestStore';
import { DATASET_PRESETS } from '@/types/dataset';
import { DatasetJobProgress } from './DatasetJobProgress';

export function DatasetCreateForm() {
  const [selectedPreset, setSelectedPreset] = useState('quickTesting');
  const [datasetName, setDatasetName] = useState('quickTesting');
  const [overwrite, setOverwrite] = useState(false);

  const { activeDatasetJobId, setActiveDatasetJobId } = useBacktestStore();
  const createDataset = useCreateDataset();

  const isJobActive = !!activeDatasetJobId;

  // Keep the default dataset name aligned with the selected preset.
  useEffect(() => {
    setDatasetName(selectedPreset);
  }, [selectedPreset]);

  const presetInfo = DATASET_PRESETS.find((p) => p.value === selectedPreset);

  const normalizedDatasetName = datasetName.trim().replace(/\.db$/i, '');

  const handleCreate = () => {
    createDataset.mutate(
      { name: normalizedDatasetName, preset: selectedPreset, overwrite },
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
          新規 dataset は snapshot として保存されます。出力は <code>dataset.duckdb</code>、
          <code>parquet/</code>、<code>manifest.v2.json</code> です。
        </p>
        <p className="text-xs text-amber-700">
          <code>作成</code> は <code>market.duckdb</code> を source of truth とした batch copy で dataset
          snapshot を作成します。J-Quants へは fetch しません。
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
            <code>{normalizedDatasetName || '<name>'}/parquet/</code>、
            <code>{normalizedDatasetName || '<name>'}/manifest.v2.json</code>
          </p>
        </div>

        <label className="flex items-center gap-2 text-sm">
          <input type="checkbox" checked={overwrite} onChange={(e) => setOverwrite(e.target.checked)} />
          既存 dataset を上書きして作り直す
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
        </div>

        {createDataset.isError && <p className="text-sm text-destructive">Error: {createDataset.error.message}</p>}

        <DatasetJobProgress />
      </CardContent>
    </Card>
  );
}
