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
  const [filename, setFilename] = useState('quickTesting.db');
  const [overwrite, setOverwrite] = useState(false);
  const [timeoutMinutes, setTimeoutMinutes] = useState(30);

  const { activeDatasetJobId, setActiveDatasetJobId } = useBacktestStore();
  const createDataset = useCreateDataset();
  const resumeDataset = useResumeDataset();

  const isJobActive = !!activeDatasetJobId;

  // Update filename when preset changes
  useEffect(() => {
    setFilename(`${selectedPreset}.db`);
  }, [selectedPreset]);

  const presetInfo = DATASET_PRESETS.find((p) => p.value === selectedPreset);

  const handleCreate = () => {
    createDataset.mutate(
      { name: filename, preset: selectedPreset, overwrite, timeoutMinutes },
      {
        onSuccess: (data) => {
          setActiveDatasetJobId(data.jobId);
        },
      }
    );
  };

  const handleResume = () => {
    resumeDataset.mutate(
      { name: filename, preset: selectedPreset, timeoutMinutes },
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
        <CardTitle className="text-base">データセット作成</CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
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
          <Label htmlFor="filename">ファイル名</Label>
          <Input
            id="filename"
            value={filename}
            onChange={(e) => setFilename(e.target.value)}
            placeholder="dataset.db"
          />
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
          <Button onClick={handleCreate} disabled={isJobActive || createDataset.isPending || !filename}>
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
            disabled={isJobActive || resumeDataset.isPending || !filename}
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
