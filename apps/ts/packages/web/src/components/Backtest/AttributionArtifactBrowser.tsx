import { FileJson, Loader2, Search } from 'lucide-react';
import { useMemo, useState } from 'react';
import { JsonView, allExpanded, defaultStyles } from 'react-json-view-lite';
import 'react-json-view-lite/dist/index.css';
import { Card, CardContent } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { useAttributionArtifactContent, useAttributionArtifactFiles } from '@/hooks/useBacktest';
import type { AttributionArtifactInfo } from '@/types/backtest';

function formatDate(dateStr: string): string {
  return new Date(dateStr).toLocaleString('ja-JP', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  const kb = bytes / 1024;
  if (kb < 1024) return `${kb.toFixed(1)} KB`;
  return `${(kb / 1024).toFixed(1)} MB`;
}

function asRecord(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return null;
  }
  return value as Record<string, unknown>;
}

function readString(record: Record<string, unknown> | null, key: string): string | null {
  if (!record) return null;
  const value = record[key];
  return typeof value === 'string' ? value : null;
}

function readNumber(record: Record<string, unknown> | null, key: string): number | null {
  if (!record) return null;
  const value = record[key];
  return typeof value === 'number' ? value : null;
}

function asArray(value: unknown): unknown[] {
  return Array.isArray(value) ? value : [];
}

function toJsonViewData(value: unknown): Record<string, unknown> | unknown[] {
  if (Array.isArray(value)) {
    return value;
  }
  if (value && typeof value === 'object') {
    return value as Record<string, unknown>;
  }
  return { value };
}

function resolveParameterPath(
  root: unknown,
  pathParts: string[]
): { traversed: string[]; value: unknown } | null {
  let current: unknown = root;
  const traversed: string[] = [];

  for (const [index, part] of pathParts.entries()) {
    const currentRecord = asRecord(current);
    if (!currentRecord) return null;
    if (part in currentRecord) {
      current = currentRecord[part];
      traversed.push(part);
      continue;
    }

    // Fallback for payloads that store dotted keys as a single field
    // (e.g. "fundamental.per") instead of nested objects.
    const remainingPath = pathParts.slice(index).join('.');
    if (remainingPath in currentRecord) {
      current = currentRecord[remainingPath];
      traversed.push(remainingPath);
      return { traversed, value: current };
    }

    return null;
  }

  return { traversed, value: current };
}

function shouldExpandArtifactNode(level: number, _value: unknown, field?: string): boolean {
  if (level < 2) return true;
  return level === 2 && field === 'effective_parameters';
}

type BestScore = {
  signalId: string;
  score: number;
};

function selectBestScore(scores: Record<string, unknown>[]): BestScore | null {
  return scores.reduce<BestScore | null>((best, scoreItem) => {
    const signalId = readString(scoreItem, 'signal_id');
    const score = readNumber(scoreItem, 'score');
    if (!signalId || score == null || !Number.isFinite(score)) return best;
    if (!best || score > best.score) {
      return { signalId, score };
    }
    return best;
  }, null);
}

function resolveSignalParameter(
  effectiveParameters: Record<string, unknown> | null,
  signalId: string
): { parameterPath: string; value: unknown } | null {
  if (!effectiveParameters) return null;

  const [scope, ...paramPathParts] = signalId.split('.');
  if (paramPathParts.length === 0) return null;

  const sectionKey = scope === 'entry' ? 'entry_filter_params' : scope === 'exit' ? 'exit_trigger_params' : null;
  if (!sectionKey) return null;
  const section = effectiveParameters[sectionKey];
  const resolved = resolveParameterPath(section, paramPathParts);
  if (!resolved) return null;

  return {
    parameterPath: `${sectionKey}.${resolved.traversed.join('.')}`,
    value: resolved.value,
  };
}

function formatMetaValue(value: string | number | null): string {
  if (value === null || value === undefined || value === '') return '—';
  return String(value);
}

type FileListItemProps = {
  file: AttributionArtifactInfo;
  isSelected: boolean;
  onSelect: () => void;
};

function FileListItem({
  file,
  isSelected,
  onSelect,
}: FileListItemProps) {
  return (
    <button
      type="button"
      onClick={onSelect}
      className={`w-full flex items-start gap-3 p-3 text-left rounded-md transition-colors ${
        isSelected ? 'bg-primary/10 border border-primary/30' : 'hover:bg-muted'
      }`}
    >
      <FileJson className="h-4 w-4 mt-0.5 shrink-0 text-muted-foreground" />
      <div className="min-w-0 flex-1">
        <p className="text-sm font-medium truncate">{file.filename}</p>
        <div className="text-xs text-muted-foreground truncate">{file.strategy_name}</div>
        <div className="text-xs text-muted-foreground">{formatDate(file.created_at)}</div>
      </div>
    </button>
  );
}

type MetadataRowProps = {
  label: string;
  value: string | number | null;
};

function MetadataRow({ label, value }: MetadataRowProps) {
  return (
    <div className="rounded-md border bg-muted/30 p-2">
      <div className="text-[11px] text-muted-foreground">{label}</div>
      <div className="text-xs font-medium break-all">{formatMetaValue(value)}</div>
    </div>
  );
}

export function AttributionArtifactBrowser() {
  const [selectedStrategy, setSelectedStrategy] = useState<string>('all');
  const [searchQuery, setSearchQuery] = useState('');
  const [selectedFile, setSelectedFile] = useState<AttributionArtifactInfo | null>(null);

  const { data: filesData, isLoading: isLoadingFiles } = useAttributionArtifactFiles(
    selectedStrategy === 'all' ? undefined : selectedStrategy
  );
  const { data: artifactData, isLoading: isLoadingArtifact } = useAttributionArtifactContent(
    selectedFile?.strategy_name ?? null,
    selectedFile?.filename ?? null
  );

  const strategies = useMemo(() => {
    if (!filesData?.files) return [];
    const set = new Set(filesData.files.map((file) => file.strategy_name));
    return Array.from(set).sort();
  }, [filesData]);

  const filteredFiles = useMemo(() => {
    if (!filesData?.files) return [];
    if (!searchQuery) return filesData.files;
    const query = searchQuery.toLowerCase();
    return filesData.files.filter(
      (file) =>
        file.filename.toLowerCase().includes(query) ||
        file.strategy_name.toLowerCase().includes(query) ||
        (file.job_id ?? '').toLowerCase().includes(query)
    );
  }, [filesData, searchQuery]);

  const sortedFiles = useMemo(
    () => [...filteredFiles].sort((a, b) => new Date(b.created_at).getTime() - new Date(a.created_at).getTime()),
    [filteredFiles]
  );

  const artifact = useMemo(() => asRecord(artifactData?.artifact), [artifactData]);

  const metadata = useMemo(() => {
    const strategy = asRecord(artifact?.strategy);
    const runtime = asRecord(artifact?.runtime);
    const databases = asRecord(artifact?.databases);
    const marketDb = asRecord(databases?.market_db);
    const portfolioDb = asRecord(databases?.portfolio_db);
    return {
      savedAt: readString(artifact, 'saved_at'),
      strategyName: readString(strategy, 'name'),
      yamlPath: readString(strategy, 'yaml_path'),
      shapleyTopN: readNumber(runtime, 'shapley_top_n'),
      shapleyPermutations: readNumber(runtime, 'shapley_permutations'),
      randomSeed: readNumber(runtime, 'random_seed'),
      datasetName: readString(databases, 'dataset_name'),
      marketDbName: readString(marketDb, 'name'),
      portfolioDbName: readString(portfolioDb, 'name'),
    };
  }, [artifact]);

  const effectiveParameters = useMemo(() => {
    const strategy = asRecord(artifact?.strategy);
    const parameters = strategy?.effective_parameters;
    if (parameters === undefined || parameters === null) return null;
    return parameters;
  }, [artifact]);

  const bestSignalParameter = useMemo(() => {
    const result = asRecord(artifact?.result);
    const strategy = asRecord(artifact?.strategy);
    const effectiveParameters = asRecord(strategy?.effective_parameters);
    const topNSelection = asRecord(result?.top_n_selection);
    const scores = asArray(topNSelection?.scores)
      .map((item) => asRecord(item))
      .filter((item): item is Record<string, unknown> => item !== null);

    const bestScore = selectBestScore(scores);

    if (!bestScore) return null;

    const signalInfo = asArray(result?.signals)
      .map((item) => asRecord(item))
      .find((item) => readString(item, 'signal_id') === bestScore.signalId);

    const resolved = resolveSignalParameter(effectiveParameters, bestScore.signalId);

    return {
      signalId: bestScore.signalId,
      signalName: readString(signalInfo ?? null, 'signal_name'),
      score: bestScore.score,
      scope: readString(signalInfo ?? null, 'scope'),
      parameterPath: resolved?.parameterPath ?? null,
      parameterValue: resolved?.value ?? null,
    };
  }, [artifact]);

  const totalCount = filesData?.total ?? 0;
  const showTotalCount = totalCount > sortedFiles.length;

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-4">
        <div className="flex-1">
          <div className="relative">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
            <Input
              placeholder="Search attribution artifacts..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="pl-10"
            />
          </div>
        </div>
        <Select value={selectedStrategy} onValueChange={setSelectedStrategy}>
          <SelectTrigger className="w-[260px]">
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
        <Card className="lg:col-span-1">
          <CardContent className="pt-4">
            {isLoadingFiles ? (
              <div className="flex items-center justify-center h-48">
                <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
              </div>
            ) : sortedFiles.length === 0 ? (
              <div className="flex flex-col items-center justify-center h-48 text-muted-foreground">
                <FileJson className="h-10 w-10 mb-2" />
                <p className="text-sm">No attribution artifacts found</p>
              </div>
            ) : (
              <div className="space-y-4 max-h-[680px] overflow-y-auto">
                <p className="text-sm text-muted-foreground">
                  {sortedFiles.length} files {showTotalCount ? `(${totalCount} total)` : ''}
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

        <Card className="lg:col-span-2">
          <CardContent className="pt-4">
            {!selectedFile ? (
              <div className="flex flex-col items-center justify-center h-[680px] text-muted-foreground">
                <FileJson className="h-12 w-12 mb-3" />
                <p className="text-sm">Select an attribution artifact from the list</p>
              </div>
            ) : isLoadingArtifact ? (
              <div className="flex items-center justify-center h-[680px]">
                <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
              </div>
            ) : (
              <div className="space-y-4">
                <div>
                  <h3 className="font-medium break-all">{selectedFile.filename}</h3>
                  <p className="text-xs text-muted-foreground break-all">
                    {selectedFile.strategy_name} | {formatDate(selectedFile.created_at)} |{' '}
                    {formatBytes(selectedFile.size_bytes)}
                  </p>
                </div>

                <div className="grid gap-2 md:grid-cols-2">
                  <MetadataRow label="Saved At" value={metadata.savedAt} />
                  <MetadataRow label="Strategy" value={metadata.strategyName} />
                  <MetadataRow label="Dataset" value={metadata.datasetName} />
                  <MetadataRow label="YAML Path" value={metadata.yamlPath} />
                  <MetadataRow label="Shapley Top N" value={metadata.shapleyTopN} />
                  <MetadataRow label="Permutations" value={metadata.shapleyPermutations} />
                  <MetadataRow label="Random Seed" value={metadata.randomSeed} />
                  <MetadataRow label="Market DB" value={metadata.marketDbName} />
                  <MetadataRow label="Portfolio DB" value={metadata.portfolioDbName} />
                  <MetadataRow label="Job ID" value={selectedFile.job_id} />
                </div>

                {bestSignalParameter && (
                  <div className="space-y-2 rounded-md border bg-muted/20 p-3">
                    <h4 className="text-sm font-medium">Best Signal Parameters</h4>
                    <div className="grid gap-2 md:grid-cols-2">
                      <MetadataRow label="Signal ID" value={bestSignalParameter.signalId} />
                      <MetadataRow label="Signal Name" value={bestSignalParameter.signalName} />
                      <MetadataRow label="Scope" value={bestSignalParameter.scope} />
                      <MetadataRow label="Top-N Score" value={bestSignalParameter.score.toFixed(6)} />
                      <MetadataRow label="Parameter Path" value={bestSignalParameter.parameterPath} />
                    </div>
                    <div>
                      <h5 className="mb-2 text-xs font-medium text-muted-foreground">Effective Parameter JSON</h5>
                      <div className="max-h-[260px] overflow-auto rounded-md border bg-background p-3 text-xs">
                        {bestSignalParameter.parameterPath ? (
                          <JsonView
                            data={toJsonViewData(bestSignalParameter.parameterValue)}
                            shouldExpandNode={allExpanded}
                            style={defaultStyles}
                          />
                        ) : (
                          <p className="text-muted-foreground">
                            Parameter value could not be resolved from strategy.effective_parameters.
                          </p>
                        )}
                      </div>
                    </div>
                  </div>
                )}

                {effectiveParameters !== null && (
                  <div>
                    <h4 className="text-sm font-medium mb-2">Effective Parameters</h4>
                    <div className="max-h-[320px] overflow-auto rounded-md border bg-muted/30 p-3 text-xs">
                      <JsonView data={toJsonViewData(effectiveParameters)} shouldExpandNode={allExpanded} style={defaultStyles} />
                    </div>
                  </div>
                )}

                <div>
                  <h4 className="text-sm font-medium mb-2">JSON</h4>
                  <div className="max-h-[420px] overflow-auto rounded-md border bg-muted/30 p-3 text-xs">
                    <JsonView
                      data={toJsonViewData(artifactData?.artifact ?? {})}
                      shouldExpandNode={shouldExpandArtifactNode}
                      style={defaultStyles}
                    />
                  </div>
                </div>
              </div>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
