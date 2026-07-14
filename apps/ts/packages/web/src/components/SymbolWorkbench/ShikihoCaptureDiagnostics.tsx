import type {
  ShikihoCaptureTraceV1,
  ShikihoFieldMilestonesV1,
  ShikihoTracePhase,
  ShikihoWaitEndReason,
} from '@trading25/shikiho-extension/contract';
import { ChevronDown, ChevronUp } from 'lucide-react';
import { useId, useState } from 'react';

interface ShikihoCaptureDiagnosticsProps {
  trace: ShikihoCaptureTraceV1;
}

const phaseLabels: Record<ShikihoTracePhase, string> = {
  queued: '待機中',
  probing_tabs: 'タブ確認',
  acquiring_tab: 'タブ準備',
  waiting_receiver: 'Receiver待ち',
  observing_dom: 'DOM確認',
  core_partial: '項目取得中',
  core_ready: '主要項目取得',
  settling: '安定確認',
  saving: '保存中',
  complete: '取得完了',
  timeout: 'タイムアウト',
  error: '取得エラー',
};

const waitReasonLabels: Record<ShikihoWaitEndReason, string> = {
  field_stable: '項目が安定',
  deadline: '期限到達',
  login_confirmed: 'ログイン要求を確認',
  navigation_changed: 'ページ遷移を検知',
  invalid_response: '応答形式エラー',
  error: '取得エラー',
};

const milestoneLabels: Array<[keyof ShikihoFieldMilestonesV1, string]> = [
  ['identity', '銘柄'],
  ['quote', '株価'],
  ['features', '特色'],
  ['consolidatedBusinesses', '連結事業'],
  ['commentary', 'コメント'],
  ['score', 'スコア'],
  ['comparisonCompanies', '比較会社'],
  ['industries', '業種'],
  ['marketThemes', 'テーマ'],
  ['profile', '会社概要'],
  ['editionLabel', '版'],
  ['pageUpdatedAt', '更新日時'],
  ['coreReady', '主要項目完了'],
];

function formatElapsed(milliseconds: number | null): string {
  if (milliseconds === null) return '—';
  if (milliseconds < 1_000) return `${milliseconds}ms`;
  return `${(milliseconds / 1_000).toFixed(1)}秒`;
}

function Metric({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="flex min-w-0 items-baseline justify-between gap-3">
      <dt className="shrink-0 text-muted-foreground">{label}</dt>
      <dd className="min-w-0 text-right font-medium text-foreground">{value}</dd>
    </div>
  );
}

export function ShikihoCaptureDiagnostics({ trace }: ShikihoCaptureDiagnosticsProps) {
  const [isExpanded, setIsExpanded] = useState(false);
  const detailsId = useId();
  const phaseElapsed = trace.timings.totalMs;

  return (
    <>
      <span
        data-phase={trace.phase}
        className="rounded-full bg-sky-500/10 px-2 py-0.5 text-[11px] font-medium text-sky-700 dark:text-sky-300"
      >
        {phaseLabels[trace.phase]} {formatElapsed(phaseElapsed)}
      </span>
      <button
        type="button"
        aria-expanded={isExpanded}
        aria-controls={detailsId}
        className="inline-flex h-7 items-center gap-1 rounded-md px-1.5 text-xs font-medium text-muted-foreground hover:bg-[var(--app-surface-muted)] hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
        onClick={() => setIsExpanded((expanded) => !expanded)}
      >
        取得診断
        {isExpanded ? (
          <ChevronUp className="h-3 w-3" aria-hidden="true" />
        ) : (
          <ChevronDown className="h-3 w-3" aria-hidden="true" />
        )}
      </button>
      <div
        id={detailsId}
        hidden={!isExpanded}
        className="basis-full rounded-lg border border-border/60 bg-[var(--app-surface-muted)] p-2.5 text-[11px]"
      >
        <div className="grid gap-x-5 gap-y-3 sm:grid-cols-2 lg:grid-cols-3">
          <dl className="space-y-1.5">
            <Metric label="Tab探索" value={formatElapsed(trace.timings.probeMs)} />
            <Metric label="Tab準備" value={formatElapsed(trace.timings.acquisitionMs)} />
            <Metric
              label="Receiver待ち"
              value={`${formatElapsed(trace.receiverReadyMs === null ? null : trace.timings.receiverMs)}（${trace.receiverAttempts}回）`}
            />
            <Metric label="DOM待ち" value={formatElapsed(trace.timings.domObservationMs)} />
            <Metric label="保存" value={formatElapsed(trace.timings.storageMs)} />
            <Metric label="合計" value={formatElapsed(trace.timings.totalMs)} />
            <Metric
              label="DOM観測"
              value={`DOM更新 ${trace.dom.mutationBatches} / 有効変化 ${trace.dom.meaningfulChanges}`}
            />
            <Metric label="DOM抽出" value={`${trace.dom.samples}サンプル`} />
            <Metric
              label="抽出処理"
              value={`抽出 ${trace.extraction.samples}回 / 合計 ${formatElapsed(trace.extraction.totalMs)} / 最大 ${formatElapsed(trace.extraction.maxMs)}`}
            />
            <Metric
              label="終了理由"
              value={trace.waitEndReason === null ? '—' : waitReasonLabels[trace.waitEndReason]}
            />
          </dl>

          <dl className="space-y-1.5">
            <Metric label="responseStart" value={formatElapsed(trace.navigation.responseStartMs)} />
            <Metric label="DOM interactive" value={formatElapsed(trace.navigation.domInteractiveMs)} />
            <Metric label="DOMContentLoaded" value={formatElapsed(trace.navigation.domContentLoadedMs)} />
            <Metric label="load" value={formatElapsed(trace.navigation.loadEndMs)} />
          </dl>

          <dl className="space-y-1.5">
            {milestoneLabels.map(([key, label]) => (
              <Metric key={key} label={label} value={formatElapsed(trace.dom.firstSeenMs[key])} />
            ))}
          </dl>
        </div>
      </div>
    </>
  );
}
