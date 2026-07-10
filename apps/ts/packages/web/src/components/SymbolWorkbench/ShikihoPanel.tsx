import type { ShikihoCaptureDiagnosticV1, ShikihoSnapshotV1 } from '@trading25/shikiho-extension/contract';
import { ChevronDown, ChevronUp, ExternalLink } from 'lucide-react';
import { useState } from 'react';
import type { ShikihoCaptureState } from '@/hooks/useShikihoSnapshot';
import { cn } from '@/lib/utils';

interface ShikihoPanelProps {
  symbol: string;
  snapshot: ShikihoSnapshotV1 | null;
  diagnostic: ShikihoCaptureDiagnosticV1 | null;
  captureState: ShikihoCaptureState;
  onSelectSymbol: (symbol: string) => void;
}

const statusLabels: Record<ShikihoCaptureState, string> = {
  checking_extension: '拡張機能を確認中',
  extension_unavailable: '拡張機能が見つかりません',
  not_captured: '未取得',
  captured: '取得済み',
  partial: '一部取得',
  stale: '古いスナップショット',
  login_required: 'ログインが必要です',
  page_changed: 'ページ構造の変更を検知しました',
  storage_error: '保存エラー',
};

const scoreLabels: Array<[keyof ShikihoSnapshotV1['score'], string]> = [
  ['overall', '総合'],
  ['growth', '成長性'],
  ['profitability', '収益性'],
  ['safety', '安全性'],
  ['scale', '規模'],
  ['value', '割安度'],
  ['priceMomentum', '値上がり'],
];

function formatCapturedAt(capturedAt: string): string {
  return new Date(capturedAt).toLocaleString('ja-JP', {
    dateStyle: 'short',
    timeStyle: 'short',
  });
}

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="space-y-1.5">
      <h4 className="text-xs font-semibold tracking-wide text-muted-foreground">{title}</h4>
      {children}
    </div>
  );
}

function ChipList({ values }: { values: string[] }) {
  return (
    <div className="flex flex-wrap gap-1.5">
      {values.map((value) => (
        <span key={value} className="rounded-full bg-[var(--app-surface-muted)] px-2 py-0.5 text-xs text-foreground">
          {value}
        </span>
      ))}
    </div>
  );
}

function EmptySnapshotMessage({ captureState }: { captureState: ShikihoCaptureState }) {
  let message = '四季報の銘柄ページを開くと、取得した企業情報をここに表示します。';
  if (captureState === 'checking_extension') message = 'Company Shikiho bridge の応答を待っています。';
  if (captureState === 'extension_unavailable') message = 'Company Shikiho bridge 拡張機能を確認してください。';
  if (captureState === 'login_required') message = '四季報オンラインへログイン後、この銘柄ページを開いてください。';
  if (captureState === 'page_changed') message = '現在のページから情報を取得できませんでした。';
  if (captureState === 'storage_error') message = '取得情報を保存できませんでした。';

  return <p className="mt-3 border-t border-border/60 pt-3 text-xs text-muted-foreground">{message}</p>;
}

function PrimaryContent({ snapshot }: { snapshot: ShikihoSnapshotV1 }) {
  const hasPrimaryContent =
    snapshot.features !== null || snapshot.consolidatedBusinesses !== null || snapshot.commentary.length > 0;
  if (!hasPrimaryContent) return null;

  return (
    <div className="min-w-0 space-y-3 lg:border-r lg:border-border/60 lg:pr-3">
      {snapshot.features ? (
        <Section title="特色">
          <p className="text-sm leading-relaxed text-foreground">{snapshot.features}</p>
        </Section>
      ) : null}
      {snapshot.consolidatedBusinesses ? (
        <Section title="連結事業">
          <p className="text-sm leading-relaxed text-foreground">{snapshot.consolidatedBusinesses}</p>
        </Section>
      ) : null}
      {snapshot.commentary.length > 0 ? (
        <Section title="会社四季報">
          <div className="space-y-2">
            {snapshot.commentary.map((item) => (
              <p
                key={`${item.heading ?? 'commentary'}-${item.body}`}
                className="text-sm leading-relaxed text-foreground"
              >
                {item.heading ? <span className="mr-2 font-semibold">【{item.heading}】</span> : null}
                {item.body}
              </p>
            ))}
          </div>
        </Section>
      ) : null}
    </div>
  );
}

function ComparisonCompanies({
  companies,
  onSelectSymbol,
}: {
  companies: ShikihoSnapshotV1['comparisonCompanies'];
  onSelectSymbol: (symbol: string) => void;
}) {
  return (
    <div className="flex flex-wrap gap-1.5">
      {companies.map((company) =>
        company.code && /^\d{4}$/.test(company.code) ? (
          <button
            key={`${company.code}-${company.name}`}
            type="button"
            className="rounded-full border border-border/60 px-2 py-0.5 text-xs text-foreground hover:border-primary/40 hover:text-primary focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
            onClick={() => onSelectSymbol(company.code as string)}
          >
            {company.code} {company.name}
          </button>
        ) : (
          <span
            key={`text-${company.name}`}
            className="rounded-full bg-[var(--app-surface-muted)] px-2 py-0.5 text-xs text-foreground"
          >
            {company.name}
          </span>
        )
      )}
    </div>
  );
}

function SecondaryContent({
  snapshot,
  onSelectSymbol,
}: {
  snapshot: ShikihoSnapshotV1;
  onSelectSymbol: (symbol: string) => void;
}) {
  const scores = scoreLabels.filter(([key]) => snapshot.score[key] !== null);
  const hasSecondaryContent =
    scores.length > 0 ||
    snapshot.industries.length > 0 ||
    snapshot.marketThemes.length > 0 ||
    snapshot.comparisonCompanies.length > 0 ||
    snapshot.profile.length > 0;
  if (!hasSecondaryContent) return null;

  return (
    <div className="min-w-0 space-y-3">
      {scores.length > 0 ? (
        <Section title="四季報スコア">
          <div className="grid grid-cols-2 gap-x-3 gap-y-1 text-xs">
            {scores.map(([key, label]) => (
              <div key={key} className="flex items-center justify-between gap-2">
                <span className="text-muted-foreground">{label}</span>
                <span className="font-semibold tabular-nums text-foreground">{snapshot.score[key]}</span>
              </div>
            ))}
          </div>
        </Section>
      ) : null}
      {snapshot.industries.length > 0 ? (
        <Section title="業種">
          <ChipList values={snapshot.industries} />
        </Section>
      ) : null}
      {snapshot.marketThemes.length > 0 ? (
        <Section title="テーマ">
          <ChipList values={snapshot.marketThemes} />
        </Section>
      ) : null}
      {snapshot.comparisonCompanies.length > 0 ? (
        <Section title="比較会社">
          <ComparisonCompanies companies={snapshot.comparisonCompanies} onSelectSymbol={onSelectSymbol} />
        </Section>
      ) : null}
      {snapshot.profile.length > 0 ? (
        <Section title="会社概要">
          <dl className="grid grid-cols-[auto_minmax(0,1fr)] gap-x-3 gap-y-1 text-xs">
            {snapshot.profile.map((item) => (
              <div key={`${item.label}-${item.value}`} className="contents">
                <dt className="text-muted-foreground">{item.label}</dt>
                <dd className="min-w-0 text-foreground">{item.value}</dd>
              </div>
            ))}
          </dl>
        </Section>
      ) : null}
    </div>
  );
}

function SnapshotBody({
  snapshot,
  onSelectSymbol,
}: {
  snapshot: ShikihoSnapshotV1;
  onSelectSymbol: (symbol: string) => void;
}) {
  return (
    <div className="mt-3 grid gap-3 border-t border-border/60 pt-3 lg:grid-cols-[minmax(0,2fr)_minmax(16rem,1fr)]">
      <PrimaryContent snapshot={snapshot} />
      <SecondaryContent snapshot={snapshot} onSelectSymbol={onSelectSymbol} />
    </div>
  );
}

function hasSnapshotContent(snapshot: ShikihoSnapshotV1): boolean {
  return (
    snapshot.features !== null ||
    snapshot.consolidatedBusinesses !== null ||
    snapshot.commentary.length > 0 ||
    Object.values(snapshot.score).some((score) => score !== null) ||
    snapshot.industries.length > 0 ||
    snapshot.marketThemes.length > 0 ||
    snapshot.comparisonCompanies.length > 0 ||
    snapshot.profile.length > 0
  );
}

function StatusMeta({
  snapshot,
  diagnostic,
}: {
  snapshot: ShikihoSnapshotV1 | null;
  diagnostic: ShikihoCaptureDiagnosticV1 | null;
}) {
  if (snapshot)
    return <span className="text-xs text-muted-foreground">取得 {formatCapturedAt(snapshot.capturedAt)}</span>;
  if (diagnostic)
    return <span className="text-xs text-muted-foreground">確認 {formatCapturedAt(diagnostic.observedAt)}</span>;
  return null;
}

export function ShikihoPanel({ symbol, snapshot, diagnostic, captureState, onSelectSymbol }: ShikihoPanelProps) {
  const [isExpanded, setIsExpanded] = useState(true);
  const sourceUrl = snapshot?.sourceUrl ?? `https://shikiho.toyokeizai.net/stocks/${symbol}`;

  return (
    <section className="mt-3 rounded-xl border border-border/60 px-3 py-2.5" aria-label="Company Shikiho">
      <div className="flex flex-wrap items-center gap-x-2 gap-y-1.5">
        <h3 className="text-sm font-semibold text-foreground">Company Shikiho</h3>
        <span
          className={cn(
            'rounded-full px-2 py-0.5 text-[11px] font-medium',
            captureState === 'captured' && 'bg-emerald-500/10 text-emerald-700 dark:text-emerald-300',
            (captureState === 'partial' || captureState === 'stale') &&
              'bg-amber-500/10 text-amber-700 dark:text-amber-300',
            captureState !== 'captured' &&
              captureState !== 'partial' &&
              captureState !== 'stale' &&
              'bg-[var(--app-surface-muted)] text-muted-foreground'
          )}
        >
          {statusLabels[captureState]}
        </span>
        {snapshot?.editionLabel ? <span className="text-xs text-muted-foreground">{snapshot.editionLabel}</span> : null}
        <StatusMeta snapshot={snapshot} diagnostic={diagnostic} />
        <a
          href={sourceUrl}
          target="_blank"
          rel="noreferrer"
          className="inline-flex items-center gap-1 text-xs font-medium text-primary underline-offset-4 hover:underline"
        >
          四季報で開く
          <ExternalLink className="h-3 w-3" aria-hidden="true" />
        </a>
        {snapshot ? (
          <button
            type="button"
            aria-expanded={isExpanded}
            aria-label={isExpanded ? '会社四季報を折りたたむ' : '会社四季報を展開する'}
            className="ml-auto inline-flex h-7 w-7 items-center justify-center rounded-md text-muted-foreground hover:bg-[var(--app-surface-muted)] hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
            onClick={() => setIsExpanded((expanded) => !expanded)}
          >
            {isExpanded ? (
              <ChevronUp className="h-4 w-4" aria-hidden="true" />
            ) : (
              <ChevronDown className="h-4 w-4" aria-hidden="true" />
            )}
          </button>
        ) : null}
      </div>

      {isExpanded && snapshot === null ? <EmptySnapshotMessage captureState={captureState} /> : null}
      {isExpanded && snapshot !== null && hasSnapshotContent(snapshot) ? (
        <SnapshotBody snapshot={snapshot} onSelectSymbol={onSelectSymbol} />
      ) : null}
    </section>
  );
}
