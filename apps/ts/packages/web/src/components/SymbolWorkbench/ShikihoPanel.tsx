import {
  normalizeShikihoCode,
  type ShikihoCaptureDiagnosticV1,
  type ShikihoCaptureTraceV1,
  type ShikihoSnapshotV1,
} from '@trading25/shikiho-extension/contract';
import { CalendarDays, ChevronDown, ChevronUp, ExternalLink, RefreshCw } from 'lucide-react';
import { useId, useState } from 'react';
import type { ShikihoCaptureState } from '@/hooks/useShikihoSnapshot';
import type { ShikihoDailyOverlayProvenance } from '@/lib/shikihoDailyOverlay';
import { cn } from '@/lib/utils';
import { ShikihoCaptureDiagnosticsDetails, ShikihoCaptureDiagnosticsTrigger } from './ShikihoCaptureDiagnostics';
import { ShikihoScoreCard } from './ShikihoScoreCard';
import { getShikihoEarningsDateState, type ShikihoEarningsDateState } from './shikihoEarningsDate';

interface ShikihoPanelProps {
  symbol: string;
  canonicalSnapshot?: ShikihoSnapshotV1 | null;
  snapshot: ShikihoSnapshotV1 | null;
  candidate?: ShikihoSnapshotV1 | null;
  trace?: ShikihoCaptureTraceV1 | null;
  diagnostic: ShikihoCaptureDiagnosticV1 | null;
  captureState: ShikihoCaptureState;
  isRefreshing: boolean;
  onRefresh: () => void;
  onSelectSymbol: (symbol: string) => void;
  provisionalProvenance?: ShikihoDailyOverlayProvenance | null;
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

function formatCapturedAt(capturedAt: string): string {
  return new Date(capturedAt).toLocaleString('ja-JP', {
    dateStyle: 'short',
    timeStyle: 'short',
  });
}

function formatQuoteTime(observedAt: string): string {
  return new Date(observedAt).toLocaleTimeString('ja-JP', {
    timeZone: 'Asia/Tokyo',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  });
}

function formatQuotePrice(value: number): string {
  return `￥${value.toLocaleString('ja-JP')}`;
}

function QuoteDetails({ snapshot, isActiveOverlay }: { snapshot: ShikihoSnapshotV1; isActiveOverlay: boolean }) {
  const quote = snapshot.quote;
  if (!quote) return null;
  const details = [
    ['現在値', formatQuotePrice(quote.currentPrice)],
    ['始値', formatQuotePrice(quote.open)],
    ['高値', formatQuotePrice(quote.high)],
    ['安値', formatQuotePrice(quote.low)],
    ['前日終値', formatQuotePrice(quote.previousClose)],
    ['出来高', quote.volume === null ? '-' : quote.volume.toLocaleString('ja-JP')],
  ];
  return (
    <div data-testid="shikiho-quote" className="mt-2 border-t border-border/50 pt-2">
      <div className="flex flex-wrap items-center gap-x-2 gap-y-0.5 text-[10px] text-muted-foreground">
        <span
          role="note"
          aria-label={isActiveOverlay ? '四季報の当日暫定値' : '四季報の取得株価'}
          className="font-medium text-amber-700 dark:text-amber-300"
        >
          四季報 15分遅延{isActiveOverlay ? '・当日暫定' : ''}
        </span>
        <time dateTime={quote.observedAt}>{formatQuoteTime(quote.observedAt)}</time>
      </div>
      <dl className="mt-1 flex flex-wrap gap-x-3 gap-y-0.5 text-[11px] tabular-nums">
        {details.map(([label, value]) => (
          <div key={label} className="flex gap-1">
            <dt className="text-muted-foreground">{label}</dt>
            <dd className="font-medium text-foreground">{value}</dd>
          </div>
        ))}
      </dl>
    </div>
  );
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

function hasPrimaryContent(snapshot: ShikihoSnapshotV1): boolean {
  return snapshot.features !== null || snapshot.consolidatedBusinesses !== null || snapshot.commentary.length > 0;
}

function hasSecondaryContent(snapshot: ShikihoSnapshotV1): boolean {
  return (
    snapshot.industries.length > 0 ||
    snapshot.marketThemes.length > 0 ||
    snapshot.comparisonCompanies.length > 0 ||
    snapshot.profile.length > 0
  );
}

function PrimaryContent({ snapshot, divided }: { snapshot: ShikihoSnapshotV1; divided: boolean }) {
  if (!hasPrimaryContent(snapshot)) return null;

  return (
    <div
      data-testid="shikiho-primary"
      className={cn('min-w-0 space-y-3', divided && 'lg:border-r lg:border-border/60 lg:pr-3')}
    >
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
        <div className="space-y-2">
          {snapshot.commentary.map((item) => (
            <p key={`${item.heading ?? 'commentary'}-${item.body}`} className="text-sm leading-relaxed text-foreground">
              {item.heading ? <span className="mr-2 font-semibold">【{item.heading}】</span> : null}
              {item.body}
            </p>
          ))}
        </div>
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
  if (!hasSecondaryContent(snapshot)) return null;

  return (
    <div data-testid="shikiho-secondary" className="min-w-0 space-y-3">
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
  bodyId,
  hidden,
  snapshot,
  onSelectSymbol,
}: {
  bodyId: string;
  hidden: boolean;
  snapshot: ShikihoSnapshotV1;
  onSelectSymbol: (symbol: string) => void;
}) {
  const hasScore = Object.values(snapshot.score).some((score) => score !== null);
  const hasPrimary = hasPrimaryContent(snapshot);
  const hasAside = hasScore || hasSecondaryContent(snapshot);
  const twoColumn = hasPrimary && hasAside;
  return (
    <div
      id={bodyId}
      data-testid="shikiho-body"
      hidden={hidden}
      className={cn(
        'mt-3 grid min-w-0 gap-3 [overflow-wrap:anywhere] border-t border-border/60 pt-3',
        twoColumn && 'lg:grid-cols-[minmax(0,2fr)_minmax(16rem,1fr)]'
      )}
    >
      <PrimaryContent snapshot={snapshot} divided={twoColumn} />
      {hasAside ? (
        <aside data-testid="shikiho-aside" className="min-w-0 space-y-3">
          {hasScore ? <ShikihoScoreCard score={snapshot.score} /> : null}
          <SecondaryContent snapshot={snapshot} onSelectSymbol={onSelectSymbol} />
        </aside>
      ) : null}
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
    return (
      <span className="min-w-0 truncate text-xs text-muted-foreground">
        取得 {formatCapturedAt(snapshot.capturedAt)}
      </span>
    );
  if (diagnostic)
    return (
      <span className="min-w-0 truncate text-xs text-muted-foreground">
        確認 {formatCapturedAt(diagnostic.observedAt)}
      </span>
    );
  return null;
}

function EditionMeta({ snapshot }: { snapshot: ShikihoSnapshotV1 | null }) {
  if (!snapshot) return null;
  const text =
    snapshot.editionLabel ?? (snapshot.pageUpdatedAt ? `更新 ${formatCapturedAt(snapshot.pageUpdatedAt)}` : null);
  if (!text) return null;
  return (
    <span data-testid="shikiho-edition-meta" className="min-w-0 truncate text-xs text-muted-foreground">
      {text}
    </span>
  );
}

const progressiveFieldNames = new Set([
  'features',
  'consolidatedBusinesses',
  'commentary',
  'score',
  'comparisonCompanies',
  'industries',
  'marketThemes',
  'profile',
  'editionLabel',
  'earningsAnnouncementDate',
  'pageUpdatedAt',
]);

function countCandidateFields(
  candidate: ShikihoSnapshotV1 | null | undefined,
  trace: ShikihoCaptureTraceV1 | null | undefined
): number {
  if (!candidate) return 0;
  if (trace) return trace.dom.presentFields.filter((field) => progressiveFieldNames.has(field)).length;
  return [
    candidate.features,
    candidate.consolidatedBusinesses,
    candidate.commentary.length > 0 ? candidate.commentary : null,
    Object.values(candidate.score).some((score) => score !== null) ? candidate.score : null,
    candidate.comparisonCompanies.length > 0 ? candidate.comparisonCompanies : null,
    candidate.industries.length > 0 ? candidate.industries : null,
    candidate.marketThemes.length > 0 ? candidate.marketThemes : null,
    candidate.profile.length > 0 ? candidate.profile : null,
    candidate.editionLabel,
    candidate.earningsAnnouncementDate,
    candidate.pageUpdatedAt,
  ].filter((value) => value !== null).length;
}

const earningsDateStateClasses: Record<ShikihoEarningsDateState, string> = {
  neutral: 'bg-[var(--app-surface-muted)] text-muted-foreground',
  yellow: 'bg-yellow-500/15 text-yellow-800 dark:text-yellow-200',
  orange: 'bg-orange-500/15 text-orange-800 dark:text-orange-200',
  red: 'bg-red-500/15 text-red-800 dark:text-red-200',
  past: 'bg-muted text-muted-foreground',
};

function EarningsAnnouncementBadge({ date }: { date: string | null }) {
  if (!date) return null;
  const presentation = getShikihoEarningsDateState(date);
  const [year, month, day] = date.split('-');
  const formattedDate = `${year}/${month}/${day}`;
  const accessibleDate = `${Number(year)}年${Number(month)}月${Number(day)}日`;
  return (
    <span
      role="note"
      aria-label={`決算発表予定日 ${accessibleDate} ${presentation.remainingDayText}`}
      className={cn(
        'inline-flex shrink-0 items-center gap-1 whitespace-nowrap rounded-full px-2.5 py-1 text-sm font-medium',
        earningsDateStateClasses[presentation.state]
      )}
    >
      <CalendarDays className="h-3 w-3" aria-hidden="true" />
      決算発表予定日 <span className="font-bold tabular-nums">{formattedDate}</span> · {presentation.remainingDayText}
    </span>
  );
}

function StatusBadge({
  captureState,
  isRefreshing,
  candidate,
  trace,
}: {
  captureState: ShikihoCaptureState;
  isRefreshing: boolean;
  candidate: ShikihoSnapshotV1 | null | undefined;
  trace: ShikihoCaptureTraceV1 | null | undefined;
}) {
  const candidateFields = countCandidateFields(candidate, trace);
  const label = isRefreshing
    ? candidateFields > 0
      ? `更新中（新規 ${candidateFields}項目）`
      : '取得中'
    : statusLabels[captureState];
  return (
    <span
      role="status"
      aria-live="polite"
      className={cn(
        'shrink-0 rounded-full px-2 py-0.5 text-[11px] font-medium',
        captureState === 'captured' && 'bg-emerald-500/10 text-emerald-700 dark:text-emerald-300',
        (captureState === 'partial' || captureState === 'stale') &&
          'bg-amber-500/10 text-amber-700 dark:text-amber-300',
        captureState !== 'captured' &&
          captureState !== 'partial' &&
          captureState !== 'stale' &&
          'bg-[var(--app-surface-muted)] text-muted-foreground'
      )}
    >
      {label}
    </span>
  );
}

function RefreshButton({ isRefreshing, onRefresh }: { isRefreshing: boolean; onRefresh: () => void }) {
  return (
    <button
      type="button"
      aria-label="会社四季報を更新"
      disabled={isRefreshing}
      className="inline-flex h-7 items-center gap-1 rounded-md px-1.5 text-xs font-medium text-muted-foreground hover:bg-[var(--app-surface-muted)] hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:cursor-not-allowed disabled:opacity-50"
      onClick={onRefresh}
    >
      <RefreshCw className={cn('h-3 w-3', isRefreshing && 'animate-spin')} aria-hidden="true" />
      更新
    </button>
  );
}

function SourceLink({ sourceUrl }: { sourceUrl: string | null }) {
  if (!sourceUrl) return null;
  return (
    <a
      href={sourceUrl}
      target="_blank"
      rel="noreferrer"
      className="inline-flex items-center gap-1 text-xs font-medium text-primary underline-offset-4 hover:underline"
    >
      四季報で開く
      <ExternalLink className="h-3 w-3" aria-hidden="true" />
    </a>
  );
}

function CollapseButton({
  bodyId,
  hasContent,
  isExpanded,
  onToggle,
}: {
  bodyId: string;
  hasContent: boolean;
  isExpanded: boolean;
  onToggle: () => void;
}) {
  if (!hasContent) return null;
  return (
    <button
      type="button"
      aria-expanded={isExpanded}
      aria-controls={bodyId}
      aria-label={isExpanded ? '会社四季報を折りたたむ' : '会社四季報を展開する'}
      className="inline-flex h-7 w-7 items-center justify-center rounded-md text-muted-foreground hover:bg-[var(--app-surface-muted)] hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
      onClick={onToggle}
    >
      {isExpanded ? (
        <ChevronUp className="h-4 w-4" aria-hidden="true" />
      ) : (
        <ChevronDown className="h-4 w-4" aria-hidden="true" />
      )}
    </button>
  );
}

function ShikihoPanelForSymbol({
  symbol,
  canonicalSnapshot: canonicalSnapshotProp,
  snapshot,
  candidate = null,
  trace = null,
  diagnostic,
  captureState,
  isRefreshing,
  onRefresh,
  onSelectSymbol,
  provisionalProvenance = null,
}: ShikihoPanelProps) {
  const [isExpanded, setIsExpanded] = useState(true);
  const [isDiagnosticsExpanded, setIsDiagnosticsExpanded] = useState(false);
  const bodyId = useId();
  const diagnosticsId = useId();
  const fallbackCode = normalizeShikihoCode(symbol);
  const sourceUrl =
    snapshot?.sourceUrl ?? (fallbackCode ? `https://shikiho.toyokeizai.net/stocks/${fallbackCode}` : null);
  const hasContent = snapshot !== null && hasSnapshotContent(snapshot);
  const canonicalSnapshot = canonicalSnapshotProp === undefined ? snapshot : canonicalSnapshotProp;

  return (
    <section className="mt-3 min-w-0 rounded-xl border border-border/60 px-3 py-2.5" aria-label="会社四季報">
      <div data-testid="shikiho-header-primary" className="flex min-w-0 items-center justify-between gap-3">
        <div className="flex min-w-0 items-center gap-2">
          <h3 className="shrink-0 text-sm font-semibold text-foreground">会社四季報</h3>
          <StatusBadge captureState={captureState} isRefreshing={isRefreshing} candidate={candidate} trace={trace} />
          <EarningsAnnouncementBadge date={snapshot?.earningsAnnouncementDate ?? null} />
        </div>
        <div className="flex shrink-0 items-center gap-1 whitespace-nowrap">
          <SourceLink sourceUrl={sourceUrl} />
          <RefreshButton isRefreshing={isRefreshing} onRefresh={onRefresh} />
          <CollapseButton
            bodyId={bodyId}
            hasContent={hasContent}
            isExpanded={isExpanded}
            onToggle={() => setIsExpanded((expanded) => !expanded)}
          />
        </div>
      </div>
      <div data-testid="shikiho-header-meta" className="mt-1 flex min-w-0 flex-wrap items-center gap-x-3 gap-y-1">
        <EditionMeta snapshot={canonicalSnapshot} />
        <StatusMeta snapshot={canonicalSnapshot} diagnostic={diagnostic} />
        {trace ? (
          <ShikihoCaptureDiagnosticsTrigger
            trace={trace}
            detailsId={diagnosticsId}
            isExpanded={isDiagnosticsExpanded}
            onToggle={() => setIsDiagnosticsExpanded((expanded) => !expanded)}
          />
        ) : null}
      </div>

      {trace ? (
        <ShikihoCaptureDiagnosticsDetails
          trace={trace}
          detailsId={diagnosticsId}
          isExpanded={isDiagnosticsExpanded}
          className="mt-2"
        />
      ) : null}

      {snapshot ? <QuoteDetails snapshot={snapshot} isActiveOverlay={provisionalProvenance !== null} /> : null}

      {snapshot === null ? <EmptySnapshotMessage captureState={captureState} /> : null}
      {hasContent ? (
        <SnapshotBody bodyId={bodyId} hidden={!isExpanded} snapshot={snapshot} onSelectSymbol={onSelectSymbol} />
      ) : null}
    </section>
  );
}

export function ShikihoPanel(props: ShikihoPanelProps) {
  return <ShikihoPanelForSymbol key={props.symbol} {...props} />;
}
