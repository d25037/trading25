import { ArrowLeft, Loader2, ScrollText, Sparkles } from 'lucide-react';
import { useMemo } from 'react';
import {
  CompactMetric,
  PageIntro,
  PageIntroMetaList,
  SectionEyebrow,
  SectionHeading,
  Surface,
} from '@/components/Layout/Workspace';
import { buildResearchReadingModel, type ResearchReadingModel, type ResearchReadingSection } from '@/utils/researchReading';
import { cn } from '@/lib/utils';
import type {
  ResearchDetailResponse,
  ResearchHighlight,
  ResearchHighlightTone,
  ResearchRunReference,
} from '@/types/research';

function formatTimestamp(value?: string | null): string {
  if (!value) return 'n/a';
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return parsed.toLocaleString();
}

function formatDateRange(start?: string | null, end?: string | null): string {
  if (!start || !end) return 'n/a';
  return `${start} -> ${end}`;
}

function getHighlightTone(tone: ResearchHighlightTone): ResearchHighlightTone {
  switch (tone) {
    case 'accent':
      return 'accent';
    case 'success':
      return 'success';
    case 'warning':
      return 'warning';
    case 'danger':
      return 'danger';
    default:
      return 'neutral';
  }
}

function MarkdownSummary({ markdown }: { markdown: string }) {
  const blocks = useMemo(
    () =>
      markdown
        .split(/\n\s*\n/g)
        .map((block) => block.trim())
        .filter((block) => block.length > 0),
    [markdown]
  );

  if (blocks.length === 0) {
    return <p className="text-sm text-muted-foreground">No markdown summary was stored for this bundle.</p>;
  }

  return (
    <div className="space-y-4">
      {blocks.map((block) => {
        const lines = block.split('\n').map((line) => line.trim()).filter((line) => line.length > 0);
        if (lines.length === 0) {
          return null;
        }
        const firstLine = lines[0] ?? '';
        const blockKey = `block:${block}`;
        if (lines.every((line) => line.startsWith('- '))) {
          return (
            <ul key={blockKey} className="space-y-2 text-sm leading-6 text-foreground">
              {lines.map((line) => (
                <li key={line} className="rounded-2xl border border-border/60 bg-[var(--app-surface-muted)] px-4 py-3">
                  {line.slice(2)}
                </li>
              ))}
            </ul>
          );
        }
        if (firstLine.startsWith('## ')) {
          return (
            <div key={blockKey} className="space-y-2">
              <h3 className="text-sm font-semibold uppercase tracking-[0.14em] text-muted-foreground">
                {firstLine.slice(3)}
              </h3>
              {lines.slice(1).length > 0 ? (
                <p className="text-sm leading-6 text-foreground">{lines.slice(1).join(' ')}</p>
              ) : null}
            </div>
          );
        }
        if (firstLine.startsWith('# ')) {
          return (
            <h2 key={blockKey} className="text-xl font-semibold tracking-tight text-foreground">
              {firstLine.slice(2)}
            </h2>
          );
        }
        return (
          <p key={blockKey} className="text-sm leading-6 text-foreground">
            {lines.join(' ')}
          </p>
        );
      })}
    </div>
  );
}

function DetailMetrics({ highlights }: { highlights: ResearchHighlight[] }) {
  if (highlights.length === 0) {
    return null;
  }

  return (
    <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-1">
      {highlights.slice(0, 3).map((highlight) => (
        <CompactMetric
          key={highlight.label}
          label={highlight.label}
          value={highlight.value}
          detail={highlight.detail ?? undefined}
          tone={getHighlightTone(highlight.tone)}
        />
      ))}
    </div>
  );
}

function ReadingSectionBlock({
  section,
  tone = 'result',
}: {
  section: ResearchReadingSection;
  tone?: 'result' | 'consideration' | 'context';
}) {
  return (
    <div
      className={cn(
        'rounded-[22px] border px-4 py-4',
        tone === 'result'
          ? 'border-white/35 bg-white/60 shadow-sm backdrop-blur-sm dark:border-white/8 dark:bg-white/4'
          : 'border-border/60 bg-[var(--app-surface-muted)]'
      )}
    >
      <p className="text-[11px] font-semibold uppercase tracking-[0.14em] text-muted-foreground">{section.title}</p>
      <div className="mt-3 space-y-3">
        {section.items.map((item) => (
          <p key={`${section.title}:${item}`} className="text-sm leading-6 text-foreground">
            {item}
          </p>
        ))}
      </div>
    </div>
  );
}

function ResearchReadingSections({ reading }: { reading: ResearchReadingModel }) {
  const leadResultSection = reading.resultSections[0] ?? null;
  const supportingResultSections = reading.resultSections.slice(1);

  return (
    <div className="space-y-6">
      <div className="grid gap-6 lg:grid-cols-[minmax(0,1.75fr)_minmax(19rem,0.85fr)]">
        <Surface className="overflow-hidden rounded-[30px] border border-amber-500/20 bg-[radial-gradient(circle_at_top_left,rgba(245,158,11,0.18),transparent_34%),radial-gradient(circle_at_bottom_right,rgba(14,165,233,0.14),transparent_36%)] px-5 py-5 sm:px-6 sm:py-6">
          <div className="flex flex-wrap items-center gap-3">
            <span className="inline-flex items-center rounded-full border border-amber-500/20 bg-amber-500/10 px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.16em] text-amber-700 dark:text-amber-300">
              Results
            </span>
            <span className="text-[11px] font-medium uppercase tracking-[0.14em] text-muted-foreground">
              Primary readout
            </span>
          </div>

          <div className="mt-5 space-y-5">
            <div className="space-y-4">
              <div className="space-y-3">
                <SectionEyebrow>What It Found</SectionEyebrow>
                <h2 className="max-w-4xl text-3xl font-semibold leading-tight tracking-tight text-foreground sm:text-[2.2rem]">
                  {reading.headline}
                </h2>
              </div>

              {leadResultSection ? (
                <div className="grid gap-3">
                  <ReadingSectionBlock section={leadResultSection} tone="result" />
                </div>
              ) : null}

              {reading.highlights.length > 0 ? (
                <div className="grid gap-3 md:grid-cols-2">
                  <DetailMetrics highlights={reading.highlights} />
                </div>
              ) : null}

              {supportingResultSections.length > 0 ? (
                <div className="grid gap-3 md:grid-cols-2">
                  {supportingResultSections.map((section) => (
                    <ReadingSectionBlock key={section.title} section={section} tone="result" />
                  ))}
                </div>
              ) : null}
            </div>
          </div>
        </Surface>

        <Surface className="rounded-[30px] border border-sky-500/15 bg-[radial-gradient(circle_at_top,rgba(14,165,233,0.12),transparent_34%)] px-5 py-5 sm:px-6 sm:py-6">
          <div className="space-y-4">
            <div className="space-y-2">
              <span className="inline-flex items-center rounded-full border border-sky-500/20 bg-sky-500/10 px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.16em] text-sky-700 dark:text-sky-300">
                Considerations
              </span>
              <h3 className="text-2xl font-semibold tracking-tight text-foreground">How To Read It</h3>
              <p className="text-sm leading-6 text-muted-foreground">
                These notes are the interpretation guardrails. They matter more than the raw markdown and bundle tables.
              </p>
            </div>

            <div className="space-y-3">
              {reading.considerationSections.map((section) => (
                <ReadingSectionBlock key={section.title} section={section} tone="consideration" />
              ))}
            </div>
          </div>
        </Surface>
      </div>

      {reading.contextSections.length > 0 || reading.parameters.length > 0 ? (
        <Surface className="rounded-[26px] px-5 py-5 sm:px-6">
          <SectionHeading
            eyebrow="Support"
            title="Supporting Context"
            description="Purpose, method, setup, and configuration that explain how to interpret the readout above."
          />

          <div className="mt-5 grid gap-4 lg:grid-cols-2">
            {reading.contextSections.map((section) => (
              <ReadingSectionBlock key={section.title} section={section} tone="context" />
            ))}

            {reading.parameters.length > 0 ? (
              <div className="rounded-[22px] border border-border/60 bg-[var(--app-surface-muted)] px-4 py-4">
                <p className="text-[11px] font-semibold uppercase tracking-[0.14em] text-muted-foreground">Key Settings</p>
                <dl className="mt-3 space-y-3">
                  {reading.parameters.map((item) => (
                    <div key={item.label}>
                      <dt className="text-[11px] font-semibold uppercase tracking-[0.14em] text-muted-foreground">
                        {item.label}
                      </dt>
                      <dd className="mt-1 text-sm leading-6 text-foreground">{item.value}</dd>
                    </div>
                  ))}
                </dl>
              </div>
            ) : null}
          </div>
        </Surface>
      ) : null}
    </div>
  );
}

function RunSelector({
  runs,
  activeRunId,
  onSelect,
}: {
  runs: ResearchRunReference[];
  activeRunId: string;
  onSelect: (runId: string) => void;
}) {
  if (runs.length === 0) {
    return null;
  }

  return (
    <Surface className="rounded-[24px] px-5 py-5 sm:px-6">
      <SectionHeading eyebrow="Runs" title="Bundle History" />
      <div className="mt-4 flex flex-wrap gap-3">
        {runs.map((run) => {
          const isActive = run.runId === activeRunId;
          return (
            <button
              key={run.runId}
              type="button"
              onClick={() => onSelect(run.runId)}
              className={cn(
                'app-interactive min-w-[13rem] rounded-2xl border px-4 py-3 text-left',
                isActive
                  ? 'border-border/70 bg-[var(--app-surface-emphasis)] text-foreground shadow-sm'
                  : 'border-border/60 bg-[var(--app-surface-muted)] text-muted-foreground hover:text-foreground'
              )}
            >
              <p className="font-mono text-xs">{run.runId}</p>
              <p className="mt-2 text-sm font-medium">{formatTimestamp(run.createdAt)}</p>
              <p className="mt-1 text-[11px] uppercase tracking-[0.14em]">{run.isLatest ? 'Latest' : 'Previous'}</p>
            </button>
          );
        })}
      </div>
    </Surface>
  );
}

interface ResearchDetailViewProps {
  detail: ResearchDetailResponse;
  onBack: () => void;
  onSelectRun: (runId: string) => void;
}

export function ResearchDetailView({ detail, onBack, onSelectRun }: ResearchDetailViewProps) {
  const reading = useMemo(() => buildResearchReadingModel(detail), [detail]);
  const activeRunId = detail.item.runId;
  const metaItems = [
    { label: 'Analysis Range', value: formatDateRange(detail.item.analysisStartDate, detail.item.analysisEndDate) },
    { label: 'Created', value: formatTimestamp(detail.item.createdAt) },
    { label: 'Run', value: detail.item.runId },
    { label: 'Git', value: detail.item.gitCommit ?? 'n/a' },
  ];

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between gap-3">
        <button
          type="button"
          onClick={onBack}
          className="app-interactive inline-flex items-center gap-2 rounded-full border border-border/70 bg-[var(--app-surface-muted)] px-4 py-2 text-sm font-medium text-muted-foreground hover:text-foreground"
        >
          <ArrowLeft className="h-4 w-4" />
          Back to catalog
        </button>
      </div>

      <PageIntro
        eyebrow="Published Research"
        title={detail.item.title}
        description={
          detail.item.headline ??
          detail.item.objective ??
          'Read the latest published analytics bundle with the result and interpretation surfaced first.'
        }
        meta={<PageIntroMetaList items={metaItems} />}
      />

      {detail.availableRuns.length > 1 ? (
        <RunSelector runs={detail.availableRuns} activeRunId={activeRunId} onSelect={onSelectRun} />
      ) : null}

      <ResearchReadingSections reading={reading} />

      <div className="space-y-6">
        <Surface className="rounded-[24px] px-5 py-5 sm:px-6">
          <div className="flex items-center justify-between gap-3">
            <div>
              <SectionEyebrow>Appendix</SectionEyebrow>
              <h2 className="mt-2 text-xl font-semibold tracking-tight text-foreground">Raw Bundle Markdown</h2>
            </div>
            <div className="flex items-center gap-2 text-xs uppercase tracking-[0.14em] text-muted-foreground">
              <ScrollText className="h-4 w-4" />
              summary.md
            </div>
          </div>
          <div className="mt-5">
            <MarkdownSummary markdown={detail.summaryMarkdown} />
          </div>
        </Surface>

        <Surface className="rounded-[24px] px-5 py-5 sm:px-6">
          <SectionHeading
            eyebrow="Appendix"
            title="Stored Artifacts"
            description="Bundle tables stay here as supporting material so they do not compete with the result and consideration panels."
          />

          {reading.tableHighlights.length > 0 ? (
            <div className="mt-4 grid gap-3 lg:grid-cols-2">
              {reading.tableHighlights.map((item) => (
                <div
                  key={item.name}
                  className="rounded-[20px] border border-border/60 bg-[var(--app-surface-muted)] px-4 py-3"
                >
                  <p className="text-sm font-semibold text-foreground">{item.label}</p>
                  <p className="mt-1 font-mono text-xs text-muted-foreground">{item.name}</p>
                  {item.description ? <p className="mt-2 text-sm leading-6 text-muted-foreground">{item.description}</p> : null}
                </div>
              ))}
            </div>
          ) : (
            <div className="mt-4 flex flex-wrap gap-2">
              {detail.outputTables.map((tableName) => (
                <span
                  key={tableName}
                  className="inline-flex items-center rounded-full border border-border/70 bg-[var(--app-surface-muted)] px-3 py-1.5 font-mono text-xs text-foreground"
                >
                  {tableName}
                </span>
              ))}
            </div>
          )}
        </Surface>
      </div>
    </div>
  );
}

export function ResearchDetailLoadingState() {
  return (
    <Surface className="flex min-h-[24rem] items-center justify-center rounded-[26px]">
      <div className="flex items-center gap-3 text-sm text-muted-foreground">
        <Loader2 className="h-4 w-4 animate-spin" />
        Loading published bundle...
      </div>
    </Surface>
  );
}

export function ResearchDetailErrorState({ message }: { message: string }) {
  return (
    <Surface className="rounded-[26px] px-5 py-5">
      <p className="text-sm font-semibold text-foreground">Research detail load failed</p>
      <p className="mt-2 text-sm leading-6 text-muted-foreground">{message}</p>
    </Surface>
  );
}

export function ResearchDetailEmptyState({ onBack }: { onBack: () => void }) {
  return (
    <Surface className="rounded-[26px] px-5 py-8 text-center">
      <div className="mx-auto flex max-w-xl flex-col items-center gap-3">
        <div className="flex h-12 w-12 items-center justify-center rounded-2xl border border-border/70 bg-[var(--app-surface-muted)]">
          <Sparkles className="h-5 w-5 text-primary" />
        </div>
        <h2 className="text-xl font-semibold tracking-tight text-foreground">No research bundle selected</h2>
        <p className="text-sm leading-6 text-muted-foreground">
          Open a published bundle from the research catalog first.
        </p>
        <button
          type="button"
          onClick={onBack}
          className="app-interactive mt-2 inline-flex items-center gap-2 rounded-full border border-border/70 bg-[var(--app-surface-muted)] px-4 py-2 text-sm font-medium text-muted-foreground hover:text-foreground"
        >
          <ArrowLeft className="h-4 w-4" />
          Back to catalog
        </button>
      </div>
    </Surface>
  );
}
