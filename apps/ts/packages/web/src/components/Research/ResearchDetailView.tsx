import { AlertTriangle, ArrowLeft, Loader2, ScrollText, Sparkles } from 'lucide-react';
import { type ReactNode, useMemo } from 'react';
import {
  CompactMetric,
  PageIntro,
  PageIntroMetaList,
  SectionEyebrow,
  SectionHeading,
  Surface,
} from '@/components/Layout/Workspace';
import { cn } from '@/lib/utils';
import type {
  ResearchDetailResponse,
  ResearchHighlight,
  ResearchHighlightTone,
  ResearchRunReference,
} from '@/types/research';
import {
  buildResearchReadingModel,
  type ResearchReadingModel,
  type ResearchReadingSection,
} from '@/utils/researchReading';

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

interface MarkdownTable {
  headers: MarkdownTableHeader[];
  rows: MarkdownTableRow[];
}

interface MarkdownTableHeader {
  id: string;
  label: string;
}

interface MarkdownTableRow {
  id: string;
  cells: MarkdownTableCell[];
}

interface MarkdownTableCell {
  id: string;
  value: string;
}

function splitMarkdownTableRow(line: string): string[] {
  const trimmed = line.trim();
  if (!trimmed.includes('|')) {
    return [];
  }

  const withoutLeadingPipe = trimmed.startsWith('|') ? trimmed.slice(1) : trimmed;
  const withoutTrailingPipe = withoutLeadingPipe.endsWith('|') ? withoutLeadingPipe.slice(0, -1) : withoutLeadingPipe;
  return withoutTrailingPipe.split('|').map((cell) => cell.trim());
}

function isMarkdownTableSeparator(line: string): boolean {
  const cells = splitMarkdownTableRow(line);
  return cells.length > 0 && cells.every((cell) => /^:?-{3,}:?$/.test(cell));
}

function isPotentialMarkdownTableLine(line: string): boolean {
  return line.trim().startsWith('|') && splitMarkdownTableRow(line).length > 1;
}

function parseMarkdownTable(lines: string[]): MarkdownTable | null {
  if (lines.length < 2 || !isMarkdownTableSeparator(lines[1] ?? '')) {
    return null;
  }

  const headerLabels = splitMarkdownTableRow(lines[0] ?? '');
  if (headerLabels.length === 0) {
    return null;
  }

  const headers = headerLabels.map((label, index) => ({
    id: `header:${index}:${label}`,
    label,
  }));
  const rows = lines.slice(2).map((line, rowIndex) => {
    const cells = splitMarkdownTableRow(line);
    return {
      id: `row:${rowIndex}:${line}`,
      cells: headers.map((header, cellIndex) => ({
        id: `cell:${rowIndex}:${header.id}`,
        value: cells[cellIndex] ?? '',
      })),
    };
  });

  return { headers, rows };
}

function isNumericMarkdownCell(value: string): boolean {
  return /^[-+]?(\d+|\d*\.\d+)(e[-+]?\d+)?%?$/i.test(value.replace(/[` ,]/g, '').trim());
}

function InlineMarkdownText({ value }: { value: string }) {
  const segments = value.split(/(`[^`]+`)/g).filter((segment) => segment.length > 0);
  const seen = new Map<string, number>();
  return (
    <>
      {segments.map((segment) => {
        const occurrence = seen.get(segment) ?? 0;
        seen.set(segment, occurrence + 1);
        const key = `${segment}:${occurrence}`;
        if (segment.startsWith('`') && segment.endsWith('`')) {
          return (
            <code key={key} className="rounded bg-[var(--app-surface-emphasis)] px-1 py-0.5 font-mono text-[0.92em]">
              {segment.slice(1, -1)}
            </code>
          );
        }
        return <span key={key}>{segment}</span>;
      })}
    </>
  );
}

function MarkdownTableBlock({ table }: { table: MarkdownTable }) {
  return (
    <div className="overflow-hidden rounded-[18px] border border-border/60 bg-[var(--app-surface-muted)]">
      <div className="overflow-x-auto">
        <table aria-label="Markdown table" className="min-w-full border-collapse text-left text-xs">
          <thead className="bg-[var(--app-surface-emphasis)] text-[11px] uppercase tracking-[0.12em] text-muted-foreground">
            <tr>
              {table.headers.map((header) => (
                <th
                  key={header.id}
                  scope="col"
                  className={cn(
                    'border-b border-border/60 px-3 py-2.5 font-semibold whitespace-nowrap',
                    isNumericMarkdownCell(header.label) ? 'text-right' : 'text-left'
                  )}
                >
                  <InlineMarkdownText value={header.label} />
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-border/50">
            {table.rows.map((row) => (
              <tr key={row.id} className="bg-background/40">
                {row.cells.map((cell) => {
                  const isNumeric = isNumericMarkdownCell(cell.value);
                  return (
                    <td
                      key={cell.id}
                      className={cn(
                        'px-3 py-2.5 whitespace-nowrap text-foreground',
                        isNumeric ? 'text-right font-mono tabular-nums' : 'text-left'
                      )}
                    >
                      <InlineMarkdownText value={cell.value} />
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function buildContentKey(prefix: string, value: string, seen: Map<string, number>): string {
  const count = seen.get(value) ?? 0;
  seen.set(value, count + 1);
  return `${prefix}:${value}:${count}`;
}

function parseMarkdownHeading(value: string): { depth: number; text: string } | null {
  const match = /^(#{1,6})\s+(.+)$/.exec(value.trim());
  if (!match) {
    return null;
  }
  const marker = match[1] ?? '';
  const text = match[2] ?? '';
  return { depth: marker.length, text: text.trim() };
}

function getMarkdownHeadingClassName(depth: number): string {
  switch (depth) {
    case 1:
      return 'text-xl font-semibold tracking-tight text-foreground';
    case 2:
      return 'text-sm font-semibold uppercase tracking-[0.14em] text-muted-foreground';
    case 3:
      return 'text-base font-semibold leading-6 text-foreground';
    default:
      return 'text-sm font-semibold leading-6 text-foreground';
  }
}

function MarkdownHeadingBlock({ depth, text }: { depth: number; text: string }) {
  const className = getMarkdownHeadingClassName(depth);
  if (depth === 1) {
    return (
      <h2 className={className}>
        <InlineMarkdownText value={text} />
      </h2>
    );
  }
  if (depth === 2) {
    return (
      <h3 className={className}>
        <InlineMarkdownText value={text} />
      </h3>
    );
  }
  return (
    <h4 className={className}>
      <InlineMarkdownText value={text} />
    </h4>
  );
}

function renderMarkdownItemBlocks(items: string[], keyPrefix: string): ReactNode[] {
  const blocks: ReactNode[] = [];
  const seen = new Map<string, number>();
  let tableLines: string[] = [];

  const appendParagraph = (item: string) => {
    blocks.push(
      <p key={buildContentKey(`${keyPrefix}:paragraph`, item, seen)} className="text-sm leading-6 text-foreground">
        <InlineMarkdownText value={item} />
      </p>
    );
  };

  const flushTable = () => {
    if (tableLines.length === 0) {
      return;
    }
    const table = parseMarkdownTable(tableLines);
    if (table) {
      blocks.push(
        <MarkdownTableBlock key={buildContentKey(`${keyPrefix}:table`, tableLines.join('\n'), seen)} table={table} />
      );
    } else {
      tableLines.forEach(appendParagraph);
    }
    tableLines = [];
  };

  for (const item of items) {
    if (isPotentialMarkdownTableLine(item)) {
      tableLines.push(item);
      continue;
    }
    flushTable();
    const heading = parseMarkdownHeading(item);
    if (heading) {
      blocks.push(
        <MarkdownHeadingBlock
          key={buildContentKey(`${keyPrefix}:heading`, item, seen)}
          depth={heading.depth}
          text={heading.text}
        />
      );
      continue;
    }
    appendParagraph(item);
  }

  flushTable();
  return blocks;
}

function getDetailDescription(reading: ResearchReadingModel): string {
  return reading.headline;
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
        const lines = block
          .split('\n')
          .map((line) => line.trim())
          .filter((line) => line.length > 0);
        if (lines.length === 0) {
          return null;
        }
        const firstLine = lines[0] ?? '';
        const blockKey = `block:${block}`;
        const table = parseMarkdownTable(lines);
        if (table) {
          return <MarkdownTableBlock key={blockKey} table={table} />;
        }
        if (lines.every((line) => line.startsWith('- '))) {
          return (
            <ul key={blockKey} className="space-y-2 text-sm leading-6 text-foreground">
              {lines.map((line) => (
                <li key={line} className="rounded-2xl border border-border/60 bg-[var(--app-surface-muted)] px-4 py-3">
                  <InlineMarkdownText value={line.slice(2)} />
                </li>
              ))}
            </ul>
          );
        }
        const heading = parseMarkdownHeading(firstLine);
        if (heading) {
          return (
            <div key={blockKey} className="space-y-2">
              <MarkdownHeadingBlock depth={heading.depth} text={heading.text} />
              {lines.slice(1).length > 0 ? (
                <p className="text-sm leading-6 text-foreground">
                  <InlineMarkdownText value={lines.slice(1).join(' ')} />
                </p>
              ) : null}
            </div>
          );
        }
        return (
          <p key={blockKey} className="text-sm leading-6 text-foreground">
            <InlineMarkdownText value={lines.join(' ')} />
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
  showTitle = true,
}: {
  section: ResearchReadingSection;
  tone?: 'result' | 'consideration' | 'context';
  showTitle?: boolean;
}) {
  return (
    <div
      className={cn(
        'rounded-[18px] border px-4 py-3.5',
        tone === 'result'
          ? 'border-primary/15 bg-primary/[0.035]'
          : tone === 'consideration'
            ? 'border-amber-500/20 bg-amber-500/[0.045]'
            : 'border-border/60 bg-[var(--app-surface-muted)]'
      )}
    >
      {showTitle ? (
        <p className="text-[11px] font-semibold uppercase tracking-[0.14em] text-muted-foreground">{section.title}</p>
      ) : null}
      <div className={cn('space-y-2.5', showTitle ? 'mt-2.5' : null)}>
        {renderMarkdownItemBlocks(section.items, `section:${section.title}`)}
      </div>
    </div>
  );
}

function MissingPublishedReadout({ reading }: { reading: ResearchReadingModel }) {
  return (
    <Surface className="rounded-[24px] border border-amber-500/25 bg-amber-500/[0.045] px-5 py-5 sm:px-6">
      <div className="flex gap-3">
        <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0 text-amber-600 dark:text-amber-300" />
        <div>
          <SectionHeading
            eyebrow="Published Readout"
            title="Needs Published Readout"
            description="This research is not promoted into the main reader until the source README or summary.json publishes the full readout contract."
          />
          <div className="mt-4 rounded-[18px] border border-amber-500/25 bg-background/45 px-4 py-3">
            <p className="text-[11px] font-semibold uppercase tracking-[0.14em] text-muted-foreground">
              Missing Sections
            </p>
            <div className="mt-3 flex flex-wrap gap-2">
              {reading.missingSections.map((section) => (
                <span
                  key={section}
                  className="rounded-full border border-amber-500/30 bg-amber-500/[0.06] px-3 py-1.5 text-xs font-medium text-foreground"
                >
                  {section}
                </span>
              ))}
            </div>
          </div>
        </div>
      </div>
    </Surface>
  );
}

function ResearchReadingSections({ reading }: { reading: ResearchReadingModel }) {
  if (!reading.hasPublishedReadout) {
    return <MissingPublishedReadout reading={reading} />;
  }

  return (
    <div className="space-y-5">
      <Surface className="rounded-[24px] border border-border/70 px-5 py-5 sm:px-6">
        <SectionHeading
          eyebrow="Published Readout"
          title="Decision"
          description="The durable decision and findings from the source Published Readout."
        />

        <div className="mt-4 space-y-4">
          {reading.decisionSections.map((section) => (
            <ReadingSectionBlock key={section.title} section={section} tone="result" showTitle={false} />
          ))}

          <div className="grid gap-4 lg:grid-cols-[minmax(0,1.45fr)_minmax(18rem,0.75fr)]">
            <div className="space-y-3">
              {reading.resultSections.map((section) => (
                <ReadingSectionBlock key={section.title} section={section} tone="result" />
              ))}

              {reading.highlights.length > 0 ? (
                <div className="grid gap-3 md:grid-cols-2">
                  <DetailMetrics highlights={reading.highlights} />
                </div>
              ) : null}
            </div>

            <div className="space-y-3">
              {reading.interpretationSections.map((section) => (
                <ReadingSectionBlock key={section.title} section={section} tone="consideration" />
              ))}
            </div>
          </div>
        </div>
      </Surface>

      {reading.contextSections.length > 0 || reading.artifactSections.length > 0 || reading.parameters.length > 0 ? (
        <Surface className="rounded-[24px] px-5 py-5 sm:px-6">
          <SectionHeading
            eyebrow="Context"
            title="Scope And Source"
            description="Why the study was run, PIT scope, and source artifacts behind the published readout."
          />

          <div className="mt-5 grid gap-4 lg:grid-cols-2">
            {reading.contextSections.map((section) => (
              <ReadingSectionBlock key={section.title} section={section} tone="context" />
            ))}

            {reading.artifactSections.map((section) => (
              <ReadingSectionBlock key={section.title} section={section} tone="context" />
            ))}

            {reading.parameters.length > 0 ? (
              <div className="rounded-[22px] border border-border/60 bg-[var(--app-surface-muted)] px-4 py-4">
                <p className="text-[11px] font-semibold uppercase tracking-[0.14em] text-muted-foreground">
                  Key Settings
                </p>
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
  const hasStoredArtifacts = reading.tableHighlights.length > 0 || detail.outputTables.length > 0;
  const markdownLabel = detail.resultMetadata.source === 'docs' ? 'README.md' : 'summary.md';
  const markdownTitle = detail.resultMetadata.source === 'docs' ? 'Source Markdown' : 'Raw Bundle Markdown';
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
        description={getDetailDescription(reading)}
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
              <h2 className="mt-2 text-xl font-semibold tracking-tight text-foreground">{markdownTitle}</h2>
            </div>
            <div className="flex items-center gap-2 text-xs uppercase tracking-[0.14em] text-muted-foreground">
              <ScrollText className="h-4 w-4" />
              {markdownLabel}
            </div>
          </div>
          <div className="mt-5">
            <MarkdownSummary markdown={detail.summaryMarkdown} />
          </div>
        </Surface>

        {hasStoredArtifacts ? (
          <Surface className="rounded-[24px] px-5 py-5 sm:px-6">
            <SectionHeading
              eyebrow="Appendix"
              title="Stored Artifacts"
              description="Bundle tables stay here as supporting material so they do not compete with the readout."
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
                    {item.description ? (
                      <p className="mt-2 text-sm leading-6 text-muted-foreground">{item.description}</p>
                    ) : null}
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
        ) : null}
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
