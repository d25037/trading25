import { useNavigate } from '@tanstack/react-router';
import { FileSearch, Filter, Loader2, Search } from 'lucide-react';
import { type ChangeEvent, useDeferredValue, useMemo, useState } from 'react';
import { PageIntro, PageIntroMetaList, SectionEyebrow, SectionHeading, Surface } from '@/components/Layout/Workspace';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { useResearchCatalog } from '@/hooks/useResearch';
import { serializeResearchSearch } from '@/lib/routeSearch';
import { cn } from '@/lib/utils';
import type { ResearchCatalogItem, ResearchDecisionStatus } from '@/types/research';

type FilterValue = 'all' | string;
type StatusFilterValue = 'all' | ResearchDecisionStatus;
type DateSortValue = 'newest' | 'oldest';

const STATUS_LABELS: Record<ResearchDecisionStatus, string> = {
  observed: 'Observed',
  robust: 'Robust',
  candidate: 'Candidate',
  ranking_surface: 'Ranking',
  strategy_draft: 'Strategy Draft',
  production: 'Production',
  rejected: 'Rejected',
};

const STATUS_CLASSES: Record<ResearchDecisionStatus, string> = {
  observed: 'border-border/70 bg-[var(--app-surface-muted)] text-muted-foreground',
  robust: 'border-emerald-500/20 bg-emerald-500/10 text-emerald-700 dark:text-emerald-300',
  candidate: 'border-amber-500/20 bg-amber-500/10 text-amber-700 dark:text-amber-300',
  ranking_surface: 'border-sky-500/20 bg-sky-500/10 text-sky-700 dark:text-sky-300',
  strategy_draft: 'border-violet-500/20 bg-violet-500/10 text-violet-700 dark:text-violet-300',
  production: 'border-primary/20 bg-primary/10 text-primary',
  rejected: 'border-red-500/20 bg-red-500/10 text-red-700 dark:text-red-300',
};

const SYSTEM_RISK_FLAGS = new Set(['needs-publication-summary', 'docs-only', 'markdown-only']);

function formatTimestamp(value?: string | null): string {
  if (!value) return 'n/a';
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return parsed.toLocaleDateString();
}

function getTimestampValue(value?: string | null): number {
  if (!value) return 0;
  const parsed = new Date(value).getTime();
  return Number.isNaN(parsed) ? 0 : parsed;
}

function formatRange(item: ResearchCatalogItem): string {
  if (!item.analysisStartDate || !item.analysisEndDate) return 'n/a';
  return `${item.analysisStartDate} -> ${item.analysisEndDate}`;
}

function buildUniqueList(items: ResearchCatalogItem[], getValue: (item: ResearchCatalogItem) => string): string[] {
  return Array.from(new Set(items.map(getValue).filter(Boolean))).sort((left, right) => left.localeCompare(right));
}

function buildTagList(items: ResearchCatalogItem[]): string[] {
  return Array.from(new Set(items.flatMap((item) => item.tags))).sort((left, right) => left.localeCompare(right));
}

function matchesQuery(item: ResearchCatalogItem, query: string): boolean {
  if (!query) return true;
  const haystack = [
    item.title,
    item.objective,
    item.headline,
    item.experimentId,
    item.family,
    item.status,
    item.decision,
    item.promotedSurface,
    item.tags.join(' '),
    item.riskFlags.filter((flag) => !SYSTEM_RISK_FLAGS.has(flag)).join(' '),
  ]
    .filter((value): value is string => Boolean(value))
    .join(' ')
    .toLowerCase();
  return haystack.includes(query);
}

function buildCatalogViewModel(
  items: ResearchCatalogItem[],
  activeFamily: FilterValue,
  activeStatus: StatusFilterValue,
  activeTag: FilterValue,
  deferredQuery: string,
  dateSort: DateSortValue
) {
  const filtered = items.filter((item) => {
    if (activeFamily !== 'all' && item.family !== activeFamily) return false;
    if (activeStatus !== 'all' && item.status !== activeStatus) return false;
    if (activeTag !== 'all' && !item.tags.includes(activeTag)) return false;
    return matchesQuery(item, deferredQuery);
  });

  return [...filtered].sort((left, right) => {
    const leftTime = getTimestampValue(left.createdAt);
    const rightTime = getTimestampValue(right.createdAt);
    const dateOrder = dateSort === 'newest' ? rightTime - leftTime : leftTime - rightTime;
    if (dateOrder !== 0) return dateOrder;
    return left.title.localeCompare(right.title);
  });
}

function StatusBadge({ status }: { status: ResearchDecisionStatus }) {
  return (
    <span
      className={cn(
        'inline-flex items-center rounded-full border px-2.5 py-1 text-[11px] font-semibold uppercase tracking-[0.12em] whitespace-nowrap',
        STATUS_CLASSES[status]
      )}
    >
      {STATUS_LABELS[status]}
    </span>
  );
}

function PublicationBadge({ item }: { item: ResearchCatalogItem }) {
  if (item.hasStructuredSummary) {
    return (
      <span className="rounded-full border border-emerald-500/20 bg-emerald-500/10 px-2 py-0.5 text-[11px] text-emerald-700 dark:text-emerald-300">
        Published Readout
      </span>
    );
  }
  return (
    <span className="rounded-full border border-amber-500/20 bg-amber-500/10 px-2 py-0.5 text-[11px] text-amber-700 dark:text-amber-300">
      Needs Readout
    </span>
  );
}

function FilterSelect({
  label,
  value,
  options,
  onChange,
  formatOption = (option) => option,
  includeAll = true,
}: {
  label: string;
  value: string;
  options: string[];
  onChange: (value: string) => void;
  formatOption?: (value: string) => string;
  includeAll?: boolean;
}) {
  return (
    <label className="grid gap-1.5">
      <span className="text-[11px] font-semibold uppercase tracking-[0.14em] text-muted-foreground">{label}</span>
      <select
        value={value}
        onChange={(event) => onChange(event.target.value)}
        className="h-10 rounded-xl border border-border/70 bg-[var(--app-surface-muted)] px-3 text-sm text-foreground outline-none"
      >
        {includeAll ? <option value="all">All</option> : null}
        {options.map((option) => (
          <option key={option} value={option}>
            {formatOption(option)}
          </option>
        ))}
      </select>
    </label>
  );
}

function EvidenceMatrix({
  items,
  onOpen,
}: {
  items: ResearchCatalogItem[];
  onOpen: (item: ResearchCatalogItem) => void;
}) {
  return (
    <Surface className="overflow-hidden rounded-[24px] border border-border/70">
      <div className="border-b border-border/60 px-5 py-4 sm:px-6">
        <SectionHeading
          eyebrow="Evidence Matrix"
          title="Research Workspace"
          description="Compare each research title and decision against its state, publication status, risk flags, and research date."
        />
      </div>
      <Table className="table-fixed">
        <TableHeader>
          <TableRow className="bg-[var(--app-surface-muted)] hover:bg-[var(--app-surface-muted)]">
            <TableHead className="w-[9rem] whitespace-nowrap">State</TableHead>
            <TableHead>Findings</TableHead>
            <TableHead className="w-[14rem]">Readout & Risk</TableHead>
            <TableHead className="w-[14rem] whitespace-nowrap">Date</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {items.map((item) => {
            const visibleRiskFlags = item.riskFlags.filter((flag) => !SYSTEM_RISK_FLAGS.has(flag));
            return (
              <TableRow
                key={item.experimentId}
                tabIndex={0}
                onClick={() => onOpen(item)}
                onKeyDown={(event) => {
                  if (event.key === 'Enter' || event.key === ' ') {
                    event.preventDefault();
                    onOpen(item);
                  }
                }}
                aria-label={`Open ${item.title}`}
                className="app-interactive cursor-pointer align-top hover:bg-[var(--app-surface-muted)] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring/60"
              >
                <TableCell>
                  <div className="space-y-2">
                    <StatusBadge status={item.status} />
                    <p className="text-xs font-medium text-foreground">{item.family}</p>
                    <p className="text-[11px] uppercase tracking-[0.12em] text-muted-foreground">
                      {item.promotedSurface ?? 'Research'}
                    </p>
                  </div>
                </TableCell>
                <TableCell className="min-w-0">
                  <div className="space-y-2">
                    <div>
                      <p className="font-semibold leading-5 text-foreground">{item.title}</p>
                      <p className="mt-1 truncate text-xs font-mono text-muted-foreground">{item.experimentId}</p>
                    </div>
                    <p className="line-clamp-2 text-sm leading-6 text-muted-foreground">
                      {item.decision ?? 'No explicit decision recorded.'}
                    </p>
                    {item.tags.length > 0 ? (
                      <div className="flex flex-wrap gap-1.5">
                        {item.tags.slice(0, 4).map((tag) => (
                          <span
                            key={tag}
                            className="rounded-full border border-border/60 px-2 py-0.5 text-[11px] text-muted-foreground"
                          >
                            {tag}
                          </span>
                        ))}
                      </div>
                    ) : null}
                  </div>
                </TableCell>
                <TableCell className="space-y-3 text-sm leading-6 text-muted-foreground">
                  <div className="flex flex-wrap gap-1.5">
                    <PublicationBadge item={item} />
                  </div>
                  {visibleRiskFlags.length > 0 ? (
                    <div className="flex flex-wrap gap-1.5">
                      {visibleRiskFlags.map((flag) => (
                        <span
                          key={flag}
                          className="rounded-full border border-amber-500/20 bg-amber-500/10 px-2 py-0.5 text-[11px] text-amber-700 dark:text-amber-300"
                        >
                          {flag}
                        </span>
                      ))}
                    </div>
                  ) : (
                    <span className="text-sm text-muted-foreground">No extra risk</span>
                  )}
                </TableCell>
                <TableCell>
                  <div className="space-y-2">
                    <div>
                      <p className="text-[11px] font-semibold uppercase tracking-[0.14em] text-muted-foreground">
                        Created
                      </p>
                      <p className="text-sm font-medium text-foreground">{formatTimestamp(item.createdAt)}</p>
                    </div>
                    <div>
                      <p className="text-[11px] font-semibold uppercase tracking-[0.14em] text-muted-foreground">
                        Range
                      </p>
                      <p className="text-xs text-muted-foreground">{formatRange(item)}</p>
                    </div>
                    <p className="font-mono text-xs text-foreground">{item.runId}</p>
                  </div>
                </TableCell>
              </TableRow>
            );
          })}
        </TableBody>
      </Table>
    </Surface>
  );
}

function ResearchCatalogContent({
  isLoading,
  errorMessage,
  filteredItems,
  onOpen,
}: {
  isLoading: boolean;
  errorMessage: string | null;
  filteredItems: ResearchCatalogItem[];
  onOpen: (item: ResearchCatalogItem) => void;
}) {
  if (isLoading) {
    return (
      <Surface className="flex min-h-[24rem] items-center justify-center rounded-[24px]">
        <div className="flex items-center gap-3 text-sm text-muted-foreground">
          <Loader2 className="h-4 w-4 animate-spin" />
          Loading research workspace...
        </div>
      </Surface>
    );
  }

  if (errorMessage) {
    return (
      <Surface className="rounded-[24px] px-6 py-6">
        <p className="text-sm font-semibold text-foreground">Research load failed</p>
        <p className="mt-2 text-sm leading-6 text-muted-foreground">{errorMessage}</p>
      </Surface>
    );
  }

  if (filteredItems.length === 0) {
    return (
      <Surface className="rounded-[24px] px-6 py-10 text-center">
        <div className="mx-auto flex max-w-xl flex-col items-center gap-3">
          <div className="flex h-12 w-12 items-center justify-center rounded-2xl border border-border/70 bg-[var(--app-surface-muted)]">
            <FileSearch className="h-5 w-5 text-primary" />
          </div>
          <h2 className="text-xl font-semibold tracking-tight text-foreground">No matching research</h2>
          <p className="text-sm leading-6 text-muted-foreground">Adjust the query, family, status, or tag filter.</p>
        </div>
      </Surface>
    );
  }

  return <EvidenceMatrix items={filteredItems} onOpen={onOpen} />;
}

export function ResearchPage() {
  const navigate = useNavigate();
  const catalogQuery = useResearchCatalog();
  const items = catalogQuery.data?.items ?? [];
  const [query, setQuery] = useState('');
  const [activeFamily, setActiveFamily] = useState<FilterValue>('all');
  const [activeStatus, setActiveStatus] = useState<StatusFilterValue>('all');
  const [activeTag, setActiveTag] = useState<FilterValue>('all');
  const [dateSort, setDateSort] = useState<DateSortValue>('newest');
  const deferredQuery = useDeferredValue(query.trim().toLowerCase());

  const availableFamilies = useMemo(() => buildUniqueList(items, (item) => item.family), [items]);
  const availableStatuses = useMemo(
    () => Array.from(new Set(items.map((item) => item.status))).sort((left, right) => left.localeCompare(right)),
    [items]
  );
  const availableTags = useMemo(() => buildTagList(items), [items]);
  const filteredItems = useMemo(
    () => buildCatalogViewModel(items, activeFamily, activeStatus, activeTag, deferredQuery, dateSort),
    [activeFamily, activeStatus, activeTag, dateSort, deferredQuery, items]
  );

  const promotedCount = items.filter((item) => item.promotedSurface && item.promotedSurface !== 'Research').length;
  const metaItems = [
    { label: 'Experiments', value: String(items.length) },
    { label: 'Visible', value: String(filteredItems.length) },
    { label: 'Families', value: String(availableFamilies.length) },
    { label: 'Promoted', value: String(promotedCount) },
  ];

  const openDetail = (item: ResearchCatalogItem) => {
    void navigate({
      to: '/research/detail',
      search: serializeResearchSearch({
        experimentId: item.experimentId,
        runId: item.runId,
      }),
    });
  };

  return (
    <div className="min-h-0 flex-1 overflow-auto px-4 py-4 sm:px-6 sm:py-5">
      <div className="mx-auto flex w-full max-w-[1180px] flex-col gap-5">
        <PageIntro
          eyebrow="Research Workspace"
          title="Evidence Matrix"
          description="Review research by title, decision, state, publication status, and risk flags before opening the detail reader."
          meta={<PageIntroMetaList items={metaItems} />}
        />

        <Surface className="rounded-[24px] border border-border/70 px-5 py-4 sm:px-6">
          <div className="grid gap-4 lg:grid-cols-[minmax(18rem,1fr)_auto] lg:items-end">
            <div className="space-y-2">
              <SectionEyebrow>Workspace Filters</SectionEyebrow>
              <label className="flex items-center gap-3 rounded-xl border border-border/70 bg-[var(--app-surface-muted)] px-3 py-2.5">
                <Search className="h-4 w-4 text-muted-foreground" />
                <input
                  value={query}
                  onChange={(event: ChangeEvent<HTMLInputElement>) => setQuery(event.target.value)}
                  placeholder="Search title, decision, experiment id, tag, or risk"
                  className="w-full bg-transparent text-sm text-foreground outline-none placeholder:text-muted-foreground"
                />
              </label>
            </div>

            <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
              <FilterSelect
                label="Family"
                value={activeFamily}
                options={availableFamilies}
                onChange={(value) => setActiveFamily(value)}
              />
              <FilterSelect
                label="Status"
                value={activeStatus}
                options={availableStatuses}
                onChange={(value) => setActiveStatus(value as StatusFilterValue)}
                formatOption={(value) => STATUS_LABELS[value as ResearchDecisionStatus] ?? value}
              />
              <FilterSelect label="Tag" value={activeTag} options={availableTags} onChange={setActiveTag} />
              <FilterSelect
                label="Sort"
                value={dateSort}
                options={['newest', 'oldest']}
                onChange={(value) => setDateSort(value as DateSortValue)}
                formatOption={(value) => (value === 'newest' ? 'Newest date' : 'Oldest date')}
                includeAll={false}
              />
            </div>
          </div>

          <div className="mt-4 flex items-center gap-2 text-xs text-muted-foreground">
            <Filter className="h-4 w-4" />
            <span>
              {activeFamily === 'all' ? 'All families' : activeFamily} /{' '}
              {activeStatus === 'all' ? 'All statuses' : STATUS_LABELS[activeStatus]} /{' '}
              {activeTag === 'all' ? 'All tags' : activeTag} / {dateSort === 'newest' ? 'Newest date' : 'Oldest date'}
            </span>
          </div>
        </Surface>

        <ResearchCatalogContent
          isLoading={catalogQuery.isLoading}
          errorMessage={catalogQuery.error?.message ?? null}
          filteredItems={filteredItems}
          onOpen={openDetail}
        />
      </div>
    </div>
  );
}
