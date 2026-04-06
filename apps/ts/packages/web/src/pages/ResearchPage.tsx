import { useNavigate } from '@tanstack/react-router';
import { FileSearch, Filter, Loader2, Search } from 'lucide-react';
import { type ChangeEvent, useDeferredValue, useMemo, useState } from 'react';
import {
  PageIntro,
  PageIntroMetaList,
  SectionEyebrow,
  SectionHeading,
  Surface,
} from '@/components/Layout/Workspace';
import { useResearchCatalog } from '@/hooks/useResearch';
import { serializeResearchSearch } from '@/lib/routeSearch';
import type { ResearchCatalogItem } from '@/types/research';
import { cn } from '@/lib/utils';

const CURATED_TOPIX_MODE_EXPERIMENTS = [
  'market-behavior/topix-extreme-mode-mean-reversion-comparison',
  'market-behavior/topix-extreme-close-to-close-mode',
  'market-behavior/topix-streak-extreme-mode',
  'market-behavior/topix-streak-multi-timeframe-mode',
] as const;

function formatTimestamp(value?: string | null): string {
  if (!value) return 'n/a';
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return parsed.toLocaleDateString();
}

function buildTagList(items: ResearchCatalogItem[]): string[] {
  return Array.from(new Set(items.flatMap((item) => item.tags))).sort((left, right) => left.localeCompare(right));
}

function matchesQuery(item: ResearchCatalogItem, query: string): boolean {
  if (!query) {
    return true;
  }
  const haystack = [item.title, item.objective, item.headline, item.experimentId, item.tags.join(' ')]
    .filter((value): value is string => Boolean(value))
    .join(' ')
    .toLowerCase();
  return haystack.includes(query);
}

function collectCuratedStudies(items: ResearchCatalogItem[]): ResearchCatalogItem[] {
  const itemMap = new Map(items.map((item) => [item.experimentId, item]));
  return CURATED_TOPIX_MODE_EXPERIMENTS.map((experimentId) => itemMap.get(experimentId)).filter(
    (item): item is ResearchCatalogItem => Boolean(item)
  );
}

function ResearchCard({
  item,
  featured = false,
  onOpen,
}: {
  item: ResearchCatalogItem;
  featured?: boolean;
  onOpen: (item: ResearchCatalogItem) => void;
}) {
  return (
    <button
      type="button"
      onClick={() => onOpen(item)}
      className={cn(
        'app-interactive group w-full text-left',
        featured ? 'rounded-[32px]' : 'rounded-[26px]'
      )}
    >
      <Surface
        className={cn(
          'h-full overflow-hidden border transition-colors',
          featured
            ? 'rounded-[32px] border-amber-500/20 bg-[radial-gradient(circle_at_top_left,rgba(245,158,11,0.14),transparent_34%),radial-gradient(circle_at_bottom_right,rgba(14,165,233,0.12),transparent_36%)] px-6 py-6 sm:px-7'
            : 'rounded-[26px] border-border/70 bg-[var(--app-surface-muted)] px-5 py-5 group-hover:bg-[var(--app-surface-emphasis)]'
        )}
      >
        <div className="flex items-start justify-between gap-4">
          <div className="min-w-0 space-y-3">
            <div className="flex flex-wrap items-center gap-2">
              <span
                className={cn(
                  'inline-flex items-center rounded-full border px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.16em]',
                  featured
                    ? 'border-amber-500/20 bg-amber-500/10 text-amber-700 dark:text-amber-300'
                    : 'border-border/70 bg-background/70 text-muted-foreground'
                )}
              >
                {featured ? 'Latest' : item.hasStructuredSummary ? 'Structured' : 'Markdown'}
              </span>
              <span className="text-[11px] font-medium uppercase tracking-[0.14em] text-muted-foreground">
                {formatTimestamp(item.createdAt)}
              </span>
            </div>

            <div className="space-y-2">
              <h2 className={cn('font-semibold tracking-tight text-foreground', featured ? 'text-3xl sm:text-[2.15rem]' : 'text-xl')}>
                {item.title}
              </h2>
              <p className={cn('max-w-3xl leading-6 text-muted-foreground', featured ? 'text-base' : 'text-sm')}>
                {item.headline ?? item.objective ?? 'Published playground analysis.'}
              </p>
            </div>

            <div className="flex flex-wrap gap-2">
              {item.tags.map((tag) => (
                <span
                  key={tag}
                  className="rounded-full border border-border/60 px-2.5 py-1 text-[11px] font-medium text-muted-foreground"
                >
                  {tag}
                </span>
              ))}
            </div>
          </div>

          <div className="hidden rounded-full border border-border/70 px-3 py-1 text-[11px] uppercase tracking-[0.14em] text-muted-foreground md:block">
            Open
          </div>
        </div>

        <div className={cn('mt-5 grid gap-3', featured ? 'md:grid-cols-3' : 'md:grid-cols-2')}>
          <div className="rounded-2xl border border-white/20 bg-white/55 px-4 py-3 dark:border-white/6 dark:bg-white/4">
            <p className="text-[11px] font-semibold uppercase tracking-[0.14em] text-muted-foreground">Experiment</p>
            <p className="mt-2 font-mono text-xs text-foreground">{item.experimentId}</p>
          </div>
          <div className="rounded-2xl border border-white/20 bg-white/55 px-4 py-3 dark:border-white/6 dark:bg-white/4">
            <p className="text-[11px] font-semibold uppercase tracking-[0.14em] text-muted-foreground">Analysis Range</p>
            <p className="mt-2 text-sm font-medium text-foreground">
              {item.analysisStartDate && item.analysisEndDate ? `${item.analysisStartDate} -> ${item.analysisEndDate}` : 'n/a'}
            </p>
          </div>
          <div className="rounded-2xl border border-white/20 bg-white/55 px-4 py-3 dark:border-white/6 dark:bg-white/4">
            <p className="text-[11px] font-semibold uppercase tracking-[0.14em] text-muted-foreground">Run</p>
            <p className="mt-2 font-mono text-xs text-foreground">{item.runId}</p>
          </div>
        </div>
      </Surface>
    </button>
  );
}

export function ResearchPage() {
  const navigate = useNavigate();
  const catalogQuery = useResearchCatalog();
  const items = catalogQuery.data?.items ?? [];
  const [query, setQuery] = useState('');
  const [activeTag, setActiveTag] = useState<'all' | string>('all');
  const deferredQuery = useDeferredValue(query.trim().toLowerCase());

  const availableTags = useMemo(() => buildTagList(items), [items]);
  const filteredItems = useMemo(
    () =>
      items.filter((item) => {
        if (activeTag !== 'all' && !item.tags.includes(activeTag)) {
          return false;
        }
        return matchesQuery(item, deferredQuery);
      }),
    [activeTag, deferredQuery, items]
  );
  const curatedItems = useMemo(() => collectCuratedStudies(filteredItems), [filteredItems]);
  const curatedExperimentIds = useMemo(() => new Set(curatedItems.map((item) => item.experimentId)), [curatedItems]);
  const libraryCandidates = useMemo(
    () => filteredItems.filter((item) => !curatedExperimentIds.has(item.experimentId)),
    [curatedExperimentIds, filteredItems]
  );
  const featuredItem = libraryCandidates[0] ?? null;
  const remainingItems = featuredItem ? libraryCandidates.slice(1) : libraryCandidates;

  const metaItems = [
    { label: 'Published Experiments', value: String(items.length) },
    { label: 'Visible Results', value: String(filteredItems.length) },
    { label: 'Structured Summaries', value: String(items.filter((item) => item.hasStructuredSummary).length) },
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
      <div className="mx-auto flex w-full max-w-[1180px] flex-col gap-6">
        <PageIntro
          eyebrow="Research Library"
          title="Playground Analyses"
          description="Use this library view to find a study first. The detail view is now dedicated to reading the result and consideration sections without catalog noise."
          meta={<PageIntroMetaList items={metaItems} />}
        />

        <Surface className="rounded-[30px] border border-border/70 px-5 py-5 sm:px-6">
          <div className="flex flex-col gap-5">
            <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
              <div className="space-y-2">
                <SectionEyebrow>Catalog Controls</SectionEyebrow>
                <h2 className="text-2xl font-semibold tracking-tight text-foreground">Search, Filter, Then Open</h2>
                <p className="max-w-3xl text-sm leading-6 text-muted-foreground">
                  This page is intentionally optimized for discovery. Open a detail page only after narrowing the catalog.
                </p>
              </div>

              <div className="grid gap-3 sm:grid-cols-[minmax(18rem,26rem)_auto]">
                <label className="flex items-center gap-3 rounded-full border border-border/70 bg-[var(--app-surface-muted)] px-4 py-3">
                  <Search className="h-4 w-4 text-muted-foreground" />
                  <input
                    value={query}
                    onChange={(event: ChangeEvent<HTMLInputElement>) => setQuery(event.target.value)}
                    placeholder="Search title, takeaway, experiment id, or tag"
                    className="w-full bg-transparent text-sm text-foreground outline-none placeholder:text-muted-foreground"
                  />
                </label>
                <div className="inline-flex items-center gap-2 rounded-full border border-border/70 bg-[var(--app-surface-muted)] px-4 py-3 text-sm text-muted-foreground">
                  <Filter className="h-4 w-4" />
                  {activeTag === 'all' ? 'All tags' : activeTag}
                </div>
              </div>
            </div>

            <div className="flex flex-wrap gap-2">
              <button
                type="button"
                onClick={() => setActiveTag('all')}
                className={cn(
                  'app-interactive rounded-full border px-3 py-1.5 text-sm',
                  activeTag === 'all'
                    ? 'border-border/70 bg-[var(--app-surface-emphasis)] text-foreground'
                    : 'border-border/60 bg-[var(--app-surface-muted)] text-muted-foreground hover:text-foreground'
                )}
              >
                All
              </button>
              {availableTags.map((tag) => (
                <button
                  key={tag}
                  type="button"
                  onClick={() => setActiveTag(tag)}
                  className={cn(
                    'app-interactive rounded-full border px-3 py-1.5 text-sm',
                    activeTag === tag
                      ? 'border-border/70 bg-[var(--app-surface-emphasis)] text-foreground'
                      : 'border-border/60 bg-[var(--app-surface-muted)] text-muted-foreground hover:text-foreground'
                  )}
                >
                  {tag}
                </button>
              ))}
            </div>
          </div>
        </Surface>

        {catalogQuery.isLoading ? (
          <Surface className="flex min-h-[24rem] items-center justify-center rounded-[30px]">
            <div className="flex items-center gap-3 text-sm text-muted-foreground">
              <Loader2 className="h-4 w-4 animate-spin" />
              Loading research catalog...
            </div>
          </Surface>
        ) : catalogQuery.error ? (
          <Surface className="rounded-[30px] px-6 py-6">
            <p className="text-sm font-semibold text-foreground">Catalog load failed</p>
            <p className="mt-2 text-sm leading-6 text-muted-foreground">{catalogQuery.error.message}</p>
          </Surface>
        ) : filteredItems.length === 0 ? (
          <Surface className="rounded-[30px] px-6 py-10 text-center">
            <div className="mx-auto flex max-w-xl flex-col items-center gap-3">
              <div className="flex h-14 w-14 items-center justify-center rounded-3xl border border-border/70 bg-[var(--app-surface-muted)]">
                <FileSearch className="h-5 w-5 text-primary" />
              </div>
              <h2 className="text-2xl font-semibold tracking-tight text-foreground">No matching analyses</h2>
              <p className="text-sm leading-6 text-muted-foreground">
                Adjust the search or tag filter. The catalog is now designed to scale by narrowing first, then opening detail.
              </p>
            </div>
          </Surface>
        ) : (
          <div className="space-y-6">
            {curatedItems.length > 0 ? (
              <section className="space-y-3">
                <SectionHeading
                  eyebrow="Curated"
                  title="TOPIX Mode Studies"
                  description="These four studies belong together: the normal daily mode, the standalone streak mode, the streak multi-timeframe pair scan, and the direct mean-reversion comparison."
                />
                <div className="grid gap-5 [grid-template-columns:repeat(auto-fit,minmax(min(100%,22rem),1fr))]">
                  {curatedItems.map((item) => (
                    <ResearchCard key={item.experimentId} item={item} onOpen={openDetail} />
                  ))}
                </div>
              </section>
            ) : null}

            {featuredItem ? (
              <section className="space-y-3">
                <SectionHeading
                  eyebrow="Featured"
                  title="Start With The Latest High-Signal Bundle"
                  description="The newest matching bundle gets the widest treatment so the catalog still reads clearly as it grows."
                />
                <ResearchCard item={featuredItem} featured onOpen={openDetail} />
              </section>
            ) : null}

            {remainingItems.length > 0 ? (
              <section className="space-y-3">
                <SectionHeading
                  eyebrow="Library"
                  title="More Published Analyses"
                  description="Everything below keeps the same open flow, but in a denser discovery grid."
                />
                <div className="grid gap-5 [grid-template-columns:repeat(auto-fit,minmax(min(100%,24rem),1fr))]">
                  {remainingItems.map((item) => (
                    <ResearchCard key={item.experimentId} item={item} onOpen={openDetail} />
                  ))}
                </div>
              </section>
            ) : null}
          </div>
        )}
      </div>
    </div>
  );
}
