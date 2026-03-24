import { useEffect, useMemo, useState } from 'react';
import { PageIntro, PageIntroMetaList } from '@/components/Layout/Workspace';
import {
  N225OptionsChainCard,
  N225OptionsDetailCard,
  N225OptionsFiltersCard,
  N225OptionsSummaryGrid,
} from '@/components/N225Options/N225OptionsSections';
import { useN225Options } from '@/hooks/useN225Options';
import { useMigrateOptions225RouteState, useOptions225RouteState } from '@/hooks/useOptions225RouteState';
import { ApiError } from '@/lib/api-client';
import {
  filterOptionsItems,
  getOptionRowKey,
  resolveSelectedOptionRowKey,
  summarizeFilteredOptions,
} from '@/lib/options225';

export function N225OptionsPage() {
  useMigrateOptions225RouteState();

  const {
    date,
    putCall,
    contractMonth,
    strikeMin,
    strikeMax,
    sortBy,
    order,
    setDate,
    setPutCall,
    setContractMonth,
    setStrikeRange,
    setSort,
  } = useOptions225RouteState();
  const { data, isLoading, error, refetch, isFetching } = useN225Options({ date: date ?? undefined });
  const [selectedRowKey, setSelectedRowKey] = useState<string | null>(null);

  const filteredItems = useMemo(
    () =>
      filterOptionsItems(data?.items ?? [], {
        putCall,
        contractMonth,
        strikeMin,
        strikeMax,
        sortBy,
        order,
      }),
    [contractMonth, data?.items, order, putCall, sortBy, strikeMax, strikeMin]
  );
  const filteredSummary = useMemo(() => summarizeFilteredOptions(filteredItems), [filteredItems]);

  useEffect(() => {
    const nextSelectedRowKey = resolveSelectedOptionRowKey(filteredItems, selectedRowKey);
    if (nextSelectedRowKey !== selectedRowKey) {
      setSelectedRowKey(nextSelectedRowKey);
    }
  }, [filteredItems, selectedRowKey]);

  const selectedItem = useMemo(
    () => filteredItems.find((item) => getOptionRowKey(item) === selectedRowKey) ?? null,
    [filteredItems, selectedRowKey]
  );
  const introMetaItems = [
    { label: 'Resolved Date', value: data?.resolvedDate ?? '-' },
    { label: 'Requested Date', value: data?.requestedDate ?? 'auto' },
    { label: 'Source Calls', value: data?.sourceCallCount?.toString() ?? '-' },
  ];

  return (
    <div className="flex flex-col gap-6 p-6">
      <PageIntro
        eyebrow="Derivatives"
        title="N225 Options"
        description="Inspect the local Nikkei 225 options chain from DuckDB with URL-synced filters and backend-managed date resolution."
        meta={<PageIntroMetaList items={introMetaItems} />}
      />

      {error instanceof ApiError && error.status === 404 ? (
        <div className="rounded-2xl border border-amber-500/30 bg-amber-500/10 px-4 py-3 text-sm text-amber-900 dark:text-amber-200">
          Local N225 options data is not available yet. Run Market DB sync to ingest `options_225_data`.{' '}
          <a href="/market-db" className="font-medium underline underline-offset-4">
            Open Market DB
          </a>
        </div>
      ) : null}

      <N225OptionsSummaryGrid data={data} filteredSummary={filteredSummary} />

      <div className="grid gap-6 xl:grid-cols-[minmax(280px,320px)_minmax(0,1fr)_minmax(300px,340px)]">
        <aside className="space-y-4">
          <N225OptionsFiltersCard
            date={date}
            putCall={putCall}
            contractMonth={contractMonth}
            strikeMin={strikeMin}
            strikeMax={strikeMax}
            sortBy={sortBy}
            order={order}
            availableContractMonths={data?.availableContractMonths ?? []}
            setDate={setDate}
            setPutCall={setPutCall}
            setContractMonth={setContractMonth}
            setStrikeRange={setStrikeRange}
            setSort={setSort}
            onReset={() => {
              setDate(null);
              setPutCall('all');
              setContractMonth(null);
              setStrikeRange(null, null);
              setSort('openInterest', 'desc');
            }}
          />
        </aside>

        <section className="min-w-0">
          <N225OptionsChainCard
            filteredItems={filteredItems}
            totalCount={data?.summary.totalCount ?? 0}
            isLoading={isLoading}
            error={error}
            isFetching={isFetching}
            selectedRowKey={selectedRowKey}
            setSelectedRowKey={setSelectedRowKey}
            onRefresh={() => {
              void refetch();
            }}
          />
        </section>

        <aside className="space-y-4">
          <N225OptionsDetailCard selectedItem={selectedItem} />
        </aside>
      </div>
    </div>
  );
}
