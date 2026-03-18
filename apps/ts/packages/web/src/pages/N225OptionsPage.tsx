import { useEffect, useMemo, useState } from 'react';
import { useN225Options } from '@/hooks/useN225Options';
import { useMigrateOptions225RouteState, useOptions225RouteState } from '@/hooks/useOptions225RouteState';
import {
  filterOptionsItems,
  getOptionRowKey,
  resolveSelectedOptionRowKey,
  summarizeFilteredOptions,
} from '@/lib/options225';
import {
  N225OptionsChainCard,
  N225OptionsDetailCard,
  N225OptionsFiltersCard,
  N225OptionsSummaryGrid,
} from '@/components/N225Options/N225OptionsSections';

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

  return (
    <div className="space-y-6 p-6">
      <div className="rounded-2xl gradient-primary p-6 text-white shadow-lg shadow-primary/20">
        <div className="flex flex-col gap-3 lg:flex-row lg:items-end lg:justify-between">
          <div>
            <p className="text-sm uppercase tracking-[0.24em] text-white/70">Derivatives Explorer</p>
            <h1 className="mt-2 text-3xl font-semibold">N225 Options</h1>
            <p className="mt-2 max-w-3xl text-sm text-white/80">
              Explore the full Nikkei 225 options chain for a resolved trading date, with backend-managed auto date
              resolution and URL-synced filters.
            </p>
          </div>
          <div className="grid gap-2 text-sm text-white/90">
            <div>
              <span className="font-medium">Resolved Date:</span> {data?.resolvedDate ?? '-'}
            </div>
            <div>
              <span className="font-medium">Requested Date:</span> {data?.requestedDate ?? 'auto'}
            </div>
            <div>
              <span className="font-medium">Source Calls:</span> {data?.sourceCallCount ?? '-'}
            </div>
          </div>
        </div>
      </div>

      <N225OptionsSummaryGrid data={data} filteredSummary={filteredSummary} />

      <div className="grid gap-6 xl:grid-cols-[320px_minmax(0,1fr)_340px]">
        <aside>
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

        <aside>
          <N225OptionsDetailCard selectedItem={selectedItem} />
        </aside>
      </div>
    </div>
  );
}
