import {
  BarChart3,
  Calendar,
  Filter,
  Loader2,
  RefreshCcw,
  Sigma,
  type LucideIcon,
} from 'lucide-react';
import type { ReactNode } from 'react';
import type { Options225PutCallFilter, Options225SortBy, SortOrder } from '@trading25/contracts/types/api-response-types';
import { DateInput } from '@/components/shared/filters';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { useVirtualizedRows } from '@/hooks/useVirtualizedRows';
import {
  formatOptionsNumber,
  formatOptionsRange,
  getOptionRowKey,
  parseOptionsNumericInput,
  type Options225FilteredSummary,
} from '@/lib/options225';
import { cn } from '@/lib/utils';
import type { N225OptionItem, N225OptionsExplorerResponse } from '@/types/options225';

const SORT_OPTIONS: Array<{ value: Options225SortBy; label: string }> = [
  { value: 'openInterest', label: 'Open Interest' },
  { value: 'volume', label: 'Volume' },
  { value: 'strikePrice', label: 'Strike Price' },
  { value: 'impliedVolatility', label: 'Implied Volatility' },
  { value: 'wholeDayClose', label: 'Whole Day Close' },
];

const ORDER_OPTIONS: Array<{ value: SortOrder; label: string }> = [
  { value: 'desc', label: 'Descending' },
  { value: 'asc', label: 'Ascending' },
];

const PUT_CALL_OPTIONS: Array<{ value: Options225PutCallFilter; label: string }> = [
  { value: 'all', label: 'All' },
  { value: 'put', label: 'Put' },
  { value: 'call', label: 'Call' },
];

const VIRTUALIZATION_THRESHOLD = 80;
const TABLE_ROW_HEIGHT = 45;
const TABLE_VIEWPORT_HEIGHT = 560;

function SummaryCard({
  title,
  value,
  detail,
  icon: Icon,
}: {
  title: string;
  value: string;
  detail: string;
  icon: LucideIcon;
}) {
  return (
    <Card className="glass-panel">
      <CardContent className="flex items-start justify-between p-4">
        <div>
          <p className="text-xs uppercase tracking-wider text-muted-foreground">{title}</p>
          <p className="mt-2 text-2xl font-semibold tabular-nums">{value}</p>
          <p className="mt-1 text-xs text-muted-foreground">{detail}</p>
        </div>
        <div className="gradient-primary rounded-lg p-2">
          <Icon className="h-4 w-4 text-white" />
        </div>
      </CardContent>
    </Card>
  );
}

export function N225OptionsSummaryGrid({
  data,
  filteredSummary,
}: {
  data: N225OptionsExplorerResponse | undefined;
  filteredSummary: Options225FilteredSummary;
}) {
  return (
    <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
      <SummaryCard
        title="Filtered Contracts"
        value={formatOptionsNumber(filteredSummary.filteredCount, 0)}
        detail={`${filteredSummary.putCount} put / ${filteredSummary.callCount} call`}
        icon={Filter}
      />
      <SummaryCard
        title="Total Open Interest"
        value={formatOptionsNumber(filteredSummary.totalOpenInterest, 0)}
        detail={`All-chain: ${formatOptionsNumber(data?.summary.totalOpenInterest, 0)}`}
        icon={BarChart3}
      />
      <SummaryCard
        title="Strike Range"
        value={formatOptionsRange(data?.summary.strikePriceRange.min ?? null, data?.summary.strikePriceRange.max ?? null, 0)}
        detail={`Months: ${(data?.availableContractMonths ?? []).length}`}
        icon={Sigma}
      />
      <SummaryCard
        title="Underlying Range"
        value={formatOptionsRange(data?.summary.underlyingPriceRange.min ?? null, data?.summary.underlyingPriceRange.max ?? null, 2)}
        detail={`Last updated: ${data?.lastUpdated ?? '-'}`}
        icon={Calendar}
      />
    </div>
  );
}

export function N225OptionsFiltersCard(props: {
  date: string | null;
  putCall: Options225PutCallFilter;
  contractMonth: string | null;
  strikeMin: number | null;
  strikeMax: number | null;
  sortBy: Options225SortBy;
  order: SortOrder;
  availableContractMonths: string[];
  setDate: (date: string | null) => void;
  setPutCall: (putCall: Options225PutCallFilter) => void;
  setContractMonth: (contractMonth: string | null) => void;
  setStrikeRange: (strikeMin: number | null, strikeMax: number | null) => void;
  setSort: (sortBy: Options225SortBy, order: SortOrder) => void;
  onReset: () => void;
}) {
  const {
    date,
    putCall,
    contractMonth,
    strikeMin,
    strikeMax,
    sortBy,
    order,
    availableContractMonths,
    setDate,
    setPutCall,
    setContractMonth,
    setStrikeRange,
    setSort,
    onReset,
  } = props;

  return (
    <Card className="glass-panel">
      <CardHeader className="pb-3">
        <div className="flex items-center justify-between gap-3">
          <CardTitle className="text-base">Filters</CardTitle>
          <button
            type="button"
            className="inline-flex items-center gap-1 text-xs text-muted-foreground transition-colors hover:text-foreground"
            onClick={onReset}
          >
            <RefreshCcw className="h-3.5 w-3.5" />
            Reset
          </button>
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        <DateInput value={date ?? undefined} onChange={(value) => setDate(value ?? null)} id="options-225-date" label="Trade Date" />

        <div className="space-y-2">
          <Label htmlFor="put-call" className="text-xs">
            Put / Call
          </Label>
          <Select value={putCall} onValueChange={(value) => setPutCall(value as Options225PutCallFilter)}>
            <SelectTrigger id="put-call" className="h-8 text-xs">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {PUT_CALL_OPTIONS.map((option) => (
                <SelectItem key={option.value} value={option.value}>
                  {option.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>

        <div className="space-y-2">
          <Label htmlFor="contract-month" className="text-xs">
            Contract Month
          </Label>
          <Select value={contractMonth ?? 'all'} onValueChange={(value) => setContractMonth(value === 'all' ? null : value)}>
            <SelectTrigger id="contract-month" className="h-8 text-xs">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All Months</SelectItem>
              {availableContractMonths.map((month) => (
                <SelectItem key={month} value={month}>
                  {month}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>

        <div className="grid gap-3 sm:grid-cols-2">
          <div className="space-y-2">
            <Label htmlFor="strike-min" className="text-xs">
              Strike Min
            </Label>
            <Input
              id="strike-min"
              type="number"
              inputMode="decimal"
              className="h-8 text-xs"
              value={strikeMin ?? ''}
              onChange={(event) => setStrikeRange(parseOptionsNumericInput(event.target.value), strikeMax)}
            />
          </div>
          <div className="space-y-2">
            <Label htmlFor="strike-max" className="text-xs">
              Strike Max
            </Label>
            <Input
              id="strike-max"
              type="number"
              inputMode="decimal"
              className="h-8 text-xs"
              value={strikeMax ?? ''}
              onChange={(event) => setStrikeRange(strikeMin, parseOptionsNumericInput(event.target.value))}
            />
          </div>
        </div>

        <div className="space-y-2">
          <Label htmlFor="sort-by" className="text-xs">
            Sort By
          </Label>
          <Select value={sortBy} onValueChange={(value) => setSort(value as Options225SortBy, order)}>
            <SelectTrigger id="sort-by" className="h-8 text-xs">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {SORT_OPTIONS.map((option) => (
                <SelectItem key={option.value} value={option.value}>
                  {option.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>

        <div className="space-y-2">
          <Label htmlFor="sort-order" className="text-xs">
            Order
          </Label>
          <Select value={order} onValueChange={(value) => setSort(sortBy, value as SortOrder)}>
            <SelectTrigger id="sort-order" className="h-8 text-xs">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {ORDER_OPTIONS.map((option) => (
                <SelectItem key={option.value} value={option.value}>
                  {option.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      </CardContent>
    </Card>
  );
}

export function N225OptionsChainCard(props: {
  filteredItems: N225OptionItem[];
  totalCount: number;
  isLoading: boolean;
  error: unknown;
  isFetching: boolean;
  selectedRowKey: string | null;
  setSelectedRowKey: (rowKey: string) => void;
  onRefresh: () => void;
}) {
  const { filteredItems, totalCount, isLoading, error, isFetching, selectedRowKey, setSelectedRowKey, onRefresh } = props;
  const shouldVirtualize = filteredItems.length >= VIRTUALIZATION_THRESHOLD;
  const virtual = useVirtualizedRows(filteredItems, {
    enabled: shouldVirtualize,
    rowHeight: TABLE_ROW_HEIGHT,
    viewportHeight: TABLE_VIEWPORT_HEIGHT,
  });

  let content: ReactNode;
  if (isLoading) {
    content = (
      <div className="flex h-80 items-center justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    );
  } else if (error) {
    content = (
      <div className="flex h-80 flex-col items-center justify-center gap-3 px-6 text-center">
        <p className="text-base font-medium text-destructive">Failed to load N225 options data</p>
        <p className="text-sm text-muted-foreground">{error instanceof Error ? error.message : 'Unknown error'}</p>
      </div>
    );
  } else if (filteredItems.length === 0) {
    content = (
      <div className="flex h-80 flex-col items-center justify-center gap-3 px-6 text-center">
        <p className="text-base font-medium">No contracts match the current filters</p>
        <p className="text-sm text-muted-foreground">Adjust put/call, contract month, or strike range filters.</p>
      </div>
    );
  } else {
    content = (
      <div className="overflow-auto" style={{ maxHeight: TABLE_VIEWPORT_HEIGHT }} onScroll={virtual.onScroll}>
        <Table>
          <TableHeader className="sticky top-0 z-10 bg-background/95 backdrop-blur">
            <TableRow>
              <TableHead>Code</TableHead>
              <TableHead>Month</TableHead>
              <TableHead>Type</TableHead>
              <TableHead className="text-right">Strike</TableHead>
              <TableHead className="text-right">Close</TableHead>
              <TableHead className="text-right">Volume</TableHead>
              <TableHead className="text-right">Open Int.</TableHead>
              <TableHead className="text-right">IV</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {shouldVirtualize && virtual.paddingTop > 0 ? (
              <TableRow>
                <TableCell colSpan={8} className="p-0" style={{ height: virtual.paddingTop }} />
              </TableRow>
            ) : null}
            {virtual.visibleItems.map((item) => {
              const rowKey = getOptionRowKey(item);
              const isSelected = rowKey === selectedRowKey;
              return (
                <TableRow
                  key={rowKey}
                  className={cn('cursor-pointer', isSelected && 'bg-accent/40')}
                  onClick={() => setSelectedRowKey(rowKey)}
                >
                  <TableCell className="font-medium">{item.code}</TableCell>
                  <TableCell>{item.contractMonth ?? '-'}</TableCell>
                  <TableCell className="uppercase">{item.putCallLabel ?? '-'}</TableCell>
                  <TableCell className="text-right tabular-nums">{formatOptionsNumber(item.strikePrice, 0)}</TableCell>
                  <TableCell className="text-right tabular-nums">{formatOptionsNumber(item.wholeDayClose, 2)}</TableCell>
                  <TableCell className="text-right tabular-nums">{formatOptionsNumber(item.volume, 0)}</TableCell>
                  <TableCell className="text-right tabular-nums">{formatOptionsNumber(item.openInterest, 0)}</TableCell>
                  <TableCell className="text-right tabular-nums">{formatOptionsNumber(item.impliedVolatility, 2)}</TableCell>
                </TableRow>
              );
            })}
            {shouldVirtualize && virtual.paddingBottom > 0 ? (
              <TableRow>
                <TableCell colSpan={8} className="p-0" style={{ height: virtual.paddingBottom }} />
              </TableRow>
            ) : null}
          </TableBody>
        </Table>
      </div>
    );
  }

  return (
    <Card className="glass-panel overflow-hidden">
      <CardHeader className="border-b border-border/30 pb-3">
        <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
          <div>
            <CardTitle className="text-base">Options Chain</CardTitle>
            <p className="mt-1 text-xs text-muted-foreground">
              {isLoading ? 'Resolving date and loading contracts...' : `${filteredItems.length} filtered contracts from ${totalCount} total`}
            </p>
          </div>
          <button
            type="button"
            className="inline-flex items-center gap-2 rounded-md border px-3 py-2 text-xs transition-colors hover:bg-accent/50"
            onClick={onRefresh}
          >
            <RefreshCcw className={cn('h-3.5 w-3.5', isFetching && 'animate-spin')} />
            Refresh
          </button>
        </div>
      </CardHeader>
      <CardContent className="p-0">{content}</CardContent>
    </Card>
  );
}

export function N225OptionsDetailCard({ selectedItem }: { selectedItem: N225OptionItem | null }) {
  return (
    <Card className="glass-panel">
      <CardHeader className="pb-3">
        <CardTitle className="text-base">Contract Detail</CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        {selectedItem ? (
          <>
            <div className="rounded-xl border border-border/40 bg-muted/20 p-4">
              <div className="text-xs uppercase tracking-wider text-muted-foreground">Selected Contract</div>
              <div className="mt-2 text-xl font-semibold">{selectedItem.code}</div>
              <div className="mt-1 text-sm text-muted-foreground">
                {selectedItem.contractMonth ?? '-'} / {selectedItem.putCallLabel ?? '-'} / strike{' '}
                {formatOptionsNumber(selectedItem.strikePrice, 0)}
              </div>
            </div>

            <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-1">
              <div className="rounded-xl border border-border/40 p-4">
                <div className="text-xs uppercase tracking-wider text-muted-foreground">Pricing</div>
                <dl className="mt-3 grid grid-cols-2 gap-y-2 text-sm">
                  <dt className="text-muted-foreground">Whole Day</dt>
                  <dd className="text-right tabular-nums">{formatOptionsNumber(selectedItem.wholeDayClose, 2)}</dd>
                  <dt className="text-muted-foreground">Settlement</dt>
                  <dd className="text-right tabular-nums">{formatOptionsNumber(selectedItem.settlementPrice, 2)}</dd>
                  <dt className="text-muted-foreground">Theoretical</dt>
                  <dd className="text-right tabular-nums">{formatOptionsNumber(selectedItem.theoreticalPrice, 3)}</dd>
                  <dt className="text-muted-foreground">Underlying</dt>
                  <dd className="text-right tabular-nums">{formatOptionsNumber(selectedItem.underlyingPrice, 2)}</dd>
                </dl>
              </div>

              <div className="rounded-xl border border-border/40 p-4">
                <div className="text-xs uppercase tracking-wider text-muted-foreground">Liquidity</div>
                <dl className="mt-3 grid grid-cols-2 gap-y-2 text-sm">
                  <dt className="text-muted-foreground">Volume</dt>
                  <dd className="text-right tabular-nums">{formatOptionsNumber(selectedItem.volume, 0)}</dd>
                  <dt className="text-muted-foreground">Open Interest</dt>
                  <dd className="text-right tabular-nums">{formatOptionsNumber(selectedItem.openInterest, 0)}</dd>
                  <dt className="text-muted-foreground">Turnover</dt>
                  <dd className="text-right tabular-nums">{formatOptionsNumber(selectedItem.turnoverValue, 0)}</dd>
                  <dt className="text-muted-foreground">Auction Vol.</dt>
                  <dd className="text-right tabular-nums">{formatOptionsNumber(selectedItem.onlyAuctionVolume, 0)}</dd>
                </dl>
              </div>

              <div className="rounded-xl border border-border/40 p-4">
                <div className="text-xs uppercase tracking-wider text-muted-foreground">Risk Metrics</div>
                <dl className="mt-3 grid grid-cols-2 gap-y-2 text-sm">
                  <dt className="text-muted-foreground">IV</dt>
                  <dd className="text-right tabular-nums">{formatOptionsNumber(selectedItem.impliedVolatility, 4)}</dd>
                  <dt className="text-muted-foreground">Base Vol.</dt>
                  <dd className="text-right tabular-nums">{formatOptionsNumber(selectedItem.baseVolatility, 4)}</dd>
                  <dt className="text-muted-foreground">Rate</dt>
                  <dd className="text-right tabular-nums">{formatOptionsNumber(selectedItem.interestRate, 4)}</dd>
                  <dt className="text-muted-foreground">Emergency</dt>
                  <dd className="text-right">{selectedItem.emergencyMarginTriggerLabel ?? '-'}</dd>
                </dl>
              </div>

              <div className="rounded-xl border border-border/40 p-4">
                <div className="text-xs uppercase tracking-wider text-muted-foreground">Calendar</div>
                <dl className="mt-3 grid grid-cols-2 gap-y-2 text-sm">
                  <dt className="text-muted-foreground">Trade Date</dt>
                  <dd className="text-right">{selectedItem.date}</dd>
                  <dt className="text-muted-foreground">Last Trading Day</dt>
                  <dd className="text-right">{selectedItem.lastTradingDay ?? '-'}</dd>
                  <dt className="text-muted-foreground">SQ Day</dt>
                  <dd className="text-right">{selectedItem.specialQuotationDay ?? '-'}</dd>
                </dl>
              </div>
            </div>
          </>
        ) : (
          <div className="flex min-h-64 items-center justify-center text-sm text-muted-foreground">
            Select a contract to inspect details.
          </div>
        )}
      </CardContent>
    </Card>
  );
}
