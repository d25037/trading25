import type { Options225SortBy, SortOrder } from '@trading25/contracts/types/api-response-types';
import type { N225OptionItem } from '@/types/options225';

export interface Options225FilterState {
  putCall: 'all' | 'put' | 'call';
  contractMonth: string | null;
  strikeMin: number | null;
  strikeMax: number | null;
  sortBy: Options225SortBy;
  order: SortOrder;
}

export interface Options225FilteredSummary {
  filteredCount: number;
  putCount: number;
  callCount: number;
  totalOpenInterest: number;
}

export function formatOptionsNumber(value: number | null | undefined, maximumFractionDigits = 2): string {
  if (value === null || value === undefined) {
    return '-';
  }
  return value.toLocaleString(undefined, { maximumFractionDigits });
}

export function formatOptionsRange(min: number | null, max: number | null, maximumFractionDigits = 2): string {
  if (min === null && max === null) {
    return '-';
  }
  return `${formatOptionsNumber(min, maximumFractionDigits)} - ${formatOptionsNumber(max, maximumFractionDigits)}`;
}

export function parseOptionsNumericInput(value: string): number | null {
  if (value.trim().length === 0) {
    return null;
  }
  const parsed = Number.parseFloat(value);
  return Number.isFinite(parsed) ? parsed : null;
}

export function getOptionRowKey(item: N225OptionItem): string {
  return [item.code, item.emergencyMarginTriggerDivision ?? 'none'].join(':');
}

function compareNullableNumber(left: number | null, right: number | null): number {
  if (left === right) return 0;
  if (left === null) return 1;
  if (right === null) return -1;
  return left - right;
}

function compareNullableString(left: string | null, right: string | null): number {
  if (left === right) return 0;
  if (left === null) return 1;
  if (right === null) return -1;
  return left.localeCompare(right);
}

export function buildOptionsComparator(sortBy: Options225SortBy, order: SortOrder) {
  const direction = order === 'asc' ? 1 : -1;

  return (left: N225OptionItem, right: N225OptionItem): number => {
    let result = 0;
    switch (sortBy) {
      case 'volume':
        result = compareNullableNumber(left.volume, right.volume);
        break;
      case 'strikePrice':
        result = compareNullableNumber(left.strikePrice, right.strikePrice);
        break;
      case 'impliedVolatility':
        result = compareNullableNumber(left.impliedVolatility, right.impliedVolatility);
        break;
      case 'wholeDayClose':
        result = compareNullableNumber(left.wholeDayClose, right.wholeDayClose);
        break;
      default:
        result = compareNullableNumber(left.openInterest, right.openInterest);
        break;
    }

    if (result !== 0) {
      return result * direction;
    }

    const contractMonthCompare = compareNullableString(left.contractMonth, right.contractMonth);
    if (contractMonthCompare !== 0) {
      return contractMonthCompare;
    }

    const strikeCompare = compareNullableNumber(left.strikePrice, right.strikePrice);
    if (strikeCompare !== 0) {
      return strikeCompare;
    }

    return compareNullableString(left.code, right.code);
  };
}

export function filterOptionsItems(items: N225OptionItem[], filters: Options225FilterState): N225OptionItem[] {
  const normalizedPutCall = filters.putCall === 'put' ? '1' : filters.putCall === 'call' ? '2' : null;

  return items
    .filter((item) => {
      if (normalizedPutCall !== null && item.putCallDivision !== normalizedPutCall) {
        return false;
      }
      if (filters.contractMonth && item.contractMonth !== filters.contractMonth) {
        return false;
      }
      if (
        typeof filters.strikeMin === 'number' &&
        (item.strikePrice === null || item.strikePrice < filters.strikeMin)
      ) {
        return false;
      }
      if (
        typeof filters.strikeMax === 'number' &&
        (item.strikePrice === null || item.strikePrice > filters.strikeMax)
      ) {
        return false;
      }
      return true;
    })
    .slice()
    .sort(buildOptionsComparator(filters.sortBy, filters.order));
}

export function summarizeFilteredOptions(items: N225OptionItem[]): Options225FilteredSummary {
  const putCount = items.filter((item) => item.putCallDivision === '1').length;
  const callCount = items.filter((item) => item.putCallDivision === '2').length;
  const totalOpenInterest = items.reduce((sum, item) => sum + (item.openInterest ?? 0), 0);

  return {
    filteredCount: items.length,
    putCount,
    callCount,
    totalOpenInterest,
  };
}

export function resolveSelectedOptionRowKey(items: N225OptionItem[], selectedRowKey: string | null): string | null {
  if (items.length === 0) {
    return null;
  }

  if (selectedRowKey !== null && items.some((item) => getOptionRowKey(item) === selectedRowKey)) {
    return selectedRowKey;
  }

  const firstItem = items.at(0);
  return firstItem ? getOptionRowKey(firstItem) : null;
}
