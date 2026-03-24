import { useNavigate } from '@tanstack/react-router';
import type {
  Options225PutCallFilter,
  Options225SortBy,
  SortOrder,
} from '@trading25/contracts/types/api-response-types';
import { useCallback } from 'react';
import { serializeOptions225Search, validateOptions225Search } from '@/lib/routeSearch';
import { options225Route } from '@/router';

interface Options225RouteState {
  date: string | null;
  putCall: Options225PutCallFilter;
  contractMonth: string | null;
  strikeMin: number | null;
  strikeMax: number | null;
  sortBy: Options225SortBy;
  order: SortOrder;
}

export function useOptions225RouteState(): Options225RouteState & {
  setDate: (date: string | null) => void;
  setPutCall: (putCall: Options225PutCallFilter) => void;
  setContractMonth: (contractMonth: string | null) => void;
  setStrikeRange: (strikeMin: number | null, strikeMax: number | null) => void;
  setSort: (sortBy: Options225SortBy, order: SortOrder) => void;
} {
  const navigate = useNavigate();
  const search = options225Route.useSearch();

  const updateSearch = useCallback(
    (updater: (currentState: Options225RouteState) => Options225RouteState) => {
      void navigate({
        to: '/options-225',
        search: (current: Record<string, unknown>) => {
          const currentSearch = validateOptions225Search(current);
          return serializeOptions225Search(
            updater({
              date: currentSearch.date ?? null,
              putCall: currentSearch.putCall ?? 'all',
              contractMonth: currentSearch.contractMonth ?? null,
              strikeMin: currentSearch.strikeMin ?? null,
              strikeMax: currentSearch.strikeMax ?? null,
              sortBy: currentSearch.sortBy ?? 'openInterest',
              order: currentSearch.order ?? 'desc',
            })
          );
        },
      });
    },
    [navigate]
  );

  return {
    date: search.date ?? null,
    putCall: search.putCall ?? 'all',
    contractMonth: search.contractMonth ?? null,
    strikeMin: search.strikeMin ?? null,
    strikeMax: search.strikeMax ?? null,
    sortBy: search.sortBy ?? 'openInterest',
    order: search.order ?? 'desc',
    setDate: (date) =>
      updateSearch((currentState) => ({
        ...currentState,
        date,
      })),
    setPutCall: (putCall) =>
      updateSearch((currentState) => ({
        ...currentState,
        putCall,
      })),
    setContractMonth: (contractMonth) =>
      updateSearch((currentState) => ({
        ...currentState,
        contractMonth,
      })),
    setStrikeRange: (strikeMin, strikeMax) =>
      updateSearch((currentState) => ({
        ...currentState,
        strikeMin,
        strikeMax,
      })),
    setSort: (sortBy, order) =>
      updateSearch((currentState) => ({
        ...currentState,
        sortBy,
        order,
      })),
  };
}

export function useMigrateOptions225RouteState(): void {}
