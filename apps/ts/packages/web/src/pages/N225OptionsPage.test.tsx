import { fireEvent, render, screen } from '@testing-library/react';
import type {
  Options225PutCallFilter,
  Options225SortBy,
  SortOrder,
} from '@trading25/contracts/types/api-response-types';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { N225OptionsPage } from './N225OptionsPage';

const mockRouteState: {
  date: string | null;
  putCall: Options225PutCallFilter;
  contractMonth: string | null;
  strikeMin: number | null;
  strikeMax: number | null;
  sortBy: Options225SortBy;
  order: SortOrder;
  setDate: ReturnType<typeof vi.fn>;
  setPutCall: ReturnType<typeof vi.fn>;
  setContractMonth: ReturnType<typeof vi.fn>;
  setStrikeRange: ReturnType<typeof vi.fn>;
  setSort: ReturnType<typeof vi.fn>;
} = {
  date: null,
  putCall: 'all',
  contractMonth: null,
  strikeMin: null,
  strikeMax: null,
  sortBy: 'openInterest',
  order: 'desc',
  setDate: vi.fn(),
  setPutCall: vi.fn(),
  setContractMonth: vi.fn(),
  setStrikeRange: vi.fn(),
  setSort: vi.fn(),
};

const mockUseN225Options = vi.fn();
const mockRefetch = vi.fn(() => Promise.resolve());

vi.mock('@/hooks/useOptions225RouteState', () => ({
  useOptions225RouteState: () => mockRouteState,
  useMigrateOptions225RouteState: () => {},
}));

vi.mock('@/hooks/useN225Options', () => ({
  useN225Options: (...args: unknown[]) => mockUseN225Options(...args),
}));

describe('N225OptionsPage', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockRefetch.mockClear();
    mockRouteState.date = null;
    mockRouteState.putCall = 'all';
    mockRouteState.contractMonth = null;
    mockRouteState.strikeMin = null;
    mockRouteState.strikeMax = null;
    mockRouteState.sortBy = 'openInterest';
    mockRouteState.order = 'desc';

    mockUseN225Options.mockReturnValue({
      data: {
        requestedDate: null,
        resolvedDate: '2026-03-18',
        lastUpdated: '2026-03-18T12:00:00Z',
        sourceCallCount: 2,
        availableContractMonths: ['2026-04', '2026-05'],
        items: [
          {
            date: '2026-03-18',
            code: '130060018',
            wholeDayOpen: 10,
            wholeDayHigh: 12,
            wholeDayLow: 9,
            wholeDayClose: 11,
            nightSessionOpen: null,
            nightSessionHigh: null,
            nightSessionLow: null,
            nightSessionClose: null,
            daySessionOpen: 10,
            daySessionHigh: 12,
            daySessionLow: 9,
            daySessionClose: 11,
            volume: 20,
            openInterest: 110,
            turnoverValue: 500000,
            contractMonth: '2026-04',
            strikePrice: 34000,
            onlyAuctionVolume: 1,
            emergencyMarginTriggerDivision: '002',
            emergencyMarginTriggerLabel: 'settlement_price_calculation',
            putCallDivision: '1',
            putCallLabel: 'put',
            lastTradingDay: '2026-04-09',
            specialQuotationDay: '2026-04-10',
            settlementPrice: 11,
            theoreticalPrice: 10.8,
            baseVolatility: 18,
            underlyingPrice: 37450,
            impliedVolatility: 20.5,
            interestRate: 0.12,
          },
          {
            date: '2026-03-18',
            code: '130060019',
            wholeDayOpen: 14,
            wholeDayHigh: 16,
            wholeDayLow: 13,
            wholeDayClose: 15,
            nightSessionOpen: null,
            nightSessionHigh: null,
            nightSessionLow: null,
            nightSessionClose: null,
            daySessionOpen: 14,
            daySessionHigh: 16,
            daySessionLow: 13,
            daySessionClose: 15,
            volume: 30,
            openInterest: 120,
            turnoverValue: 800000,
            contractMonth: '2026-05',
            strikePrice: 35000,
            onlyAuctionVolume: 0,
            emergencyMarginTriggerDivision: '001',
            emergencyMarginTriggerLabel: 'emergency_margin_triggered',
            putCallDivision: '2',
            putCallLabel: 'call',
            lastTradingDay: '2026-05-14',
            specialQuotationDay: '2026-05-15',
            settlementPrice: 15,
            theoreticalPrice: 14.5,
            baseVolatility: 17.5,
            underlyingPrice: 37480,
            impliedVolatility: 19.2,
            interestRate: 0.11,
          },
        ],
        summary: {
          totalCount: 2,
          putCount: 1,
          callCount: 1,
          totalVolume: 50,
          totalOpenInterest: 230,
          strikePriceRange: { min: 34000, max: 35000 },
          underlyingPriceRange: { min: 37450, max: 37480 },
          settlementPriceRange: { min: 11, max: 15 },
        },
      },
      isLoading: false,
      error: null,
      refetch: mockRefetch,
      isFetching: false,
    });
  });

  it('renders explorer summary and table', () => {
    render(<N225OptionsPage />);

    expect(screen.getByRole('heading', { name: 'N225 Options' })).toBeInTheDocument();
    expect(screen.getByText('Resolved Date')).toBeInTheDocument();
    expect(screen.getByText('130060018')).toBeInTheDocument();
    expect(screen.getAllByText('130060019').length).toBeGreaterThan(0);
    expect(screen.getByText('Filtered Contracts')).toBeInTheDocument();
    expect(screen.getByText('Contract Detail')).toBeInTheDocument();
    expect(screen.getByText('2026-05 / call / strike 35,000')).toBeInTheDocument();
  });

  it('updates strike range via route state setters', () => {
    render(<N225OptionsPage />);

    fireEvent.change(screen.getByLabelText('Strike Min'), { target: { value: '34500' } });
    expect(mockRouteState.setStrikeRange).toHaveBeenCalledWith(34500, null);
  });

  it('renders loading state while resolving data', () => {
    mockUseN225Options.mockReturnValue({
      data: null,
      isLoading: true,
      error: null,
      refetch: mockRefetch,
      isFetching: false,
    });

    render(<N225OptionsPage />);

    expect(screen.getByText('Resolving date and loading contracts...')).toBeInTheDocument();
  });

  it('renders error state for failed requests', () => {
    mockUseN225Options.mockReturnValue({
      data: null,
      isLoading: false,
      error: new Error('Gateway timeout'),
      refetch: mockRefetch,
      isFetching: false,
    });

    render(<N225OptionsPage />);

    expect(screen.getByText('Failed to load N225 options data')).toBeInTheDocument();
    expect(screen.getByText('Gateway timeout')).toBeInTheDocument();
  });

  it('renders empty state when filters remove all contracts', () => {
    mockRouteState.putCall = 'put';
    mockUseN225Options.mockReturnValue({
      data: {
        requestedDate: null,
        resolvedDate: '2026-03-18',
        lastUpdated: '2026-03-18T12:00:00Z',
        sourceCallCount: 1,
        availableContractMonths: ['2026-05'],
        items: [
          {
            date: '2026-03-18',
            code: '130060019',
            wholeDayOpen: 14,
            wholeDayHigh: 16,
            wholeDayLow: 13,
            wholeDayClose: 15,
            nightSessionOpen: null,
            nightSessionHigh: null,
            nightSessionLow: null,
            nightSessionClose: null,
            daySessionOpen: 14,
            daySessionHigh: 16,
            daySessionLow: 13,
            daySessionClose: 15,
            volume: 30,
            openInterest: 120,
            turnoverValue: 800000,
            contractMonth: '2026-05',
            strikePrice: 35000,
            onlyAuctionVolume: 0,
            emergencyMarginTriggerDivision: '001',
            emergencyMarginTriggerLabel: 'emergency_margin_triggered',
            putCallDivision: '2',
            putCallLabel: 'call',
            lastTradingDay: '2026-05-14',
            specialQuotationDay: '2026-05-15',
            settlementPrice: 15,
            theoreticalPrice: 14.5,
            baseVolatility: 17.5,
            underlyingPrice: 37480,
            impliedVolatility: 19.2,
            interestRate: 0.11,
          },
        ],
        summary: {
          totalCount: 1,
          putCount: 0,
          callCount: 1,
          totalVolume: 30,
          totalOpenInterest: 120,
          strikePriceRange: { min: 35000, max: 35000 },
          underlyingPriceRange: { min: 37480, max: 37480 },
          settlementPriceRange: { min: 15, max: 15 },
        },
      },
      isLoading: false,
      error: null,
      refetch: mockRefetch,
      isFetching: false,
    });

    render(<N225OptionsPage />);

    expect(screen.getByText('No contracts match the current filters')).toBeInTheDocument();
  });

  it('applies call-side contract month and strike filters', () => {
    mockRouteState.putCall = 'call';
    mockRouteState.contractMonth = '2026-05';
    mockRouteState.strikeMin = 34900;
    mockRouteState.strikeMax = 35050;
    mockRouteState.sortBy = 'wholeDayClose';
    mockRouteState.order = 'asc';
    mockUseN225Options.mockReturnValue({
      data: {
        requestedDate: null,
        resolvedDate: '2026-03-18',
        lastUpdated: '2026-03-18T12:00:00Z',
        sourceCallCount: 1,
        availableContractMonths: ['2026-04', '2026-05', '2026-06'],
        items: [
          {
            date: '2026-03-18',
            code: 'put-filtered',
            wholeDayOpen: 8,
            wholeDayHigh: 9,
            wholeDayLow: 7,
            wholeDayClose: 8,
            nightSessionOpen: null,
            nightSessionHigh: null,
            nightSessionLow: null,
            nightSessionClose: null,
            daySessionOpen: 8,
            daySessionHigh: 9,
            daySessionLow: 7,
            daySessionClose: 8,
            volume: 5,
            openInterest: 10,
            turnoverValue: 100,
            contractMonth: '2026-05',
            strikePrice: 35000,
            onlyAuctionVolume: 0,
            emergencyMarginTriggerDivision: '001',
            emergencyMarginTriggerLabel: 'emergency_margin_triggered',
            putCallDivision: '1',
            putCallLabel: 'put',
            lastTradingDay: '2026-05-14',
            specialQuotationDay: '2026-05-15',
            settlementPrice: 8,
            theoreticalPrice: 8,
            baseVolatility: 19,
            underlyingPrice: 37480,
            impliedVolatility: 21,
            interestRate: 0.1,
          },
          {
            date: '2026-03-18',
            code: 'month-filtered',
            wholeDayOpen: 11,
            wholeDayHigh: 12,
            wholeDayLow: 10,
            wholeDayClose: 11,
            nightSessionOpen: null,
            nightSessionHigh: null,
            nightSessionLow: null,
            nightSessionClose: null,
            daySessionOpen: 11,
            daySessionHigh: 12,
            daySessionLow: 10,
            daySessionClose: 11,
            volume: 12,
            openInterest: 20,
            turnoverValue: 200,
            contractMonth: '2026-04',
            strikePrice: 35000,
            onlyAuctionVolume: 0,
            emergencyMarginTriggerDivision: '001',
            emergencyMarginTriggerLabel: 'emergency_margin_triggered',
            putCallDivision: '2',
            putCallLabel: 'call',
            lastTradingDay: '2026-04-09',
            specialQuotationDay: '2026-04-10',
            settlementPrice: 11,
            theoreticalPrice: 10.5,
            baseVolatility: 18,
            underlyingPrice: 37480,
            impliedVolatility: 19,
            interestRate: 0.1,
          },
          {
            date: '2026-03-18',
            code: 'strike-min-filtered',
            wholeDayOpen: 13,
            wholeDayHigh: 14,
            wholeDayLow: 12,
            wholeDayClose: 13,
            nightSessionOpen: null,
            nightSessionHigh: null,
            nightSessionLow: null,
            nightSessionClose: null,
            daySessionOpen: 13,
            daySessionHigh: 14,
            daySessionLow: 12,
            daySessionClose: 13,
            volume: 14,
            openInterest: 30,
            turnoverValue: 300,
            contractMonth: '2026-05',
            strikePrice: 34800,
            onlyAuctionVolume: 0,
            emergencyMarginTriggerDivision: '001',
            emergencyMarginTriggerLabel: 'emergency_margin_triggered',
            putCallDivision: '2',
            putCallLabel: 'call',
            lastTradingDay: '2026-05-14',
            specialQuotationDay: '2026-05-15',
            settlementPrice: 13,
            theoreticalPrice: 12.5,
            baseVolatility: 17,
            underlyingPrice: 37480,
            impliedVolatility: 18.5,
            interestRate: 0.1,
          },
          {
            date: '2026-03-18',
            code: 'strike-max-filtered',
            wholeDayOpen: 16,
            wholeDayHigh: 17,
            wholeDayLow: 15,
            wholeDayClose: 16,
            nightSessionOpen: null,
            nightSessionHigh: null,
            nightSessionLow: null,
            nightSessionClose: null,
            daySessionOpen: 16,
            daySessionHigh: 17,
            daySessionLow: 15,
            daySessionClose: 16,
            volume: 16,
            openInterest: 40,
            turnoverValue: 400,
            contractMonth: '2026-05',
            strikePrice: 35200,
            onlyAuctionVolume: 0,
            emergencyMarginTriggerDivision: '001',
            emergencyMarginTriggerLabel: 'emergency_margin_triggered',
            putCallDivision: '2',
            putCallLabel: 'call',
            lastTradingDay: '2026-05-14',
            specialQuotationDay: '2026-05-15',
            settlementPrice: 16,
            theoreticalPrice: 15.5,
            baseVolatility: 17,
            underlyingPrice: 37480,
            impliedVolatility: 18.5,
            interestRate: 0.1,
          },
          {
            date: '2026-03-18',
            code: 'call-matching',
            wholeDayOpen: 14,
            wholeDayHigh: 15,
            wholeDayLow: 13,
            wholeDayClose: 14,
            nightSessionOpen: null,
            nightSessionHigh: null,
            nightSessionLow: null,
            nightSessionClose: null,
            daySessionOpen: 14,
            daySessionHigh: 15,
            daySessionLow: 13,
            daySessionClose: 14,
            volume: 18,
            openInterest: null,
            turnoverValue: 500,
            contractMonth: '2026-05',
            strikePrice: 35000,
            onlyAuctionVolume: 0,
            emergencyMarginTriggerDivision: '001',
            emergencyMarginTriggerLabel: null,
            putCallDivision: '2',
            putCallLabel: 'call',
            lastTradingDay: null,
            specialQuotationDay: null,
            settlementPrice: 14,
            theoreticalPrice: 13.5,
            baseVolatility: 16.5,
            underlyingPrice: 37480,
            impliedVolatility: 18.1,
            interestRate: 0.1,
          },
        ],
        summary: {
          totalCount: 5,
          putCount: 1,
          callCount: 4,
          totalVolume: 65,
          totalOpenInterest: 100,
          strikePriceRange: { min: 34800, max: 35200 },
          underlyingPriceRange: { min: 37480, max: 37480 },
          settlementPriceRange: { min: 8, max: 16 },
        },
      },
      isLoading: false,
      error: null,
      refetch: mockRefetch,
      isFetching: false,
    });

    render(<N225OptionsPage />);

    expect(screen.getByText('1 filtered contracts from 5 total')).toBeInTheDocument();
    expect(screen.getByText('All-chain: 100')).toBeInTheDocument();
    expect(screen.getByText('2026-05 / call / strike 35,000')).toBeInTheDocument();
    expect(screen.getByText('0 put / 1 call')).toBeInTheDocument();
  });

  it('resets filters and triggers manual refetch', () => {
    render(<N225OptionsPage />);

    fireEvent.click(screen.getByRole('button', { name: 'Refresh' }));
    fireEvent.click(screen.getByRole('button', { name: 'Reset' }));

    expect(mockRefetch).toHaveBeenCalledTimes(1);
    expect(mockRouteState.setDate).toHaveBeenCalledWith(null);
    expect(mockRouteState.setPutCall).toHaveBeenCalledWith('all');
    expect(mockRouteState.setContractMonth).toHaveBeenCalledWith(null);
    expect(mockRouteState.setStrikeRange).toHaveBeenCalledWith(null, null);
    expect(mockRouteState.setSort).toHaveBeenCalledWith('openInterest', 'desc');
  });

  it('updates the detail panel when a different row is selected', () => {
    render(<N225OptionsPage />);

    fireEvent.click(screen.getByRole('row', { name: /130060018/i }));

    expect(screen.getByText('2026-04 / put / strike 34,000')).toBeInTheDocument();
    expect(screen.getByText('2026-04-09')).toBeInTheDocument();
  });

  it('shows fetching state on the refresh control', () => {
    mockUseN225Options.mockReturnValue({
      data: {
        requestedDate: null,
        resolvedDate: '2026-03-18',
        lastUpdated: '2026-03-18T12:00:00Z',
        sourceCallCount: 1,
        availableContractMonths: [],
        items: [],
        summary: {
          totalCount: 0,
          putCount: 0,
          callCount: 0,
          totalVolume: 0,
          totalOpenInterest: 0,
          strikePriceRange: { min: null, max: null },
          underlyingPriceRange: { min: null, max: null },
          settlementPriceRange: { min: null, max: null },
        },
      },
      isLoading: false,
      error: null,
      refetch: mockRefetch,
      isFetching: true,
    });

    render(<N225OptionsPage />);

    const refreshIcon = document.querySelector('.animate-spin');
    expect(refreshIcon).toBeTruthy();
  });

  it('falls back to unknown error text for non-Error failures', () => {
    mockUseN225Options.mockReturnValue({
      data: null,
      isLoading: false,
      error: 'boom',
      refetch: mockRefetch,
      isFetching: false,
    });

    render(<N225OptionsPage />);

    expect(screen.getByText('Unknown error')).toBeInTheDocument();
  });
});
