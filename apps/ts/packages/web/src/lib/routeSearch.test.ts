import { describe, expect, it } from 'vitest';
import {
  extractLegacyAnalysisSearch,
  extractLegacyBacktestSearch,
  extractLegacyChartsSearch,
  extractLegacyIndicesSearch,
  extractLegacyPortfolioSearch,
  getAnalysisStateFromSearch,
  prunePersistedStoreFields,
  readPersistedStoreState,
  serializeAnalysisSearch,
  serializeBacktestSearch,
  serializeIndicesSearch,
  serializeOptions225Search,
  serializePortfolioSearch,
  validateAnalysisSearch,
  validateBacktestSearch,
  validateChartsSearch,
  validateIndicesSearch,
  validateOptions225Search,
  validatePortfolioSearch,
} from './routeSearch';

function createMemoryStorage(initial: Record<string, string> = {}): Storage {
  const store = new Map(Object.entries(initial));

  return {
    getItem: (key) => store.get(key) ?? null,
    setItem: (key, value) => {
      store.set(key, value);
    },
    removeItem: (key) => {
      store.delete(key);
    },
    clear: () => {
      store.clear();
    },
    key: (index) => Array.from(store.keys())[index] ?? null,
    get length() {
      return store.size;
    },
  } as Storage;
}

describe('routeSearch', () => {
  it('validates and serializes charts/portfolio search params', () => {
    expect(validateChartsSearch({ symbol: ' 7203 ' })).toEqual({ symbol: '7203' });
    expect(validateIndicesSearch({ code: ' topix ' })).toEqual({ code: 'topix' });
    expect(validateIndicesSearch({ code: '   ' })).toEqual({});
    expect(validatePortfolioSearch({ tab: 'watchlists', portfolioId: '3', watchlistId: 'bad' })).toEqual({
      tab: 'watchlists',
      portfolioId: 3,
    });
    expect(serializePortfolioSearch({ tab: 'portfolios', portfolioId: null, watchlistId: 9 })).toEqual({
      watchlistId: 9,
    });
    expect(serializeIndicesSearch('   ')).toEqual({});
    expect(extractLegacyChartsSearch({ selectedSymbol: '6758' })).toEqual({ symbol: '6758' });
    expect(
      extractLegacyPortfolioSearch({
        portfolioSubTab: 'watchlists',
        selectedPortfolioId: '5',
        selectedWatchlistId: 8,
      })
    ).toEqual({
      tab: 'watchlists',
      portfolioId: 5,
      watchlistId: 8,
    });
    expect(extractLegacyIndicesSearch({ selectedIndexCode: 'jasdaq' })).toEqual({ code: 'jasdaq' });
  });

  it('roundtrips analysis search state with defaults', () => {
    const search = validateAnalysisSearch({
      tab: 'sameDayScreening',
      sameDayMarkets: '0113',
      sameDayStrategies: 'production/in_session_strategy',
      sameDayRecentDays: '5',
      sameDaySortBy: 'matchStrategyCount',
      sameDayOrder: 'asc',
      sameDayLimit: '60',
      rankingLimit: '25',
      fundamentalLimit: '30',
      forecastAboveRecentFyActuals: 'true',
    });

    const state = getAnalysisStateFromSearch(search);

    expect(state.activeSubTab).toBe('inSessionScreening');
    expect(state.preOpenScreeningParams.entry_decidability).toBe('pre_open_decidable');
    expect(state.inSessionScreeningParams.entry_decidability).toBe('requires_same_session_observation');
    expect(state.inSessionScreeningParams.sortBy).toBe('matchStrategyCount');

    expect(serializeAnalysisSearch(state)).toEqual({
      tab: 'inSessionScreening',
      inSessionMarkets: '0113',
      inSessionStrategies: 'production/in_session_strategy',
      inSessionRecentDays: 5,
      inSessionSortBy: 'matchStrategyCount',
      inSessionOrder: 'asc',
      inSessionLimit: 60,
      rankingLimit: 25,
      fundamentalLimit: 30,
      forecastAboveRecentFyActuals: true,
    });

    expect(
      extractLegacyAnalysisSearch({
        activeSubTab: 'fundamentalRanking',
        screeningParams: { strategies: 'production/a' },
        sameDayScreeningParams: { strategies: 'production/b' },
        rankingParams: { lookbackDays: 5 },
        fundamentalRankingParams: { forecastLookbackFyCount: 5 },
      })
    ).toEqual({
      tab: 'fundamentalRanking',
      preOpenStrategies: 'production/a',
      inSessionStrategies: 'production/b',
      rankingLookbackDays: 5,
      forecastLookbackFyCount: 5,
    });
  });

  it('serializes non-default ranking and fundamental filters', () => {
    const search = validateAnalysisSearch({
      rankingMarkets: '0111',
      rankingLookbackDays: '15',
      rankingPeriodDays: '60',
      fundamentalMarkets: '0112',
      forecastAboveRecentFyActuals: true,
      forecastLookbackFyCount: '7',
    });

    const state = getAnalysisStateFromSearch(search);

    expect(serializeAnalysisSearch(state)).toEqual({
      rankingMarkets: '0111',
      rankingLookbackDays: 15,
      rankingPeriodDays: 60,
      fundamentalMarkets: '0112',
      forecastAboveRecentFyActuals: true,
      forecastLookbackFyCount: 7,
    });
  });

  it('validates and serializes backtest route state', () => {
    const search = validateBacktestSearch({
      tab: 'lab',
      strategy: 'production/alpha',
      resultJobId: 'job-1',
      dataset: 'snapshot-a',
      labType: 'optimize',
    });

    expect(search).toEqual({
      tab: 'lab',
      strategy: 'production/alpha',
      resultJobId: 'job-1',
      dataset: 'snapshot-a',
      labType: 'optimize',
    });

    expect(
      serializeBacktestSearch({
        activeSubTab: 'results',
        selectedStrategy: 'production/alpha',
        selectedResultJobId: 'job-1',
        selectedDatasetName: 'snapshot-a',
        activeLabType: 'improve',
      })
    ).toEqual({
      tab: 'results',
      strategy: 'production/alpha',
      resultJobId: 'job-1',
      dataset: 'snapshot-a',
      labType: 'improve',
    });

    expect(
      extractLegacyBacktestSearch({
        activeSubTab: 'lab',
        selectedStrategy: 'production/beta',
        selectedResultJobId: 'job-9',
        selectedDatasetName: 'snapshot-b',
        activeLabType: 'evolve',
      })
    ).toEqual({
      tab: 'lab',
      strategy: 'production/beta',
      resultJobId: 'job-9',
      dataset: 'snapshot-b',
      labType: 'evolve',
    });
  });

  it('reads and prunes persisted legacy state containers', () => {
    const storage = createMemoryStorage({
      'trading25-chart-store': JSON.stringify({
        state: {
          selectedSymbol: '7203',
          settings: { visibleBars: 180 },
        },
        version: 0,
      }),
    });

    expect(readPersistedStoreState(storage, 'trading25-chart-store')).toEqual({
      selectedSymbol: '7203',
      settings: { visibleBars: 180 },
    });

    prunePersistedStoreFields(storage, 'trading25-chart-store', ['selectedSymbol']);

    expect(JSON.parse(storage.getItem('trading25-chart-store') ?? '{}')).toEqual({
      state: {
        settings: { visibleBars: 180 },
      },
      version: 0,
    });
  });

  it('prunes persisted raw records without removing remaining fields', () => {
    const storage = createMemoryStorage({
      rawRecord: JSON.stringify({
        selectedSymbol: '7203',
        settings: { visibleBars: 180 },
      }),
    });

    prunePersistedStoreFields(storage, 'rawRecord', ['selectedSymbol']);

    expect(JSON.parse(storage.getItem('rawRecord') ?? '{}')).toEqual({
      settings: { visibleBars: 180 },
    });
  });

  it('handles malformed or non-record persisted state safely', () => {
    const storage = createMemoryStorage({
      invalid: '{',
      plain: JSON.stringify(['not-a-record']),
      broken: '{',
      rawRecord: JSON.stringify({ selectedSymbol: '7203' }),
    });

    expect(readPersistedStoreState(storage, 'missing')).toBeNull();
    expect(readPersistedStoreState(storage, 'invalid')).toBeNull();
    expect(readPersistedStoreState(storage, 'plain')).toBeNull();
    expect(readPersistedStoreState(storage, 'rawRecord')).toEqual({ selectedSymbol: '7203' });

    prunePersistedStoreFields(storage, 'broken', ['selectedSymbol']);
    expect(storage.getItem('broken')).toBeNull();

    prunePersistedStoreFields(storage, 'rawRecord', ['selectedSymbol']);
    expect(storage.getItem('rawRecord')).toBeNull();
  });

  it('drops invalid analysis and backtest search values', () => {
    expect(
      validateAnalysisSearch({
        tab: 'unknown',
        screeningRecentDays: '0',
        screeningOrder: 'invalid',
        forecastAboveRecentFyActuals: 'not-bool',
        forecastLookbackFyCount: '-1',
      })
    ).toEqual({});

    expect(
      validateBacktestSearch({
        tab: 'invalid',
        strategy: '  ',
        resultJobId: '',
        dataset: ' snapshot-a ',
        labType: 'bogus',
      })
    ).toEqual({
      dataset: 'snapshot-a',
    });
  });

  it('extracts legacy searches with safe defaults when persisted payloads are malformed', () => {
    expect(
      extractLegacyAnalysisSearch({
        activeSubTab: 'not-a-tab',
        screeningParams: 'bad',
        sameDayScreeningParams: null,
        rankingParams: 1,
        fundamentalRankingParams: false,
      })
    ).toEqual({});

    expect(
      extractLegacyBacktestSearch({
        activeSubTab: 'not-a-tab',
        selectedStrategy: 123,
        selectedResultJobId: [],
        selectedDatasetName: null,
        activeLabType: 'unknown',
      })
    ).toEqual({});
  });

  it('validates and normalizes options-225 search params', () => {
    expect(
      validateOptions225Search({
        date: '2026-03-18',
        putCall: 'put',
        contractMonth: '2026-04',
        strikeMin: '34000',
        strikeMax: 36000,
        sortBy: 'volume',
        order: 'asc',
      })
    ).toEqual({
      date: '2026-03-18',
      putCall: 'put',
      contractMonth: '2026-04',
      strikeMin: 34000,
      strikeMax: 36000,
      sortBy: 'volume',
      order: 'asc',
    });
  });

  it('omits default values when serializing options-225 search params', () => {
    expect(
      serializeOptions225Search({
        date: null,
        putCall: 'all',
        contractMonth: null,
        strikeMin: null,
        strikeMax: null,
        sortBy: 'openInterest',
        order: 'desc',
      })
    ).toEqual({});
  });
});
