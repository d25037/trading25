import { describe, expect, it } from 'vitest';
import {
  extractLegacyBacktestSearch,
  extractLegacySymbolWorkbenchSearch,
  extractLegacyIndicesSearch,
  extractLegacyPortfolioSearch,
  extractLegacyScreeningSearch,
  getRankingStateFromScreeningSearch,
  getRankingStateFromSearch,
  getScreeningStateFromSearch,
  prunePersistedStoreFields,
  readPersistedStoreState,
  serializeBacktestSearch,
  serializeIndicesSearch,
  serializeOptions225Search,
  serializePortfolioSearch,
  serializeResearchSearch,
  serializeRankingSearch,
  serializeScreeningSearch,
  validateBacktestSearch,
  validateSymbolWorkbenchSearch,
  validateIndicesSearch,
  validateOptions225Search,
  validatePortfolioSearch,
  validateResearchSearch,
  validateRankingSearch,
  validateScreeningSearch,
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
  it('validates and serializes symbol workbench/portfolio search params', () => {
    expect(validateSymbolWorkbenchSearch({ symbol: ' 7203 ' })).toEqual({ symbol: '7203' });
    expect(validateIndicesSearch({ code: ' topix ' })).toEqual({ code: 'topix' });
    expect(validateResearchSearch({ experimentId: ' market/a ', runId: ' 20260405 ' })).toEqual({
      experimentId: 'market/a',
      runId: '20260405',
    });
    expect(validateIndicesSearch({ code: '   ' })).toEqual({});
    expect(validatePortfolioSearch({ tab: 'watchlists', portfolioId: '3', watchlistId: 'bad' })).toEqual({
      tab: 'watchlists',
      portfolioId: 3,
    });
    expect(serializePortfolioSearch({ tab: 'portfolios', portfolioId: null, watchlistId: 9 })).toEqual({
      watchlistId: 9,
    });
    expect(serializeIndicesSearch('   ')).toEqual({});
    expect(serializeResearchSearch({ experimentId: ' market/a ', runId: '   ' })).toEqual({
      experimentId: 'market/a',
    });
    expect(extractLegacySymbolWorkbenchSearch({ selectedSymbol: '6758' })).toEqual({ symbol: '6758' });
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

  it('roundtrips screening search state with defaults', () => {
    const search = validateScreeningSearch({
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

    const state = getScreeningStateFromSearch(search);

    expect(state.activeSubTab).toBe('inSessionScreening');
    expect(state.preOpenScreeningParams.entry_decidability).toBe('pre_open_decidable');
    expect(state.preOpenScreeningParams.markets).toBeUndefined();
    expect(state.inSessionScreeningParams.entry_decidability).toBe('requires_same_session_observation');
    expect(state.inSessionScreeningParams.sortBy).toBe('matchStrategyCount');

    expect(serializeScreeningSearch(state)).toEqual({
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
      extractLegacyScreeningSearch({
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

  it('omits auto screening markets from serialized screening search', () => {
    const state = getScreeningStateFromSearch(validateScreeningSearch({}));

    expect(state.preOpenScreeningParams.markets).toBeUndefined();
    expect(state.inSessionScreeningParams.markets).toBeUndefined();
    expect(serializeScreeningSearch(state)).toEqual({});
  });

  it('serializes non-default ranking and fundamental filters', () => {
    const search = validateScreeningSearch({
      rankingMarkets: '0111',
      rankingLookbackDays: '15',
      rankingPeriodDays: '60',
      rankingTopix100Metric: 'price_vs_sma20_gap',
      rankingTopix100SmaWindow: '100',
      rankingTopix100PriceBucket: 'q10',
      rankingTopix100StudyMode: 'intraday',
      rankingTopix100SortBy: 'longScore5d',
      rankingTopix100SortOrder: 'desc',
      fundamentalMarkets: '0112',
      forecastAboveRecentFyActuals: true,
      forecastLookbackFyCount: '7',
    });

    const state = getScreeningStateFromSearch(search);

    expect(state.rankingParams.topix100StudyMode).toBe('intraday');
    expect(state.rankingParams.topix100SortBy).toBe('intradayScore');

    expect(serializeScreeningSearch(state)).toEqual({
      rankingMarkets: '0111',
      rankingLookbackDays: 15,
      rankingPeriodDays: 60,
      rankingTopix100SmaWindow: 100,
      rankingTopix100PriceBucket: 'q10',
      rankingTopix100StudyMode: 'intraday',
      fundamentalMarkets: '0112',
      forecastAboveRecentFyActuals: true,
      forecastLookbackFyCount: 7,
    });
  });

  it('normalizes topix100 default sort when study mode changes through url state', () => {
    const rankingSearch = validateRankingSearch({
      dailyView: 'topix100',
      rankingTopix100StudyMode: 'intraday',
    });

    const rankingState = getRankingStateFromSearch(rankingSearch);

    expect(rankingState.rankingParams.topix100StudyMode).toBe('intraday');
    expect(rankingState.rankingParams.topix100SortBy).toBe('intradayScore');
    expect(serializeRankingSearch(rankingState)).toEqual({
      dailyView: 'topix100',
      rankingTopix100StudyMode: 'intraday',
    });
  });

  it('migrates legacy topix100 sma20 metric urls to the new metric plus sma window', () => {
    const search = validateScreeningSearch({
      rankingTopix100Metric: 'price_sma_20_80',
      rankingTopix100SmaWindow: '20',
    });

    expect(search).toEqual({
      rankingTopix100Metric: 'price_sma_20_80',
      rankingTopix100SmaWindow: 20,
    });

    const legacySearch = validateScreeningSearch({
      rankingTopix100Metric: 'price_vs_sma20_gap',
    });

    expect(legacySearch).toEqual({
      rankingTopix100Metric: 'price_vs_sma_gap',
      rankingTopix100SmaWindow: 20,
    });
  });

  it('migrates legacy q456 topix100 bucket urls to q234', () => {
    const search = validateRankingSearch({
      rankingTopix100PriceBucket: 'q456',
    });

    expect(search).toEqual({
      rankingTopix100PriceBucket: 'q234',
    });
  });

  it('roundtrips ranking route state and maps screening ranking tabs', () => {
    const rankingSearch = validateRankingSearch({
      tab: 'fundamentalRanking',
      dailyView: 'topix100',
      rankingMarkets: '0111',
      rankingLookbackDays: '15',
      rankingTopix100StudyMode: 'intraday',
      rankingTopix100Metric: 'price_vs_sma_gap',
      rankingTopix100SmaWindow: '100',
      rankingTopix100PriceBucket: 'q1',
      rankingTopix100SortBy: 'shortScore1d',
      rankingTopix100SortOrder: 'desc',
      fundamentalMarkets: '0112',
      forecastAboveRecentFyActuals: true,
    });

    const rankingState = getRankingStateFromSearch(rankingSearch);
    expect(rankingState.activeSubTab).toBe('fundamentalRanking');
    expect(rankingState.activeDailyView).toBe('topix100');
    expect(serializeRankingSearch(rankingState)).toEqual({
      tab: 'fundamentalRanking',
      dailyView: 'topix100',
      rankingMarkets: '0111',
      rankingLookbackDays: 15,
      rankingTopix100StudyMode: 'intraday',
      rankingTopix100SmaWindow: 100,
      rankingTopix100PriceBucket: 'q1',
      rankingTopix100SortBy: 'intradayShortRank',
      fundamentalMarkets: '0112',
      forecastAboveRecentFyActuals: true,
    });

    const screeningSearch = validateScreeningSearch({
      tab: 'fundamentalRanking',
      rankingMarkets: 'growth',
      fundamentalLimit: '30',
    });

    expect(getRankingStateFromScreeningSearch(screeningSearch)).toEqual({
      activeSubTab: 'fundamentalRanking',
      activeDailyView: 'stocks',
      rankingParams: expect.objectContaining({
        markets: 'growth',
      }),
      fundamentalRankingParams: expect.objectContaining({
        limit: 30,
      }),
    });
  });

  it('roundtrips value composite ranking route state', () => {
    const rankingSearch = validateRankingSearch({
      tab: 'valueComposite',
      valueDate: '2026-04-24',
      valueMarkets: 'standard',
      valueLimit: '100',
      valueScoreMethod: 'equal_weight',
    });

    const rankingState = getRankingStateFromSearch(rankingSearch);

    expect(rankingState.activeSubTab).toBe('valueComposite');
    expect(rankingState.valueCompositeRankingParams).toEqual({
      date: '2026-04-24',
      markets: 'standard',
      limit: 100,
      scoreMethod: 'equal_weight',
    });
    expect(serializeRankingSearch(rankingState)).toEqual({
      tab: 'valueComposite',
      valueDate: '2026-04-24',
      valueLimit: 100,
      valueScoreMethod: 'equal_weight',
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

  it('drops invalid screening and backtest search values', () => {
    expect(
      validateScreeningSearch({
        tab: 'unknown',
        screeningRecentDays: '0',
        screeningOrder: 'invalid',
        forecastAboveRecentFyActuals: 'not-bool',
        forecastLookbackFyCount: '-1',
      })
    ).toEqual({});

    expect(
      validateRankingSearch({
        tab: 'ranking',
        dailyView: 'invalid',
      })
    ).toEqual({
      tab: 'ranking',
    });

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
      extractLegacyScreeningSearch({
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
