import { describe, expect, it } from 'vitest';
import {
  extractLegacyBacktestSearch,
  extractLegacyIndicesSearch,
  extractLegacyPortfolioSearch,
  extractLegacyScreeningSearch,
  extractLegacySymbolWorkbenchSearch,
  getRankingStateFromSearch,
  getScreeningStateFromSearch,
  prunePersistedStoreFields,
  readPersistedStoreState,
  serializeBacktestSearch,
  serializeIndicesSearch,
  serializeOptions225Search,
  serializePortfolioSearch,
  serializeRankingSearch,
  serializeResearchSearch,
  serializeScreeningSearch,
  validateBacktestSearch,
  validateIndicesSearch,
  validateOptions225Search,
  validatePortfolioSearch,
  validateRankingSearch,
  validateResearchSearch,
  validateScreeningSearch,
  validateSymbolWorkbenchSearch,
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
    });

    expect(
      extractLegacyScreeningSearch({
        activeSubTab: 'fundamentalRanking',
        screeningParams: { strategies: 'production/a' },
        sameDayScreeningParams: { strategies: 'production/b' },
        rankingParams: { lookbackDays: 5 },
      })
    ).toEqual({
      preOpenStrategies: 'production/a',
      inSessionStrategies: 'production/b',
      rankingLookbackDays: 5,
    });
  });

  it('omits auto screening markets from serialized screening search', () => {
    const state = getScreeningStateFromSearch(validateScreeningSearch({}));

    expect(state.preOpenScreeningParams.markets).toBeUndefined();
    expect(state.inSessionScreeningParams.markets).toBeUndefined();
    expect(serializeScreeningSearch(state)).toEqual({});
  });

  it('serializes non-default ranking filters', () => {
    const search = validateScreeningSearch({
      rankingMarkets: '0111',
      rankingLookbackDays: '15',
      rankingPeriodDays: '60',
      rankingLiquidityState: 'overheat',
      rankingSortBy: 'forwardPer',
      rankingOrder: 'asc',
      rankingForwardEpsDisclosedWithinDays: '126',
    });

    const state = getScreeningStateFromSearch(search);

    expect(serializeScreeningSearch(state)).toEqual({
      rankingMarkets: '0111',
      rankingLookbackDays: 15,
      rankingPeriodDays: 60,
      rankingLiquidityState: 'overheat',
      rankingSortBy: 'forwardPer',
      rankingOrder: 'asc',
      rankingForwardEpsDisclosedWithinDays: 126,
    });
  });

  it('drops removed topix100 ranking url state', () => {
    const rankingSearch = validateRankingSearch({
      dailyView: 'topix100',
      rankingTopix100StudyMode: 'intraday',
      rankingTopix100Metric: 'price_sma_20_80',
      rankingTopix100SmaWindow: '20',
      rankingTopix100PriceBucket: 'q456',
      rankingTopix100SortBy: 'shortScore1d',
    });

    expect(rankingSearch).toEqual({});
    const rankingState = getRankingStateFromSearch(rankingSearch);
    expect(rankingState.activeDailyView).toBe('stocks');
    expect(serializeRankingSearch(rankingState)).toEqual({});
  });

  it('roundtrips ranking route state', () => {
    const rankingSearch = validateRankingSearch({
      dailyView: 'technicalEvents',
      rankingMarkets: '0111',
      rankingLookbackDays: '15',
      rankingPeriodDays: '120',
      rankingTechnicalEventType: 'periodLow',
      rankingLiquidityState: 'distribution_stress',
      rankingSortBy: 'adv60ToFreeFloatPct',
      rankingOrder: 'asc',
      rankingForwardEpsDisclosedWithinDays: '0',
    });

    const rankingState = getRankingStateFromSearch(rankingSearch);
    expect(rankingState.activeDailyView).toBe('technicalEvents');
    expect(serializeRankingSearch(rankingState)).toEqual({
      dailyView: 'technicalEvents',
      rankingMarkets: '0111',
      rankingLookbackDays: 15,
      rankingPeriodDays: 120,
      rankingTechnicalEventType: 'periodLow',
      rankingLiquidityState: 'distribution_stress',
      rankingSortBy: 'adv60ToFreeFloatPct',
      rankingOrder: 'asc',
    });
  });

  it('drops removed ranking tabs and value-composite url state', () => {
    const rankingSearch = validateRankingSearch({
      tab: 'valueComposite',
      valueDate: '2026-04-24',
      valueMarkets: 'standard',
      valueLimit: '100',
      valueProfileId: 'prime_size75_forward_per25',
      valueApplyLiquidityFilter: false,
      valueForwardEpsMode: 'fy',
    });

    const rankingState = getRankingStateFromSearch(rankingSearch);

    expect(rankingState.activeDailyView).toBe('stocks');
    expect(serializeRankingSearch(rankingState)).toEqual({});
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
      })
    ).toEqual({});

    expect(
      validateRankingSearch({
        tab: 'ranking',
        dailyView: 'invalid',
        rankingSortBy: 'invalid',
        rankingOrder: 'sideways',
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
      extractLegacyScreeningSearch({
        activeSubTab: 'not-a-tab',
        screeningParams: 'bad',
        sameDayScreeningParams: null,
        rankingParams: 1,
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
