import { describe, expect, it } from 'vitest';
import {
  getRankingStateFromSearch,
  getScreeningStateFromSearch,
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
  });

  it('roundtrips screening search state with defaults', () => {
    const search = validateScreeningSearch({
      tab: 'inSessionScreening',
      inSessionMarkets: '0113',
      inSessionStrategies: 'production/in_session_strategy',
      inSessionRecentDays: '5',
      inSessionSortBy: 'matchStrategyCount',
      inSessionOrder: 'asc',
      inSessionLimit: '60',
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
    });

  });

  it('omits auto screening markets from serialized screening search', () => {
    const state = getScreeningStateFromSearch(validateScreeningSearch({}));

    expect(state.preOpenScreeningParams.markets).toBeUndefined();
    expect(state.inSessionScreeningParams.markets).toBeUndefined();
    expect(serializeScreeningSearch(state)).toEqual({});
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

  });

  it('drops invalid screening and backtest search values', () => {
    expect(
      validateScreeningSearch({
        tab: 'unknown',
        preOpenRecentDays: '0',
        preOpenOrder: 'invalid',
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
