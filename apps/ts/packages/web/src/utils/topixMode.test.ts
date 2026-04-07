import { describe, expect, it } from 'vitest'
import {
  buildTopixModeStateKey,
  buildTopixMultiTimeframeModeAnalysis,
  formatTopixModeStateLabel,
  getTopixModeStateCopy,
} from './topixMode'

describe('buildTopixMultiTimeframeModeAnalysis', () => {
  it('computes streak-based short and long states with streak-span lengths', () => {
    const analysis = buildTopixMultiTimeframeModeAnalysis(
      [
        { date: '2026-01-01', close: 100 },
        { date: '2026-01-02', close: 103 },
        { date: '2026-01-03', close: 105 },
        { date: '2026-01-04', close: 104 },
        { date: '2026-01-05', close: 102 },
        { date: '2026-01-06', close: 103 },
        { date: '2026-01-07', close: 104 },
        { date: '2026-01-08', close: 100 },
        { date: '2026-01-09', close: 99 },
      ],
      { shortWindowStreaks: 2, longWindowStreaks: 3 }
    )

    expect(analysis.minimumRequiredStreaks).toBe(3)
    expect(analysis.streakCount).toBe(4)
    expect(analysis.points).toHaveLength(2)
    expect(analysis.points[0]).toMatchObject({
      date: '2026-01-07',
      segmentStartDate: '2026-01-06',
      segmentEndDate: '2026-01-07',
      shortMode: 'bearish',
      longMode: 'bullish',
      shortDominantSegmentEndDate: '2026-01-05',
      longDominantSegmentEndDate: '2026-01-03',
      shortModeSpanStreakCount: 1,
      longModeSpanStreakCount: 1,
      stateKey: 'long_bullish__short_bearish',
      stateSegmentLength: 1,
    })
    expect(analysis.currentPoint).toMatchObject({
      date: '2026-01-09',
      shortMode: 'bearish',
      longMode: 'bearish',
      shortDominantSegmentEndDate: '2026-01-09',
      longDominantSegmentEndDate: '2026-01-09',
      shortModeSpanStreakCount: 2,
      longModeSpanStreakCount: 1,
      stateKey: 'long_bearish__short_bearish',
      stateSegmentLength: 1,
    })
  })

  it('matches the Python tie-breaking rule by keeping the earliest dominant streak shock', () => {
    const analysis = buildTopixMultiTimeframeModeAnalysis(
      [
        { date: '2026-01-01', close: 100 },
        { date: '2026-01-02', close: 110 },
        { date: '2026-01-03', close: 99 },
      ],
      { shortWindowStreaks: 2, longWindowStreaks: 2 }
    )

    expect(analysis.currentPoint).toMatchObject({
      date: '2026-01-03',
      shortMode: 'bullish',
      longMode: 'bullish',
      shortDominantSegmentEndDate: '2026-01-02',
      longDominantSegmentEndDate: '2026-01-02',
    })
  })

  it('returns an unavailable analysis when streak history is shorter than the longest window', () => {
    const analysis = buildTopixMultiTimeframeModeAnalysis(
      [
        { date: '2026-01-01', close: 100 },
        { date: '2026-01-02', close: 101 },
        { date: '2026-01-03', close: 100 },
      ],
      { shortWindowStreaks: 2, longWindowStreaks: 4 }
    )

    expect(analysis.points).toEqual([])
    expect(analysis.currentPoint).toBeNull()
    expect(analysis.minimumRequiredStreaks).toBe(4)
    expect(analysis.streakCount).toBe(2)
  })

  it('handles flat streaks explicitly and keeps them inside the streak history', () => {
    const analysis = buildTopixMultiTimeframeModeAnalysis(
      [
        { date: '2026-01-01', close: 100 },
        { date: '2026-01-02', close: 100 },
        { date: '2026-01-03', close: 103 },
        { date: '2026-01-04', close: 103 },
        { date: '2026-01-05', close: 99 },
      ],
      { shortWindowStreaks: 2, longWindowStreaks: 3 }
    )

    expect(analysis.streakCount).toBe(4)
    expect(analysis.currentPoint).toMatchObject({
      baseStreakMode: 'bearish',
      shortMode: 'bearish',
      longMode: 'bearish',
    })
  })

  it('rejects non-positive streak windows', () => {
    expect(() =>
      buildTopixMultiTimeframeModeAnalysis(
        [
          { date: '2026-01-01', close: 100 },
          { date: '2026-01-02', close: 101 },
        ],
        { shortWindowStreaks: 0, longWindowStreaks: 2 }
      )
    ).toThrow('Window streaks must be positive integers')
  })
})

describe('topix mode helpers', () => {
  it('formats state labels consistently', () => {
    const stateKey = buildTopixModeStateKey('bullish', 'bearish')
    expect(stateKey).toBe('long_bullish__short_bearish')
    expect(formatTopixModeStateLabel(stateKey)).toBe('Long Bullish / Short Bearish')
  })

  it('returns the streak-state narrative copy used in the UI', () => {
    expect(getTopixModeStateCopy('long_bearish__short_bearish')).toMatchObject({
      toneLabel: 'Mean-reversion sweet spot',
    })
  })
})
