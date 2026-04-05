export const TOPIX_MODE_SHORT_WINDOW_STREAKS = 3
export const TOPIX_MODE_LONG_WINDOW_STREAKS = 53
export const TOPIX_MODE_RECENT_POINT_LIMIT = 120

export type TopixMode = 'bullish' | 'bearish'
export type TopixStreakBaseMode = TopixMode | 'flat'

export type TopixModeStateKey =
  | 'long_bullish__short_bullish'
  | 'long_bullish__short_bearish'
  | 'long_bearish__short_bullish'
  | 'long_bearish__short_bearish'

export interface TopixModeInputPoint {
  date: string
  close: number
}

export interface TopixModePoint {
  date: string
  segmentStartDate: string
  segmentEndDate: string
  segmentDayCount: number
  segmentReturn: number
  baseStreakMode: TopixStreakBaseMode
  shortMode: TopixMode
  longMode: TopixMode
  shortDominantSegmentReturn: number
  longDominantSegmentReturn: number
  shortDominantSegmentEndDate: string
  longDominantSegmentEndDate: string
  shortDominantSegmentDayCount: number
  longDominantSegmentDayCount: number
  shortModeSpanStreakCount: number
  longModeSpanStreakCount: number
  stateKey: TopixModeStateKey
  stateLabel: string
  stateSegmentLength: number
}

export interface TopixModeAnalysis {
  shortWindowStreaks: number
  longWindowStreaks: number
  minimumRequiredStreaks: number
  streakCount: number
  points: TopixModePoint[]
  currentPoint: TopixModePoint | null
}

export interface TopixModeStateCopy {
  toneLabel: string
  description: string
}

interface TopixStreakCandle {
  segmentId: number
  baseStreakMode: TopixStreakBaseMode
  startIndex: number
  endIndex: number
  startDate: string
  endDate: string
  segmentDayCount: number
  segmentReturn: number
}

const TOPIX_MODE_STATE_COPY: Record<TopixModeStateKey, TopixModeStateCopy> = {
  long_bullish__short_bullish: {
    toneLabel: 'Crowded upside',
    description:
      'This was the weakest streak-based validation state. Long and short streak shocks were already up, so reversion edge looked thin.',
  },
  long_bullish__short_bearish: {
    toneLabel: 'Pullback inside strength',
    description:
      'Short weakness appeared inside a longer up backdrop. It can still bounce, but the washout signal was weaker than bearish / bearish.',
  },
  long_bearish__short_bullish: {
    toneLabel: 'Bounce in repair',
    description:
      'A short rebound appeared while the longer streak lens stayed down. Validation showed some recovery, but less cleanly than full washout states.',
  },
  long_bearish__short_bearish: {
    toneLabel: 'Mean-reversion sweet spot',
    description:
      'This was the most stable streak state in validation. Long and short dominant streaks both leaned down, and subsequent returns rebounded best.',
  },
}

export function buildTopixModeStateKey(longMode: TopixMode, shortMode: TopixMode): TopixModeStateKey {
  return `long_${longMode}__short_${shortMode}`
}

export function formatTopixModeStateLabel(stateKey: TopixModeStateKey): string {
  return stateKey.replace('long_', 'Long ').replace('__short_', ' / Short ').replace(/_/g, ' ').replace(/\b\w/g, (char) =>
    char.toUpperCase()
  )
}

export function getTopixModeStateCopy(stateKey: TopixModeStateKey): TopixModeStateCopy {
  return TOPIX_MODE_STATE_COPY[stateKey]
}

export function buildTopixMultiTimeframeModeAnalysis(
  data: readonly TopixModeInputPoint[],
  {
    shortWindowStreaks = TOPIX_MODE_SHORT_WINDOW_STREAKS,
    longWindowStreaks = TOPIX_MODE_LONG_WINDOW_STREAKS,
  }: {
    shortWindowStreaks?: number
    longWindowStreaks?: number
  } = {}
): TopixModeAnalysis {
  if (shortWindowStreaks <= 0 || longWindowStreaks <= 0) {
    throw new Error('Window streaks must be positive integers')
  }

  const streakCandles = buildStreakCandles(data)
  const minimumRequiredStreaks = Math.max(shortWindowStreaks, longWindowStreaks)
  if (streakCandles.length < minimumRequiredStreaks) {
    return {
      shortWindowStreaks,
      longWindowStreaks,
      minimumRequiredStreaks,
      streakCount: streakCandles.length,
      points: [],
      currentPoint: null,
    }
  }

  const firstIndex = minimumRequiredStreaks - 1
  const points: TopixModePoint[] = []
  let previousShortMode: TopixMode | null = null
  let previousLongMode: TopixMode | null = null
  let previousStateKey: TopixModeStateKey | null = null
  let shortModeSpanStreakCount = 0
  let longModeSpanStreakCount = 0
  let stateSegmentLength = 0

  for (let index = firstIndex; index < streakCandles.length; index += 1) {
    const currentStreak = streakCandles[index]
    if (!currentStreak) {
      continue
    }
    const shortDominant = resolveDominantStreakReturn(streakCandles, index, shortWindowStreaks)
    const longDominant = resolveDominantStreakReturn(streakCandles, index, longWindowStreaks)
    const shortMode: TopixMode = shortDominant.returnValue >= 0 ? 'bullish' : 'bearish'
    const longMode: TopixMode = longDominant.returnValue >= 0 ? 'bullish' : 'bearish'
    const stateKey = buildTopixModeStateKey(longMode, shortMode)

    shortModeSpanStreakCount = shortMode === previousShortMode ? shortModeSpanStreakCount + 1 : 1
    longModeSpanStreakCount = longMode === previousLongMode ? longModeSpanStreakCount + 1 : 1
    stateSegmentLength = stateKey === previousStateKey ? stateSegmentLength + 1 : 1

    points.push({
      date: currentStreak.endDate,
      segmentStartDate: currentStreak.startDate,
      segmentEndDate: currentStreak.endDate,
      segmentDayCount: currentStreak.segmentDayCount,
      segmentReturn: currentStreak.segmentReturn,
      baseStreakMode: currentStreak.baseStreakMode,
      shortMode,
      longMode,
      shortDominantSegmentReturn: shortDominant.returnValue,
      longDominantSegmentReturn: longDominant.returnValue,
      shortDominantSegmentEndDate: shortDominant.segmentEndDate,
      longDominantSegmentEndDate: longDominant.segmentEndDate,
      shortDominantSegmentDayCount: shortDominant.segmentDayCount,
      longDominantSegmentDayCount: longDominant.segmentDayCount,
      shortModeSpanStreakCount,
      longModeSpanStreakCount,
      stateKey,
      stateLabel: formatTopixModeStateLabel(stateKey),
      stateSegmentLength,
    })

    previousShortMode = shortMode
    previousLongMode = longMode
    previousStateKey = stateKey
  }

  return {
    shortWindowStreaks,
    longWindowStreaks,
    minimumRequiredStreaks,
    streakCount: streakCandles.length,
    points,
    currentPoint: points.at(-1) ?? null,
  }
}

function buildStreakCandles(data: readonly TopixModeInputPoint[]): TopixStreakCandle[] {
  if (data.length < 2) {
    return []
  }

  const closeReturns = data.map((point, index) => {
    if (index === 0) {
      return Number.NaN
    }
    const previousClose = data[index - 1]?.close ?? Number.NaN
    if (!Number.isFinite(previousClose) || !Number.isFinite(point.close) || previousClose === 0) {
      return Number.NaN
    }
    return point.close / previousClose - 1
  })

  const streakCandles: TopixStreakCandle[] = []
  let currentStartIndex = 1
  let currentMode = classifyCloseReturn(closeReturns[1] ?? Number.NaN)

  for (let index = 2; index < data.length; index += 1) {
    const mode = classifyCloseReturn(closeReturns[index] ?? Number.NaN)
    if (mode === currentMode) {
      continue
    }
    streakCandles.push(createStreakCandle(data, currentStartIndex, index - 1, currentMode, streakCandles.length + 1))
    currentStartIndex = index
    currentMode = mode
  }

  streakCandles.push(createStreakCandle(data, currentStartIndex, data.length - 1, currentMode, streakCandles.length + 1))
  return streakCandles
}

function createStreakCandle(
  data: readonly TopixModeInputPoint[],
  startIndex: number,
  endIndex: number,
  baseStreakMode: TopixStreakBaseMode,
  segmentId: number
): TopixStreakCandle {
  const anchorIndex = startIndex - 1
  const syntheticOpen = data[anchorIndex]?.close ?? Number.NaN
  const syntheticClose = data[endIndex]?.close ?? Number.NaN
  const segmentReturn =
    Number.isFinite(syntheticOpen) && Number.isFinite(syntheticClose) && syntheticOpen !== 0
      ? syntheticClose / syntheticOpen - 1
      : Number.NaN

  return {
    segmentId,
    baseStreakMode,
    startIndex,
    endIndex,
    startDate: data[startIndex]?.date ?? '',
    endDate: data[endIndex]?.date ?? '',
    segmentDayCount: endIndex - startIndex + 1,
    segmentReturn,
  }
}

function classifyCloseReturn(value: number): TopixStreakBaseMode {
  if (value > 0) {
    return 'bullish'
  }
  if (value < 0) {
    return 'bearish'
  }
  return 'flat'
}

function resolveDominantStreakReturn(
  streakCandles: readonly TopixStreakCandle[],
  endIndex: number,
  windowStreaks: number
): { returnValue: number; segmentEndDate: string; segmentDayCount: number } {
  const startIndex = endIndex - windowStreaks + 1
  let dominantStreak = streakCandles[startIndex] as TopixStreakCandle

  for (let index = startIndex + 1; index <= endIndex; index += 1) {
    const currentStreak = streakCandles[index] as TopixStreakCandle
    if (!Number.isFinite(currentStreak.segmentReturn)) {
      continue
    }
    if (!Number.isFinite(dominantStreak.segmentReturn) || Math.abs(currentStreak.segmentReturn) > Math.abs(dominantStreak.segmentReturn)) {
      dominantStreak = currentStreak
    }
  }

  return {
    returnValue: dominantStreak.segmentReturn,
    segmentEndDate: dominantStreak.endDate,
    segmentDayCount: dominantStreak.segmentDayCount,
  }
}
