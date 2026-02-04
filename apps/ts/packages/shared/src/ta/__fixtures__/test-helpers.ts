export function calculateVolatility(values: number[]): number {
  const returns: number[] = [];
  for (let i = 1; i < values.length; i++) {
    const current = values[i];
    const previous = values[i - 1];
    if (current !== undefined && previous !== undefined && previous !== 0) {
      returns.push((current - previous) / previous);
    }
  }
  return calculateStdDev(returns);
}

export function calculateStdDev(values: number[]): number {
  const mean = values.reduce((a, b) => a + b, 0) / values.length;
  const squaredDiffs = values.map((v) => (v - mean) ** 2);
  const variance = squaredDiffs.reduce((a, b) => a + b, 0) / values.length;
  return Math.sqrt(variance);
}

export function calculateCorrelation(x: number[], y: number[]): number {
  const n = Math.min(x.length, y.length);
  const xMean = x.slice(0, n).reduce((a, b) => a + b, 0) / n;
  const yMean = y.slice(0, n).reduce((a, b) => a + b, 0) / n;

  let numerator = 0;
  let xDenom = 0;
  let yDenom = 0;

  for (let i = 0; i < n; i++) {
    const xVal = x[i];
    const yVal = y[i];
    if (xVal !== undefined && yVal !== undefined) {
      const xDiff = xVal - xMean;
      const yDiff = yVal - yMean;
      numerator += xDiff * yDiff;
      xDenom += xDiff ** 2;
      yDenom += yDiff ** 2;
    }
  }

  return numerator / Math.sqrt(xDenom * yDenom);
}

export function findCrossovers(
  series1: number[],
  series2: number[]
): Array<{ type: 'golden' | 'death'; index: number }> {
  const crossovers: Array<{ type: 'golden' | 'death'; index: number }> = [];

  for (let i = 1; i < Math.min(series1.length, series2.length); i++) {
    const prev1 = series1[i - 1];
    const curr1 = series1[i];
    const prev2 = series2[i - 1];
    const curr2 = series2[i];

    if (prev1 !== undefined && curr1 !== undefined && prev2 !== undefined && curr2 !== undefined) {
      if (prev1 <= prev2 && curr1 > curr2) {
        crossovers.push({ type: 'golden', index: i });
      } else if (prev1 >= prev2 && curr1 < curr2) {
        crossovers.push({ type: 'death', index: i });
      }
    }
  }

  return crossovers;
}

export function findHistogramCrossovers(histogram: number[]): Array<{ type: 'bullish' | 'bearish'; index: number }> {
  const crossovers: Array<{ type: 'bullish' | 'bearish'; index: number }> = [];

  for (let i = 1; i < histogram.length; i++) {
    const prevHist = histogram[i - 1];
    const currHist = histogram[i];

    if (prevHist !== undefined && currHist !== undefined) {
      if (prevHist <= 0 && currHist > 0) {
        crossovers.push({ type: 'bullish', index: i });
      } else if (prevHist >= 0 && currHist < 0) {
        crossovers.push({ type: 'bearish', index: i });
      }
    }
  }

  return crossovers;
}
