export type EvidenceColorTier = 'excellent' | 'good' | 'light_good' | 'neutral' | 'bad' | 'very_bad';
export type ValuationSignal =
  | 'strong_value_confirmation'
  | 'medium_value_confirmation'
  | 'very_overvalued_warning'
  | 'overvalued_warning'
  | 'no_positive_earnings_valuation';

export interface EvidenceRankingItem {
  per?: number | null;
  perPercentile?: number | null;
  forwardPer?: number | null;
  forwardPerPercentile?: number | null;
  forwardPOp?: number | null;
  forwardPOpPercentile?: number | null;
  pbrPercentile?: number | null;
  valueCompositeScore?: number | null;
  liquidityResidualZ?: number | null;
  liquidityRegime?:
    | 'neutral_rerating'
    | 'crowded_rerating'
    | 'distribution_stress'
    | 'stale_liquidity'
    | 'neutral'
    | null;
}

export function getCheapValuationPercentileTier(percentile: number | null | undefined): EvidenceColorTier {
  if (percentile == null || !Number.isFinite(percentile)) return 'neutral';
  if (percentile <= 0.1) return 'excellent';
  if (percentile <= 0.2) return 'good';
  if (percentile >= 0.9) return 'very_bad';
  if (percentile >= 0.8) return 'bad';
  return 'neutral';
}

export function getPerEvidenceTier(percentile: number | null | undefined): EvidenceColorTier {
  if (percentile == null || !Number.isFinite(percentile)) return 'neutral';
  if (percentile <= 0.2) return 'good';
  if (percentile >= 0.9) return 'very_bad';
  if (percentile >= 0.8) return 'bad';
  return 'neutral';
}

export function getForwardPerEvidenceTier(item: EvidenceRankingItem): EvidenceColorTier {
  const standaloneTier = getCheapValuationPercentileTier(item.forwardPerPercentile);
  if (standaloneTier === 'very_bad' || standaloneTier === 'bad') {
    return standaloneTier;
  }
  const forwardPerToPerRatio = getPositiveRatio(item.forwardPer, item.per);
  if (hasLowPer(item.perPercentile) && forwardPerToPerRatio != null) {
    if (forwardPerToPerRatio <= 0.8) return 'excellent';
    if (forwardPerToPerRatio <= 1.0) return 'good';
  }
  if (standaloneTier === 'excellent') {
    return 'excellent';
  }
  return standaloneTier;
}

export function getForwardPOpEvidenceTier(
  forwardPOpPercentile: number | null | undefined,
  forwardPerPercentile: number | null | undefined,
  perPercentile: number | null | undefined,
  forwardPOp: number | null | undefined,
  per: number | null | undefined
): EvidenceColorTier {
  if (forwardPOpPercentile == null || !Number.isFinite(forwardPOpPercentile)) {
    return 'neutral';
  }
  if (forwardPOpPercentile >= 0.9) return 'very_bad';
  if (forwardPOpPercentile >= 0.8) return 'bad';
  const forwardPOpToPerRatio = getPositiveRatio(forwardPOp, per);
  if (hasLowPer(perPercentile) && forwardPOpToPerRatio != null && forwardPOpToPerRatio > 1.25) {
    return 'bad';
  }
  if (
    forwardPerPercentile != null &&
    Number.isFinite(forwardPerPercentile) &&
    forwardPerPercentile <= 0.2 &&
    forwardPOpPercentile <= 0.2
  ) {
    return 'good';
  }
  return 'neutral';
}

export function getForecastOperatingProfitGrowthTier(value: number | null | undefined): EvidenceColorTier {
  if (value == null || !Number.isFinite(value)) return 'neutral';
  if (value >= 1.5) return 'excellent';
  if (value >= 1.2) return 'good';
  if (value < 0.8) return 'very_bad';
  if (value < 1.0) return 'bad';
  return 'neutral';
}

export function getFwdPerPbrValueCompositeTier(value: number | null | undefined): EvidenceColorTier {
  if (value == null || !Number.isFinite(value)) return 'neutral';
  if (value >= 0.9) return 'excellent';
  if (value >= 0.8) return 'good';
  if (value <= 0.1) return 'very_bad';
  if (value <= 0.2) return 'bad';
  return 'neutral';
}

export function getLiquidityEvidenceTier(item: EvidenceRankingItem): EvidenceColorTier {
  if (item.liquidityRegime === 'neutral_rerating') {
    return getNeutralReratingEvidenceTier(item);
  }
  if (item.liquidityRegime === 'crowded_rerating') {
    return getCrowdedReratingEvidenceTier(item);
  }
  if (item.liquidityRegime === 'distribution_stress') return 'bad';
  if (item.liquidityRegime === 'stale_liquidity') {
    return hasExpensiveValuationWarning(item) || hasEarningsValuationWarning(item) ? 'very_bad' : 'bad';
  }
  if (item.liquidityRegime === 'neutral') return 'neutral';
  if (item.liquidityResidualZ != null && Number.isFinite(item.liquidityResidualZ) && item.liquidityResidualZ <= -1) {
    return 'bad';
  }
  return 'neutral';
}

export function getValuationSignal(item: EvidenceRankingItem): ValuationSignal | null {
  if (hasCrowdedReratingGreenConfirmation(item)) return 'strong_value_confirmation';
  if (hasVeryExpensiveValuationWarning(item)) return 'very_overvalued_warning';
  if (hasExpensiveValuationWarning(item)) return 'overvalued_warning';
  if (hasEarningsValuationWarning(item)) return 'no_positive_earnings_valuation';
  if (hasReratingValueConfirmation(item)) return 'medium_value_confirmation';
  return null;
}

function getPositiveRatio(numerator: number | null | undefined, denominator: number | null | undefined): number | null {
  if (
    numerator == null ||
    denominator == null ||
    !Number.isFinite(numerator) ||
    !Number.isFinite(denominator) ||
    numerator <= 0 ||
    denominator <= 0
  ) {
    return null;
  }
  return numerator / denominator;
}

function hasLowPer(perPercentile: number | null | undefined): boolean {
  return perPercentile != null && Number.isFinite(perPercentile) && perPercentile <= 0.2;
}

function hasLowPbr(item: EvidenceRankingItem): boolean {
  return item.pbrPercentile != null && Number.isFinite(item.pbrPercentile) && item.pbrPercentile <= 0.2;
}

function hasLowForwardPer(item: EvidenceRankingItem): boolean {
  return (
    item.forwardPerPercentile != null && Number.isFinite(item.forwardPerPercentile) && item.forwardPerPercentile <= 0.2
  );
}

function hasLowPerForwardPerImprovement(item: EvidenceRankingItem): boolean {
  const forwardPerToPerRatio = getPositiveRatio(item.forwardPer, item.per);
  return hasLowPer(item.perPercentile) && forwardPerToPerRatio != null && forwardPerToPerRatio <= 0.8;
}

function hasCrowdedReratingGreenConfirmation(item: EvidenceRankingItem): boolean {
  return (hasLowPbr(item) && hasLowForwardPer(item)) || hasLowPerForwardPerImprovement(item);
}

function hasNeutralReratingStrongBlueConfirmation(item: EvidenceRankingItem): boolean {
  return hasLowPbr(item) && hasLowForwardPer(item);
}

function hasExpensiveValuationWarning(item: EvidenceRankingItem): boolean {
  return [item.perPercentile, item.forwardPerPercentile, item.forwardPOpPercentile, item.pbrPercentile].some(
    (percentile) => percentile != null && Number.isFinite(percentile) && percentile >= 0.8
  );
}

function hasVeryExpensiveValuationWarning(item: EvidenceRankingItem): boolean {
  return [item.perPercentile, item.forwardPerPercentile, item.forwardPOpPercentile, item.pbrPercentile].some(
    (percentile) => percentile != null && Number.isFinite(percentile) && percentile >= 0.9
  );
}

function hasEarningsValuationWarning(item: EvidenceRankingItem): boolean {
  return item.perPercentile == null && item.forwardPerPercentile == null;
}

function hasReratingValueConfirmation(item: EvidenceRankingItem): boolean {
  const forwardPerToPerRatio = getPositiveRatio(item.forwardPer, item.per);
  return (
    hasCrowdedReratingGreenConfirmation(item) ||
    hasLowPbr(item) ||
    (hasLowPer(item.perPercentile) && forwardPerToPerRatio != null && forwardPerToPerRatio <= 1.0)
  );
}

function hasNeutralReratingMediumValueConfirmation(item: EvidenceRankingItem): boolean {
  const forwardPerToPerRatio = getPositiveRatio(item.forwardPer, item.per);
  return (
    hasLowPbr(item) || (hasLowPer(item.perPercentile) && forwardPerToPerRatio != null && forwardPerToPerRatio <= 1.0)
  );
}

function getNeutralReratingEvidenceTier(item: EvidenceRankingItem): EvidenceColorTier {
  if (hasLowPerForwardPerImprovement(item)) return 'excellent';
  if (hasNeutralReratingStrongBlueConfirmation(item)) return 'good';
  if (hasNeutralReratingMediumValueConfirmation(item)) return 'light_good';
  return 'neutral';
}

function getCrowdedReratingEvidenceTier(item: EvidenceRankingItem): EvidenceColorTier {
  if (hasCrowdedReratingGreenConfirmation(item)) return 'excellent';
  if (hasEarningsValuationWarning(item)) return 'bad';
  if (hasExpensiveValuationWarning(item)) return 'bad';
  if (hasReratingValueConfirmation(item)) return 'good';
  return 'bad';
}
