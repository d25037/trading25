import { describe, expect, it } from 'vitest';
import { getValuationSignal } from './rankingEvidenceTiers';

describe('getValuationSignal', () => {
  it('classifies the shared strong and medium value confirmation rules', () => {
    expect(
      getValuationSignal({
        pbrPercentile: 0.18,
        forwardPerPercentile: 0.18,
      })
    ).toBe('strong_value_confirmation');

    expect(
      getValuationSignal({
        pbrPercentile: 0.18,
        forwardPerPercentile: 0.55,
      })
    ).toBe('medium_value_confirmation');
  });

  it('classifies overvalued, very overvalued, and no-positive-earnings valuation warnings', () => {
    expect(
      getValuationSignal({
        perPercentile: 0.82,
        forwardPerPercentile: 0.5,
      })
    ).toBe('high_valuation_warning');

    expect(
      getValuationSignal({
        pbrPercentile: 0.92,
      })
    ).toBe('very_high_valuation_warning');

    expect(
      getValuationSignal({
        perPercentile: null,
        forwardPerPercentile: null,
      })
    ).toBe('no_positive_earnings_valuation');
  });
});
