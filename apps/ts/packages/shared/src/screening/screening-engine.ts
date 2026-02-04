/**
 * Screening Engine
 * Main engine for running stock screening algorithms
 */

import { detectRangeBreak } from './range-break-detection';
import type { ScreeningConfig, ScreeningInput, ScreeningResult, StockDataPoint } from './types';
import { DEFAULT_SCREENING_CONFIG } from './types';

export class ScreeningEngine {
  private config: ScreeningConfig;

  constructor(config: Partial<ScreeningConfig> = {}) {
    this.config = { ...DEFAULT_SCREENING_CONFIG, ...config };
  }

  /**
   * Run screening on a single stock
   */
  public screenStock(input: ScreeningInput): ScreeningResult[] {
    const results: ScreeningResult[] = [];

    this.runRangeBreakScreening('rangeBreakFast', input, results);
    this.runRangeBreakScreening('rangeBreakSlow', input, results);

    return results;
  }

  /**
   * Run range break screening for specific type
   */
  private runRangeBreakScreening(
    type: 'rangeBreakFast' | 'rangeBreakSlow',
    input: ScreeningInput,
    results: ScreeningResult[]
  ): void {
    const enabledMap = {
      rangeBreakFast: this.config.rangeBreakFastEnabled,
      rangeBreakSlow: this.config.rangeBreakSlowEnabled,
    };

    const paramsMap = {
      rangeBreakFast: this.config.rangeBreakFastParams,
      rangeBreakSlow: this.config.rangeBreakSlowParams,
    };

    if (!enabledMap[type]) {
      return;
    }

    const rangeBreakResult = detectRangeBreak(input.data, paramsMap[type], this.config.recentDays);

    if (rangeBreakResult.found && rangeBreakResult.details) {
      results.push({
        stockCode: input.stockCode,
        companyName: input.companyName,
        scaleCategory: input.scaleCategory,
        sector33Name: input.sector33Name,
        screeningType: type,
        matchedDate: rangeBreakResult.details.breakDate,
        details: { rangeBreak: rangeBreakResult.details },
      });
    }
  }

  /**
   * Run screening on multiple stocks
   */
  public async screenMultipleStocks(
    inputs: ScreeningInput[],
    progressCallback?: (processed: number, total: number, current?: string) => void
  ): Promise<ScreeningResult[]> {
    const allResults: ScreeningResult[] = [];
    const total = inputs.length;

    for (let i = 0; i < inputs.length; i++) {
      const input = inputs[i];

      if (!input) continue;

      if (progressCallback) {
        progressCallback(i, total, input.stockCode);
      }

      try {
        const stockResults = this.screenStock(input);
        allResults.push(...stockResults);
      } catch (error) {
        console.warn(`Screening failed for ${input.stockCode}:`, error);
      }
    }

    if (progressCallback) {
      progressCallback(total, total);
    }

    return allResults;
  }

  /**
   * Check basic input requirements
   */
  private validateBasicInput(input: ScreeningInput, errors: string[], warnings: string[]): boolean {
    if (!input.stockCode) {
      errors.push('Stock code is required');
    }

    if (!input.companyName) {
      warnings.push('Company name is missing');
    }

    if (!input.data || input.data.length === 0) {
      errors.push('Stock data is required');
      return false;
    }

    const maxRequiredLength = this.getRequiredDataLength();
    if (maxRequiredLength > 0 && input.data.length < maxRequiredLength) {
      warnings.push(
        `Data length (${input.data.length}) is less than maximum required (${maxRequiredLength}). Some screenings may be skipped.`
      );
    }

    return true;
  }

  /**
   * Validate individual data points
   */
  private validateDataPoints(
    data: StockDataPoint[],
    errors: string[],
    warnings: string[]
  ): { invalidCount: number; zeroVolumeCount: number } {
    let invalidDataCount = 0;
    let zeroVolumeCount = 0;

    for (const [index, dataPoint] of data.entries()) {
      if (!dataPoint.date || Number.isNaN(dataPoint.date.getTime())) {
        errors.push(`Invalid date at index ${index}`);
      }

      if (
        typeof dataPoint.open !== 'number' ||
        dataPoint.open <= 0 ||
        typeof dataPoint.high !== 'number' ||
        dataPoint.high <= 0 ||
        typeof dataPoint.low !== 'number' ||
        dataPoint.low <= 0 ||
        typeof dataPoint.close !== 'number' ||
        dataPoint.close <= 0
      ) {
        invalidDataCount++;
      }

      if (typeof dataPoint.volume !== 'number' || dataPoint.volume < 0) {
        zeroVolumeCount++;
      }

      // OHLC validation
      if (
        dataPoint.high < dataPoint.low ||
        dataPoint.high < dataPoint.open ||
        dataPoint.high < dataPoint.close ||
        dataPoint.low > dataPoint.open ||
        dataPoint.low > dataPoint.close
      ) {
        warnings.push(`Invalid OHLC relationship at ${dataPoint.date.toISOString().split('T')[0]}`);
      }
    }

    return { invalidCount: invalidDataCount, zeroVolumeCount };
  }

  /**
   * Check data continuity
   */
  private checkDataContinuity(data: StockDataPoint[]): number {
    const sortedData = [...data].sort((a, b) => a.date.getTime() - b.date.getTime());
    let gapsCount = 0;

    for (let i = 1; i < sortedData.length; i++) {
      const currentData = sortedData[i];
      const prevData = sortedData[i - 1];

      if (currentData && prevData) {
        const daysDiff = (currentData.date.getTime() - prevData.date.getTime()) / (1000 * 60 * 60 * 24);
        if (daysDiff > 7) {
          gapsCount++;
        }
      }
    }

    return gapsCount;
  }

  /**
   * Validate input data quality
   * Note: Data length validation is performed by each individual screening function
   */
  public validateInput(input: ScreeningInput): {
    isValid: boolean;
    errors: string[];
    warnings: string[];
  } {
    const errors: string[] = [];
    const warnings: string[] = [];

    if (!this.validateBasicInput(input, errors, warnings)) {
      return { isValid: false, errors, warnings };
    }

    const { invalidCount, zeroVolumeCount } = this.validateDataPoints(input.data, errors, warnings);

    if (invalidCount > 0) {
      warnings.push(`${invalidCount} data points have invalid price data`);
    }

    if (zeroVolumeCount > input.data.length * 0.1) {
      warnings.push(
        `${zeroVolumeCount} data points have zero or invalid volume (${((zeroVolumeCount / input.data.length) * 100).toFixed(1)}%)`
      );
    }

    const gapsCount = this.checkDataContinuity(input.data);
    if (gapsCount > 0) {
      warnings.push(`${gapsCount} large time gaps detected in data`);
    }

    return {
      isValid: errors.length === 0,
      errors,
      warnings,
    };
  }

  /**
   * Get configuration
   */
  public getConfig(): ScreeningConfig {
    return { ...this.config };
  }

  /**
   * Update configuration
   */
  public updateConfig(newConfig: Partial<ScreeningConfig>): void {
    this.config = { ...this.config, ...newConfig };
  }

  /**
   * Get maximum required data length for screening
   * Note: This is for informational purposes only (e.g., CLI display)
   * Each screening function validates data length independently
   */
  public getRequiredDataLength(): number {
    const requirements: number[] = [];

    // Only consider enabled screening types
    if (this.config.rangeBreakFastEnabled) {
      requirements.push(this.config.rangeBreakFastParams.period + this.config.recentDays);
    }

    if (this.config.rangeBreakSlowEnabled) {
      requirements.push(this.config.rangeBreakSlowParams.period + this.config.recentDays);
    }

    // Return maximum of enabled requirements, or 0 if none enabled
    return requirements.length > 0 ? Math.max(...requirements) : 0;
  }

  /**
   * Check if result matches screening type filter
   */
  private static matchesScreeningType(
    result: ScreeningResult,
    screeningTypes?: ('rangeBreakFast' | 'rangeBreakSlow')[]
  ): boolean {
    if (!screeningTypes) {
      return true;
    }
    return screeningTypes.includes(result.screeningType);
  }

  /**
   * Check if result matches date range filter
   */
  private static matchesDateRange(result: ScreeningResult, dateRange?: { from: Date; to: Date }): boolean {
    if (!dateRange) {
      return true;
    }
    return result.matchedDate >= dateRange.from && result.matchedDate <= dateRange.to;
  }

  /**
   * Check if result matches break percentage filter (range break only)
   */
  private static matchesBreakPercentage(result: ScreeningResult, minBreakPercentage?: number): boolean {
    if (!minBreakPercentage) {
      return true;
    }

    if (result.screeningType !== 'rangeBreakFast' && result.screeningType !== 'rangeBreakSlow') {
      return true; // Not applicable for non-range-break strategies
    }

    const breakPercentage = result.details?.rangeBreak?.breakPercentage || 0;
    return breakPercentage >= minBreakPercentage;
  }

  /**
   * Get volume ratio for result
   */
  private static getVolumeRatio(result: ScreeningResult): number {
    return result.details?.rangeBreak?.volumeRatio || 0;
  }

  /**
   * Check if result matches volume ratio filter
   */
  private static matchesVolumeRatio(result: ScreeningResult, minVolumeRatio?: number): boolean {
    if (!minVolumeRatio) {
      return true;
    }

    const volumeRatio = ScreeningEngine.getVolumeRatio(result);
    return volumeRatio >= minVolumeRatio;
  }

  /**
   * Filter results by criteria
   */
  public static filterResults(
    results: ScreeningResult[],
    criteria: {
      screeningTypes?: ('rangeBreakFast' | 'rangeBreakSlow')[];
      minBreakPercentage?: number;
      minVolumeRatio?: number;
      dateRange?: { from: Date; to: Date };
    }
  ): ScreeningResult[] {
    return results.filter((result) => {
      return (
        ScreeningEngine.matchesScreeningType(result, criteria.screeningTypes) &&
        ScreeningEngine.matchesDateRange(result, criteria.dateRange) &&
        ScreeningEngine.matchesBreakPercentage(result, criteria.minBreakPercentage) &&
        ScreeningEngine.matchesVolumeRatio(result, criteria.minVolumeRatio)
      );
    });
  }

  /**
   * Compare results by date
   */
  private static compareByDate(a: ScreeningResult, b: ScreeningResult): number {
    return a.matchedDate.getTime() - b.matchedDate.getTime();
  }

  /**
   * Compare results by stock code
   */
  private static compareByStockCode(a: ScreeningResult, b: ScreeningResult): number {
    return a.stockCode.localeCompare(b.stockCode);
  }

  /**
   * Compare results by volume ratio
   */
  private static compareByVolumeRatio(a: ScreeningResult, b: ScreeningResult): number {
    const aRatio = ScreeningEngine.getVolumeRatio(a);
    const bRatio = ScreeningEngine.getVolumeRatio(b);
    return aRatio - bRatio;
  }

  /**
   * Get break percentage for result (range break only)
   */
  private static getBreakPercentage(result: ScreeningResult): number {
    if (result.screeningType === 'rangeBreakFast' || result.screeningType === 'rangeBreakSlow') {
      return result.details?.rangeBreak?.breakPercentage || 0;
    }
    return 0;
  }

  /**
   * Compare results by break percentage
   */
  private static compareByBreakPercentage(a: ScreeningResult, b: ScreeningResult): number {
    const aBreak = ScreeningEngine.getBreakPercentage(a);
    const bBreak = ScreeningEngine.getBreakPercentage(b);
    return aBreak - bBreak;
  }

  /**
   * Get comparison function for sort type
   */
  private static getComparisonFunction(
    sortBy: 'date' | 'stockCode' | 'volumeRatio' | 'breakPercentage'
  ): (a: ScreeningResult, b: ScreeningResult) => number {
    switch (sortBy) {
      case 'date':
        return ScreeningEngine.compareByDate;
      case 'stockCode':
        return ScreeningEngine.compareByStockCode;
      case 'volumeRatio':
        return ScreeningEngine.compareByVolumeRatio;
      case 'breakPercentage':
        return ScreeningEngine.compareByBreakPercentage;
    }
  }

  /**
   * Sort results by criteria
   */
  public static sortResults(
    results: ScreeningResult[],
    sortBy: 'date' | 'stockCode' | 'volumeRatio' | 'breakPercentage' = 'date',
    order: 'asc' | 'desc' = 'desc'
  ): ScreeningResult[] {
    const compareFunc = ScreeningEngine.getComparisonFunction(sortBy);

    return [...results].sort((a, b) => {
      const comparison = compareFunc(a, b);
      return order === 'asc' ? comparison : -comparison;
    });
  }
}
