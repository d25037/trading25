import { calculateROEBatch } from '@trading25/shared';
import type { ROEResult } from '@trading25/shared/fundamental-analysis';
import { logger } from '@trading25/shared/utils/logger';
import type { ROEResponse, ROEResultItem, ROESummary } from '../schemas/roe';
import { BaseJQuantsService } from './base-jquants-service';

interface ROEQueryOptions {
  code?: string;
  date?: string;
  annualize: boolean;
  preferConsolidated: boolean;
  minEquity: number;
  sortBy: 'roe' | 'code' | 'date';
  limit: number;
}

export class ROEDataService extends BaseJQuantsService {
  async calculateROE(options: ROEQueryOptions): Promise<ROEResponse> {
    logger.debug('Calculating ROE', { options });

    const fetchParams: { code?: string; date?: string } = {};
    if (options.code) {
      fetchParams.code = options.code;
    }
    if (options.date) {
      fetchParams.date = options.date;
    }

    // Fetch financial statements from JQuants API
    const client = this.getJQuantsClient();
    const response = await this.withTokenRefresh(() => client.getStatements(fetchParams));

    if (!response.data || response.data.length === 0) {
      logger.debug('No financial statements found', { fetchParams });
      return {
        results: [],
        summary: {
          averageROE: 0,
          maxROE: 0,
          minROE: 0,
          totalCompanies: 0,
        },
        lastUpdated: new Date().toISOString(),
      };
    }

    logger.debug('Found financial statements', { count: response.data.length });

    // Calculate ROE using shared module
    const calcOptions = {
      annualize: options.annualize,
      preferConsolidated: options.preferConsolidated,
      minEquityThreshold: options.minEquity,
    };

    const results = calculateROEBatch(response.data, calcOptions);

    if (results.length === 0) {
      logger.debug('No ROE calculations possible with given criteria');
      return {
        results: [],
        summary: {
          averageROE: 0,
          maxROE: 0,
          minROE: 0,
          totalCompanies: 0,
        },
        lastUpdated: new Date().toISOString(),
      };
    }

    // Sort results
    const sortedResults = this.sortResults(results, options.sortBy);

    // Apply limit
    const limitedResults = sortedResults.slice(0, options.limit);

    // Transform to API response format
    const apiResults = limitedResults.map((result) => this.transformToApiResult(result));

    // Calculate summary statistics
    const summary = this.calculateSummary(limitedResults);

    logger.debug('ROE calculation complete', {
      totalResults: limitedResults.length,
      averageROE: summary.averageROE,
    });

    return {
      results: apiResults,
      summary,
      lastUpdated: new Date().toISOString(),
    };
  }

  private sortResults(results: ROEResult[], sortBy: 'roe' | 'code' | 'date'): ROEResult[] {
    return results.sort((a, b) => {
      switch (sortBy) {
        case 'code':
          return a.metadata.code.localeCompare(b.metadata.code);
        case 'date':
          return new Date(b.metadata.periodEnd).getTime() - new Date(a.metadata.periodEnd).getTime();
        default:
          return b.roe - a.roe;
      }
    });
  }

  private transformToApiResult(result: ROEResult): ROEResultItem {
    return {
      roe: result.roe,
      netProfit: result.netProfit,
      equity: result.equity,
      metadata: {
        code: result.metadata.code,
        periodType: result.metadata.periodType,
        periodEnd: result.metadata.periodEnd,
        isConsolidated: result.metadata.isConsolidated,
        accountingStandard: result.metadata.accountingStandard,
        isAnnualized: result.metadata.isAnnualized,
      },
    };
  }

  private calculateSummary(results: ROEResult[]): ROESummary {
    if (results.length === 0) {
      return {
        averageROE: 0,
        maxROE: 0,
        minROE: 0,
        totalCompanies: 0,
      };
    }

    const roeValues = results.map((r) => r.roe);
    const averageROE = roeValues.reduce((sum, roe) => sum + roe, 0) / roeValues.length;
    const maxROE = Math.max(...roeValues);
    const minROE = Math.min(...roeValues);

    return {
      averageROE: Math.round(averageROE * 100) / 100,
      maxROE: Math.round(maxROE * 100) / 100,
      minROE: Math.round(minROE * 100) / 100,
      totalCompanies: results.length,
    };
  }
}
