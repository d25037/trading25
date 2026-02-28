import {
  AnalyticsClient as SharedAnalyticsClient,
  type FactorRegressionParams,
  type FactorRegressionResponse,
  type MarketRankingParams,
  type MarketRankingResponse,
  type MarketScreeningResponse,
  type PortfolioFactorRegressionParams,
  type PortfolioFactorRegressionResponse,
  type ROEParams,
  type ROEResponse,
  type ScreeningJobRequest,
  type ScreeningJobResponse,
} from '@trading25/api-clients/analytics';
import { HttpRequestError } from '@trading25/api-clients/base/http-client';

export class AnalyticsClient extends SharedAnalyticsClient {
  constructor(baseUrl: string) {
    super({ baseUrl });
  }

  private async withCliErrorHandling<T>(operation: () => Promise<T>): Promise<T> {
    try {
      return await operation();
    } catch (error) {
      throw this.mapCliError(error);
    }
  }

  private mapCliError(error: unknown): Error {
    if (error instanceof HttpRequestError && error.kind === 'http') {
      const body = error.body as { message?: unknown } | undefined;
      if (body && typeof body.message === 'string' && body.message.trim().length > 0) {
        return new Error(body.message);
      }
      return new Error(`HTTP error! status: ${error.status ?? 'unknown'}`);
    }

    const message = error instanceof HttpRequestError ? error.message : error instanceof Error ? error.message : '';
    if (message.includes('fetch failed') || message.includes('ECONNREFUSED')) {
      return new Error(
        'Cannot connect to API server. Please ensure bt FastAPI is running with "uv run bt server --port 3002"'
      );
    }

    if (message) {
      return new Error(message);
    }

    return new Error('Unknown error occurred');
  }

  override getMarketRanking(params: MarketRankingParams = {}): Promise<MarketRankingResponse> {
    return this.withCliErrorHandling(() => super.getMarketRanking(params));
  }

  override createScreeningJob(params: ScreeningJobRequest): Promise<ScreeningJobResponse> {
    return this.withCliErrorHandling(() => super.createScreeningJob(params));
  }

  override getScreeningJobStatus(jobId: string): Promise<ScreeningJobResponse> {
    return this.withCliErrorHandling(() => super.getScreeningJobStatus(jobId));
  }

  override cancelScreeningJob(jobId: string): Promise<ScreeningJobResponse> {
    return this.withCliErrorHandling(() => super.cancelScreeningJob(jobId));
  }

  override getScreeningResult(jobId: string): Promise<MarketScreeningResponse> {
    return this.withCliErrorHandling(() => super.getScreeningResult(jobId));
  }

  override getROE(params: ROEParams): Promise<ROEResponse> {
    return this.withCliErrorHandling(() => super.getROE(params));
  }

  override getFactorRegression(params: FactorRegressionParams): Promise<FactorRegressionResponse> {
    return this.withCliErrorHandling(() => super.getFactorRegression(params));
  }

  override getPortfolioFactorRegression(
    params: PortfolioFactorRegressionParams
  ): Promise<PortfolioFactorRegressionResponse> {
    return this.withCliErrorHandling(() => super.getPortfolioFactorRegression(params));
  }
}
