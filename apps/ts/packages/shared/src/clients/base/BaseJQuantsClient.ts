import type {
  JQuantsConfig,
  JQuantsDailyQuotesParams,
  JQuantsDailyQuotesResponse,
  JQuantsIndicesParams,
  JQuantsIndicesResponse,
  JQuantsListedInfoParams,
  JQuantsListedInfoResponse,
  JQuantsWeeklyMarginInterestParams,
  JQuantsWeeklyMarginInterestResponse,
} from '../../types/jquants';
import { createDefaultLogger, type Logger } from '../../utils/logger';
import { JQuantsApiError } from './errors';

/**
 * JQuants API rate limits by plan (requests per minute)
 */
export const JQUANTS_PLAN_LIMITS = {
  free: 5,
  light: 60,
  standard: 120,
  premium: 500,
} as const;

export type JQuantsPlan = keyof typeof JQUANTS_PLAN_LIMITS;

/**
 * Validate and return the JQuants plan from environment variable
 */
export function validateJQuantsPlan(plan: string | undefined): JQuantsPlan {
  if (!plan) {
    throw new Error('JQUANTS_PLAN environment variable is required. Set it to one of: free, light, standard, premium');
  }
  if (!(plan in JQUANTS_PLAN_LIMITS)) {
    throw new Error(`Invalid JQUANTS_PLAN: "${plan}". Must be one of: free, light, standard, premium`);
  }
  return plan as JQuantsPlan;
}

/**
 * Convert requests per minute to requests per second
 */
export function getRequestsPerSecond(plan: JQuantsPlan): number {
  const requestsPerMinute = JQUANTS_PLAN_LIMITS[plan];
  return Math.min(requestsPerMinute / 60, 10);
}

/**
 * Calculate optimal concurrency based on plan
 */
export function calculatePlanConcurrency(plan: JQuantsPlan): number {
  const rps = getRequestsPerSecond(plan);
  if (rps < 1) return 1;
  if (rps < 2) return 2;
  return 3;
}

/**
 * Get minimum interval between requests based on plan (in ms)
 */
function getMinIntervalMs(): number {
  const planEnv = process.env.JQUANTS_PLAN;
  const plan = planEnv && planEnv in JQUANTS_PLAN_LIMITS ? (planEnv as JQuantsPlan) : 'free';
  const requestsPerMinute = JQUANTS_PLAN_LIMITS[plan];
  // Convert to interval in ms, with 10% safety margin
  return Math.ceil(((60 * 1000) / requestsPerMinute) * 1.1);
}

/**
 * FIFO rate limit queue with explicit lock/queue control.
 * Serializes concurrent requests to enforce minimum interval between API calls.
 * A single global instance is shared across all BaseJQuantsClient instances.
 */
class RateLimitQueue {
  private queue: Array<{
    resolve: () => void;
  }> = [];
  private processing = false;
  private lastRequestTime = 0;
  private _disabled = false;

  get disabled(): boolean {
    return this._disabled;
  }

  reset(options?: { disable?: boolean }): void {
    this.lastRequestTime = 0;
    this.queue = [];
    // processing は触らない — processQueue ループが空キューで自然終了する
    this._disabled = options?.disable ?? false;
  }

  /**
   * Acquire a rate limit slot. Resolves when this request is allowed to proceed.
   * Requests are processed in strict FIFO order.
   */
  async acquire(): Promise<void> {
    if (this._disabled) return;

    return new Promise<void>((resolve) => {
      this.queue.push({ resolve });
      this.processQueue();
    });
  }

  private async processQueue(): Promise<void> {
    if (this.processing) return;
    this.processing = true;

    try {
      while (this.queue.length > 0) {
        const next = this.queue.shift();
        if (!next) break;

        if (this._disabled) {
          next.resolve();
          continue;
        }

        const minInterval = getMinIntervalMs();
        const now = Date.now();
        const elapsed = now - this.lastRequestTime;

        if (elapsed < minInterval) {
          await new Promise<void>((r) => setTimeout(r, minInterval - elapsed));
        }

        this.lastRequestTime = Date.now();
        next.resolve();
      }
    } finally {
      this.processing = false;
    }
  }
}

/** Global rate limiter shared across all client instances */
const globalRateLimiter = new RateLimitQueue();

/**
 * Reset the global rate limiter state.
 * This is useful for testing to ensure clean state between tests.
 * @param options.disable - If true, disables rate limiting entirely (useful for tests)
 */
export function resetRateLimiter(options?: { disable?: boolean }): void {
  globalRateLimiter.reset(options);
}

export abstract class BaseJQuantsClient {
  protected readonly baseURL = 'https://api.jquants.com/v2/';
  protected config: JQuantsConfig;
  protected logger: Logger;

  constructor(
    config?: Partial<
      JQuantsConfig & {
        logger?: Logger;
      }
    >
  ) {
    this.config = {
      apiKey: config?.apiKey || process.env.JQUANTS_API_KEY || '',
    };

    this.logger = config?.logger || createDefaultLogger();

    if (!this.config.apiKey) {
      this.logger.warn('JQUANTS_API_KEY is not set. API calls will fail.');
    }
  }

  private buildURL(endpoint: string, params?: Record<string, unknown>): string {
    const normalizedEndpoint = endpoint.startsWith('/') ? endpoint.slice(1) : endpoint;
    const url = new URL(normalizedEndpoint, this.baseURL);

    if (params) {
      for (const [key, value] of Object.entries(params)) {
        if (value !== undefined && value !== null) {
          url.searchParams.set(key, String(value));
        }
      }
    }

    return url.toString();
  }

  private async handleResponse<T>(response: Response): Promise<T> {
    if (!response.ok) {
      let errorBody: unknown;
      try {
        errorBody = await response.json();
      } catch {
        errorBody = await response.text().catch(() => null);
      }

      const errorMessage =
        typeof errorBody === 'object' && errorBody !== null
          ? ((errorBody as { message?: string }).message ?? response.statusText)
          : response.statusText;

      throw new JQuantsApiError({
        message: errorMessage,
        status: response.status,
        statusText: response.statusText,
        responseBody: errorBody,
      });
    }

    return response.json() as Promise<T>;
  }

  protected async makeAuthenticatedRequest<T, P = Record<string, unknown>>(
    method: 'get' | 'post',
    endpoint: string,
    params?: P
  ): Promise<T> {
    if (!this.config.apiKey) {
      throw new Error('API key is required for authentication');
    }

    // Wait for rate limit before each request
    await this.waitForRateLimit();

    const isGet = method === 'get';
    const url = isGet ? this.buildURL(endpoint, params as Record<string, unknown>) : this.buildURL(endpoint);

    const response = await fetch(url, {
      method: method.toUpperCase(),
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': this.config.apiKey,
      },
      body: isGet ? undefined : JSON.stringify(params),
    });

    return await this.handleResponse<T>(response);
  }

  /**
   * Wait for rate limit before making a request.
   * Delegates to the global RateLimitQueue which enforces FIFO ordering.
   */
  private async waitForRateLimit(): Promise<void> {
    await globalRateLimiter.acquire();
  }

  private async fetchSinglePage<T>(url: string): Promise<T> {
    // Wait for rate limit before each request
    await this.waitForRateLimit();

    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 30000); // 30 second timeout

    try {
      const response = await fetch(url, {
        method: 'GET',
        headers: {
          'Content-Type': 'application/json',
          'x-api-key': this.config.apiKey,
        },
        signal: controller.signal,
      });

      return await this.handleResponse<T>(response);
    } catch (error) {
      if (error instanceof Error && error.name === 'AbortError') {
        throw new Error(`Request timeout after 30s: ${url}`);
      }
      throw error;
    } finally {
      clearTimeout(timeoutId);
    }
  }

  protected async makePaginatedRequest<
    T extends { data: D[]; pagination_key?: string },
    D,
    P = Record<string, unknown>,
  >(endpoint: string, params?: P, maxPages = 10): Promise<T> {
    if (!this.config.apiKey) {
      throw new Error('API key is required for authentication');
    }

    const allData: D[] = [];
    let paginationKey: string | undefined;
    let pageCount = 0;

    this.logger.debug(`[PAGINATED] Starting ${endpoint}`);

    do {
      pageCount++;
      const requestParams = paginationKey ? { ...params, pagination_key: paginationKey } : params;
      const url = this.buildURL(endpoint, requestParams as Record<string, unknown>);

      this.logger.debug(`[PAGINATED] Fetching page ${pageCount} for ${endpoint}`);
      let data: T;
      try {
        data = await this.fetchSinglePage<T>(url);
      } catch (error) {
        this.logger.error(`[PAGINATED] ERROR on page ${pageCount} for ${endpoint}:`, error);
        throw error;
      }
      this.logger.debug(`[PAGINATED] Got ${data.data?.length ?? 0} records`);

      if (data.data && data.data.length > 0) {
        allData.push(...data.data);
      }
      paginationKey = data.pagination_key;
    } while (paginationKey && pageCount < maxPages);

    this.logger.debug(`[PAGINATED] Completed ${endpoint} with ${allData.length} total records`);
    return { data: allData } as T;
  }

  protected handleError(error: unknown, message: string): void {
    if (JQuantsApiError.isJQuantsApiError(error)) {
      throw new Error(`${message}: ${error.message}`);
    }
    throw new Error(`${message}: ${error}`);
  }

  public updateApiKey(apiKey: string): void {
    this.config.apiKey = apiKey;
  }

  public getApiKeyStatus(): { hasApiKey: boolean; maskedKey: string | null } {
    if (!this.config.apiKey) {
      return { hasApiKey: false, maskedKey: null };
    }
    const key = this.config.apiKey;
    const masked = key.length > 8 ? `${key.slice(0, 4)}...${key.slice(-4)}` : '****';
    return { hasApiKey: true, maskedKey: masked };
  }

  async getIndices(params: JQuantsIndicesParams): Promise<JQuantsIndicesResponse> {
    return this.makeAuthenticatedRequest<JQuantsIndicesResponse, JQuantsIndicesParams>(
      'get',
      '/indices/bars/daily',
      params
    );
  }

  async getListedInfo(params?: JQuantsListedInfoParams): Promise<JQuantsListedInfoResponse> {
    return this.makeAuthenticatedRequest<JQuantsListedInfoResponse, JQuantsListedInfoParams>(
      'get',
      '/equities/master',
      params
    );
  }

  async getDailyQuotes(params: JQuantsDailyQuotesParams): Promise<JQuantsDailyQuotesResponse> {
    return this.makeAuthenticatedRequest<JQuantsDailyQuotesResponse, JQuantsDailyQuotesParams>(
      'get',
      '/equities/bars/daily',
      params
    );
  }

  async getWeeklyMarginInterest(
    params: JQuantsWeeklyMarginInterestParams
  ): Promise<JQuantsWeeklyMarginInterestResponse> {
    return this.makeAuthenticatedRequest<JQuantsWeeklyMarginInterestResponse, JQuantsWeeklyMarginInterestParams>(
      'get',
      '/markets/margin-interest',
      params
    );
  }
}
