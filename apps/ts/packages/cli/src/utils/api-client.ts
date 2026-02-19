/**
 * API Client entrypoint for CLI commands.
 * Composes domain-specific clients.
 */

import { AnalyticsClient } from './api-clients/analytics-client.js';
import { AuthClient } from './api-clients/auth-client.js';
import { DatabaseClient } from './api-clients/database-client.js';
import { DatasetClient } from './api-clients/dataset-client.js';
import { JQuantsClient } from './api-clients/jquants-client.js';
import { PortfolioClient } from './api-clients/portfolio-client.js';
import { WatchlistClient } from './api-clients/watchlist-client.js';

export type * from './api-clients/types.js';

export class ApiClient {
  readonly baseUrl: string;
  readonly analytics: AnalyticsClient;
  readonly auth: AuthClient;
  readonly database: DatabaseClient;
  readonly dataset: DatasetClient;
  readonly jquants: JQuantsClient;
  readonly portfolio: PortfolioClient;
  readonly watchlist: WatchlistClient;

  constructor(baseUrl: string = process.env.API_BASE_URL || 'http://localhost:3002') {
    this.baseUrl = baseUrl;
    this.analytics = new AnalyticsClient(this.baseUrl);
    this.auth = new AuthClient(this.baseUrl);
    this.database = new DatabaseClient(this.baseUrl);
    this.dataset = new DatasetClient(this.baseUrl);
    this.jquants = new JQuantsClient(this.baseUrl);
    this.portfolio = new PortfolioClient(this.baseUrl);
    this.watchlist = new WatchlistClient(this.baseUrl);
  }
}
