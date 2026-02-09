import type {
  JQuantsConfig,
  JQuantsDailyQuotesParams,
  JQuantsDailyQuotesResponse,
  JQuantsIndicesParams,
  JQuantsIndicesResponse,
  JQuantsListedInfoParams,
  JQuantsListedInfoResponse,
  JQuantsStatementsParams,
  JQuantsStatementsResponse,
  JQuantsTOPIXParams,
  JQuantsTOPIXResponse,
  JQuantsTradingCalendarParams,
  JQuantsTradingCalendarResponse,
  JQuantsWeeklyMarginInterestParams,
  JQuantsWeeklyMarginInterestResponse,
} from './types/jquants';
import type { Logger } from './utils/logger';
import { BaseJQuantsClient } from './base/BaseJQuantsClient';
import { CalendarClient } from './markets/CalendarClient';
import { IndexClient } from './markets/IndexClient';
import { StatementsClient } from './markets/StatementsClient';
import { StockDataClient } from './markets/StockDataClient';

export class JQuantsClient extends BaseJQuantsClient {
  private stockDataClient: StockDataClient;
  private indexClient: IndexClient;
  private calendarClient: CalendarClient;
  private statementsClient: StatementsClient;

  constructor(config?: Partial<JQuantsConfig & { updateEnv?: boolean; envPath?: string; logger?: Logger }>) {
    super(config);

    // Initialize specialized clients with shared config
    this.stockDataClient = new StockDataClient(config);
    this.indexClient = new IndexClient(config);
    this.calendarClient = new CalendarClient(config);
    this.statementsClient = new StatementsClient(config);
  }

  // Stock Data API Methods
  override async getListedInfo(params?: JQuantsListedInfoParams): Promise<JQuantsListedInfoResponse> {
    return this.stockDataClient.getListedInfo(params);
  }

  override async getDailyQuotes(params: JQuantsDailyQuotesParams): Promise<JQuantsDailyQuotesResponse> {
    return this.stockDataClient.getDailyQuotes(params);
  }

  override async getWeeklyMarginInterest(
    params: JQuantsWeeklyMarginInterestParams
  ): Promise<JQuantsWeeklyMarginInterestResponse> {
    return this.stockDataClient.getWeeklyMarginInterest(params);
  }

  // Index API Methods
  override async getIndices(params: JQuantsIndicesParams): Promise<JQuantsIndicesResponse> {
    return this.indexClient.getIndices(params);
  }

  async getTOPIX(params?: JQuantsTOPIXParams): Promise<JQuantsTOPIXResponse> {
    return this.indexClient.getTOPIX(params);
  }

  // Trading Calendar API Methods
  async getTradingCalendar(params?: JQuantsTradingCalendarParams): Promise<JQuantsTradingCalendarResponse> {
    return this.calendarClient.getTradingCalendar(params);
  }

  // Financial Statements API Methods
  async getStatements(params: JQuantsStatementsParams): Promise<JQuantsStatementsResponse> {
    return this.statementsClient.getStatements(params);
  }

  // Access to specialized clients for advanced usage
  get stocks(): StockDataClient {
    return this.stockDataClient;
  }

  get indices(): IndexClient {
    return this.indexClient;
  }

  get calendar(): CalendarClient {
    return this.calendarClient;
  }

  get statements(): StatementsClient {
    return this.statementsClient;
  }
}
