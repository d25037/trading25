import { BaseApiClient, toQueryString } from './base-client.js';

export class JQuantsClient extends BaseApiClient {
  /**
   * Get daily stock quotes for chart display
   */
  async getDailyQuotes(symbol: string, params?: { from?: string; to?: string; date?: string }) {
    const query = toQueryString({
      from: params?.from,
      to: params?.to,
      date: params?.date,
    });
    const url = `/api/chart/stocks/${symbol}${query ? `?${query}` : ''}`;

    return this.request<{
      data: Array<{
        time: string;
        open: number;
        high: number;
        low: number;
        close: number;
        volume?: number;
      }>;
      symbol: string;
      companyName?: string;
      timeframe: string;
      lastUpdated: string;
    }>(url);
  }

  /**
   * Get listed stock information
   */
  async getListedInfo(params?: { code?: string; date?: string }) {
    const query = toQueryString({
      code: params?.code,
      date: params?.date,
    });
    const url = `/api/jquants/listed-info${query ? `?${query}` : ''}`;

    return this.request<{
      info: Array<{
        code: string;
        companyName: string;
        companyNameEnglish?: string;
        marketCode?: string;
        marketCodeName?: string;
        sector33Code?: string;
        sector33CodeName?: string;
        scaleCategory?: string;
      }>;
      lastUpdated: string;
    }>(url);
  }

  /**
   * Get weekly margin interest data
   */
  async getMarginInterest(symbol: string, params?: { from?: string; to?: string; date?: string }) {
    const query = toQueryString({
      from: params?.from,
      to: params?.to,
      date: params?.date,
    });
    const url = `/api/jquants/stocks/${symbol}/margin-interest${query ? `?${query}` : ''}`;

    return this.request<{
      symbol: string;
      marginInterest: Array<{
        date: string;
        code: string;
        shortMarginTradeVolume: number;
        longMarginTradeVolume: number;
      }>;
      lastUpdated: string;
    }>(url);
  }

  /**
   * Get index data
   */
  async getIndices(params?: { code?: string; from?: string; to?: string; date?: string }) {
    const query = toQueryString({
      code: params?.code,
      from: params?.from,
      to: params?.to,
      date: params?.date,
    });
    const url = `/api/jquants/indices${query ? `?${query}` : ''}`;

    return this.request<{
      indices: Array<{
        date: string;
        code?: string;
        open: number;
        high: number;
        low: number;
        close: number;
      }>;
      lastUpdated: string;
    }>(url);
  }

  /**
   * Get TOPIX index data for chart display
   */
  async getTOPIX(params?: { from?: string; to?: string; date?: string }) {
    const query = toQueryString({
      from: params?.from,
      to: params?.to,
      date: params?.date,
    });
    const url = `/api/chart/indices/topix${query ? `?${query}` : ''}`;

    return this.request<{
      topix: Array<{
        date: string;
        open: number;
        high: number;
        low: number;
        close: number;
        volume: number;
      }>;
      lastUpdated: string;
    }>(url);
  }
}
