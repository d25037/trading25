import { BaseApiClient } from './base-client.js';
import type {
  DeleteResponse,
  ListPortfoliosResponse,
  PortfolioItemDeletedResponse,
  PortfolioItemResponse,
  PortfolioResponse,
  PortfolioWithItemsResponse,
} from './types.js';

export class PortfolioClient extends BaseApiClient {
  /**
   * List all portfolios with summary statistics
   */
  async listPortfolios(): Promise<ListPortfoliosResponse> {
    return this.request<ListPortfoliosResponse>('/api/portfolio');
  }

  /**
   * Create a new portfolio
   */
  async createPortfolio(data: { name: string; description?: string }): Promise<PortfolioResponse> {
    return this.request<PortfolioResponse>('/api/portfolio', {
      method: 'POST',
      body: JSON.stringify(data),
    });
  }

  /**
   * Get portfolio with all items
   */
  async getPortfolio(id: number): Promise<PortfolioWithItemsResponse> {
    return this.request<PortfolioWithItemsResponse>(`/api/portfolio/${id}`);
  }

  /**
   * Update portfolio
   */
  async updatePortfolio(id: number, data: { name?: string; description?: string }): Promise<PortfolioResponse> {
    return this.request<PortfolioResponse>(`/api/portfolio/${id}`, {
      method: 'PUT',
      body: JSON.stringify(data),
    });
  }

  /**
   * Delete portfolio
   */
  async deletePortfolio(id: number): Promise<DeleteResponse> {
    return this.request<DeleteResponse>(`/api/portfolio/${id}`, {
      method: 'DELETE',
    });
  }

  /**
   * Add item to portfolio
   */
  async addPortfolioItem(
    portfolioId: number,
    data: {
      code: string;
      companyName?: string;
      quantity: number;
      purchasePrice: number;
      purchaseDate: string;
      account?: string;
      notes?: string;
    }
  ): Promise<PortfolioItemResponse> {
    return this.request<PortfolioItemResponse>(`/api/portfolio/${portfolioId}/items`, {
      method: 'POST',
      body: JSON.stringify(data),
    });
  }

  /**
   * Update portfolio item by ID
   */
  async updatePortfolioItem(
    portfolioId: number,
    itemId: number,
    data: {
      quantity?: number;
      purchasePrice?: number;
      purchaseDate?: string;
      account?: string;
      notes?: string;
    }
  ): Promise<PortfolioItemResponse> {
    return this.request<PortfolioItemResponse>(`/api/portfolio/${portfolioId}/items/${itemId}`, {
      method: 'PUT',
      body: JSON.stringify(data),
    });
  }

  /**
   * Delete portfolio item by ID
   */
  async deletePortfolioItem(portfolioId: number, itemId: number): Promise<DeleteResponse> {
    return this.request<DeleteResponse>(`/api/portfolio/${portfolioId}/items/${itemId}`, {
      method: 'DELETE',
    });
  }

  /**
   * Update stock in portfolio by name and code
   */
  async updatePortfolioStock(
    portfolioName: string,
    code: string,
    data: {
      quantity?: number;
      purchasePrice?: number;
      purchaseDate?: string;
      account?: string;
      notes?: string;
    }
  ): Promise<PortfolioItemResponse> {
    return this.request<PortfolioItemResponse>(`/api/portfolio/${encodeURIComponent(portfolioName)}/stocks/${code}`, {
      method: 'PUT',
      body: JSON.stringify(data),
    });
  }

  /**
   * Delete stock from portfolio by name and code
   */
  async deletePortfolioStock(portfolioName: string, code: string): Promise<PortfolioItemDeletedResponse> {
    return this.request<PortfolioItemDeletedResponse>(`/api/portfolio/${encodeURIComponent(portfolioName)}/stocks/${code}`, {
      method: 'DELETE',
    });
  }
}
