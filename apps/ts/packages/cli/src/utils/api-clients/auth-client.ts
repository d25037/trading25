import { BaseApiClient } from './base-client.js';
import type { AuthStatusResponse, RefreshTokenResponse } from './types.js';

export class AuthClient extends BaseApiClient {
  /**
   * Get JQuants authentication status
   */
  async getAuthStatus(): Promise<AuthStatusResponse> {
    return this.request<AuthStatusResponse>('/api/jquants/auth/status');
  }

  /**
   * Refresh JQuants authentication tokens
   */
  async refreshTokens(params: {
    mailAddress?: string;
    password?: string;
    refreshToken?: string;
  }): Promise<RefreshTokenResponse> {
    return this.request<RefreshTokenResponse>('/api/jquants/auth/refresh', {
      method: 'POST',
      body: JSON.stringify(params),
    });
  }
}
