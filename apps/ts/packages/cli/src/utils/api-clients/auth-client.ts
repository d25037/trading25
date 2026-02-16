import { BaseApiClient } from './base-client.js';
import type { AuthStatusResponse } from './types.js';

export class AuthClient extends BaseApiClient {
  /**
   * Get JQuants authentication status
   */
  async getAuthStatus(): Promise<AuthStatusResponse> {
    return this.request<AuthStatusResponse>('/api/jquants/auth/status');
  }
}
