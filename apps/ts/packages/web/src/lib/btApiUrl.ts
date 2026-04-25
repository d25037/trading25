export const DEFAULT_BT_API_URL = 'http://localhost:3002';

export function resolveBtApiUrl(env: Record<string, string | undefined>): string {
  const btApiUrl = env.BT_API_URL?.trim();
  return btApiUrl ? btApiUrl.replace(/\/+$/, '') : DEFAULT_BT_API_URL;
}
