import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { HttpRequestError } from '@trading25/api-clients/base/http-client';
import type { ReactNode } from 'react';
import { ApiError } from '@/lib/api-client';

/**
 * Determine if an error should be retried
 * - Don't retry client errors (4xx) except for 408 (Timeout) and 429 (Too Many Requests)
 * - Retry server errors (5xx) and network errors
 */
function isRetryableHttpStatus(status: number): boolean {
  return status === 408 || status === 429 || (status >= 500 && status < 600);
}

export function shouldRetry(failureCount: number, error: unknown): boolean {
  if (failureCount >= 3) {
    return false;
  }

  if (error instanceof ApiError) {
    return isRetryableHttpStatus(error.status);
  }

  if (error instanceof HttpRequestError) {
    if (error.kind === 'network' || error.kind === 'timeout') {
      return true;
    }
    return error.kind === 'http' && error.status !== undefined && isRetryableHttpStatus(error.status);
  }

  return true;
}

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 30 * 1000, // 30 seconds
      gcTime: 5 * 60 * 1000, // 5 minutes
      retry: shouldRetry,
    },
  },
});

interface QueryProviderProps {
  children: ReactNode;
}

export function QueryProvider({ children }: QueryProviderProps) {
  return <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>;
}
