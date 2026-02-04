import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import type { ReactNode } from 'react';
import { ApiError } from '@/lib/api-client';

/**
 * Determine if an error should be retried
 * - Don't retry client errors (4xx) except for 408 (Timeout) and 429 (Too Many Requests)
 * - Retry server errors (5xx) and network errors
 */
function shouldRetry(failureCount: number, error: Error): boolean {
  // Use ApiError's methods for type-safe status checking
  if (error instanceof ApiError) {
    // Don't retry most client errors
    if (error.isClientError() && error.status !== 408 && error.status !== 429) {
      return false;
    }
  }

  // Retry up to 3 times for server errors and network failures
  return failureCount < 3;
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
