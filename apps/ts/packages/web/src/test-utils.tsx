import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import type { ReactNode } from 'react';

export function createTestQueryClient() {
  return new QueryClient({
    defaultOptions: {
      queries: {
        retry: false,
      },
      mutations: {
        retry: false,
      },
    },
  });
}

export function createQueryWrapper(queryClient: QueryClient) {
  return function QueryWrapper({ children }: { children: ReactNode }) {
    return <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>;
  };
}

/**
 * Creates a fresh QueryClient and wrapper for hook tests.
 * Returns both so tests can spy on queryClient when needed.
 */
export function createTestWrapper(): { queryClient: QueryClient; wrapper: ReturnType<typeof createQueryWrapper> } {
  const queryClient = createTestQueryClient();
  return { queryClient, wrapper: createQueryWrapper(queryClient) };
}
