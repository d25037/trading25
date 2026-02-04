import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { apiDelete, apiGet, apiPost, apiPut } from '@/lib/api-client';
import type {
  CreatePortfolioItemRequest,
  CreatePortfolioRequest,
  DeleteSuccessResponse,
  ListPortfoliosResponse,
  PortfolioItem,
  PortfolioSummary,
  PortfolioWithItems,
  UpdatePortfolioItemRequest,
  UpdatePortfolioRequest,
} from '@/types/portfolio';
import { logger } from '@/utils/logger';

// Fetch functions
const fetchPortfolios = () => apiGet<ListPortfoliosResponse>('/api/portfolio');

const fetchPortfolioWithItems = (id: number) => apiGet<PortfolioWithItems>(`/api/portfolio/${id}`);

// Mutation functions
const createPortfolio = (data: CreatePortfolioRequest) => apiPost<PortfolioSummary>('/api/portfolio', data);

const updatePortfolio = (id: number, data: UpdatePortfolioRequest) =>
  apiPut<PortfolioSummary>(`/api/portfolio/${id}`, data);

const deletePortfolio = (id: number) => apiDelete<DeleteSuccessResponse>(`/api/portfolio/${id}`);

const addPortfolioItem = (portfolioId: number, data: CreatePortfolioItemRequest) =>
  apiPost<PortfolioItem>(`/api/portfolio/${portfolioId}/items`, data);

const updatePortfolioItem = (portfolioId: number, itemId: number, data: UpdatePortfolioItemRequest) =>
  apiPut<PortfolioItem>(`/api/portfolio/${portfolioId}/items/${itemId}`, data);

const deletePortfolioItem = (portfolioId: number, itemId: number) =>
  apiDelete<DeleteSuccessResponse>(`/api/portfolio/${portfolioId}/items/${itemId}`);

// Query hooks
export function usePortfolios() {
  return useQuery({
    queryKey: ['portfolios'],
    queryFn: () => {
      logger.debug('Fetching portfolios list');
      return fetchPortfolios();
    },
    staleTime: 30 * 1000, // 30 seconds
    gcTime: 5 * 60 * 1000, // 5 minutes
  });
}

export function usePortfolioWithItems(id: number | null) {
  return useQuery({
    queryKey: ['portfolio', id],
    queryFn: () => {
      if (id === null) {
        throw new Error('Portfolio ID is required');
      }
      logger.debug('Fetching portfolio details', { id });
      return fetchPortfolioWithItems(id);
    },
    enabled: id !== null,
    staleTime: 30 * 1000,
    gcTime: 5 * 60 * 1000,
  });
}

// Mutation hooks
export function useCreatePortfolio() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: createPortfolio,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['portfolios'] });
      logger.debug('Portfolio created successfully');
    },
    onError: (error) => {
      logger.error('Failed to create portfolio', { error: error.message });
    },
  });
}

export function useUpdatePortfolio() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ id, data }: { id: number; data: UpdatePortfolioRequest }) => updatePortfolio(id, data),
    onSuccess: (_, variables) => {
      queryClient.invalidateQueries({ queryKey: ['portfolios'] });
      queryClient.invalidateQueries({ queryKey: ['portfolio', variables.id] });
      logger.debug('Portfolio updated successfully');
    },
    onError: (error) => {
      logger.error('Failed to update portfolio', { error: error.message });
    },
  });
}

export function useDeletePortfolio() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: deletePortfolio,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['portfolios'] });
      logger.debug('Portfolio deleted successfully');
    },
    onError: (error) => {
      logger.error('Failed to delete portfolio', { error: error.message });
    },
  });
}

export function useAddPortfolioItem() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ portfolioId, data }: { portfolioId: number; data: CreatePortfolioItemRequest }) =>
      addPortfolioItem(portfolioId, data),
    onSuccess: (_, variables) => {
      queryClient.invalidateQueries({ queryKey: ['portfolios'] });
      queryClient.invalidateQueries({ queryKey: ['portfolio', variables.portfolioId] });
      logger.debug('Stock added to portfolio successfully');
    },
    onError: (error) => {
      logger.error('Failed to add stock to portfolio', { error: error.message });
    },
  });
}

export function useUpdatePortfolioItem() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({
      portfolioId,
      itemId,
      data,
    }: {
      portfolioId: number;
      itemId: number;
      data: UpdatePortfolioItemRequest;
    }) => updatePortfolioItem(portfolioId, itemId, data),
    onSuccess: (_, variables) => {
      queryClient.invalidateQueries({ queryKey: ['portfolios'] });
      queryClient.invalidateQueries({ queryKey: ['portfolio', variables.portfolioId] });
      logger.debug('Portfolio item updated successfully');
    },
    onError: (error) => {
      logger.error('Failed to update portfolio item', { error: error.message });
    },
  });
}

export function useDeletePortfolioItem() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ portfolioId, itemId }: { portfolioId: number; itemId: number }) =>
      deletePortfolioItem(portfolioId, itemId),
    onSuccess: (_, variables) => {
      queryClient.invalidateQueries({ queryKey: ['portfolios'] });
      queryClient.invalidateQueries({ queryKey: ['portfolio', variables.portfolioId] });
      logger.debug('Stock removed from portfolio successfully');
    },
    onError: (error) => {
      logger.error('Failed to remove stock from portfolio', { error: error.message });
    },
  });
}
