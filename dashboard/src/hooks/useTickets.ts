import { useQuery, type UseQueryOptions } from '@tanstack/react-query';
import { ticketsApi } from '../services/endpoints';
import type {
  TicketCard,
  TicketByMonth,
  TicketByYear,
  TicketPriority,
  TicketCategory,
  TicketLocation,
  TicketTechnician,
  TicketHeatmapData,
} from '../types';

// ================== TICKETS QUERIES ==================

export const useTicketCards = (
  period: string = 'ALL',
  options?: UseQueryOptions<TicketCard>
) => {
  return useQuery({
    queryKey: ['tickets', 'cards', period],
    queryFn: () => ticketsApi.getCards(period),
    retry: 2,
    staleTime: 5 * 60 * 1000,
    gcTime: 30 * 60 * 1000,
    ...options,
  });
};

export const useTicketsByMonth = (
  period: string = 'ALL',
  options?: UseQueryOptions<TicketByMonth[]>
) => {
  return useQuery({
    queryKey: ['tickets', 'by-month', period],
    queryFn: () => ticketsApi.getByMonth(period),
    retry: 2,
    staleTime: 5 * 60 * 1000,
    gcTime: 30 * 60 * 1000,
    ...options,
  });
};

export const useTicketsByYear = (
  period: string = 'ALL',
  options?: UseQueryOptions<TicketByYear[]>
) => {
  return useQuery({
    queryKey: ['tickets', 'by-year', period],
    retry: 2,
    queryFn: () => ticketsApi.getByYear(period),
    staleTime: 5 * 60 * 1000,
    gcTime: 30 * 60 * 1000,
    ...options,
  });
};

export const useTicketPriority = (
  period: string = 'ALL',
  options?: UseQueryOptions<TicketPriority[]>
) => {
  return useQuery({
    queryKey: ['tickets', 'priority', period],
    retry: 2,
    queryFn: () => ticketsApi.getPriority(period),
    staleTime: 5 * 60 * 1000,
    gcTime: 30 * 60 * 1000,
    ...options,
  });
};

export const useTicketCategory = (
  period: string = 'ALL',
  options?: UseQueryOptions<TicketCategory[]>
) => {
  return useQuery({
    queryKey: ['tickets', 'category', period],
    queryFn: () => ticketsApi.getCategory(period),
    retry: 2,
    staleTime: 5 * 60 * 1000,
    gcTime: 30 * 60 * 1000,
    ...options,
  });
};

export const useTicketLocation = (
  period: string = 'ALL',
  options?: UseQueryOptions<TicketLocation[]>
) => {
  return useQuery({
    queryKey: ['tickets', 'location', period],
    queryFn: () => ticketsApi.getLocation(period),
    retry: 2,
    staleTime: 5 * 60 * 1000,
    gcTime: 30 * 60 * 1000,
    ...options,
  });
};

export const useTicketByTechnician = (
  period: string = 'ALL',
  options?: UseQueryOptions<TicketTechnician[]>
) => {
  return useQuery({
    queryKey: ['tickets', 'by-technician', period],
    retry: 2,
    queryFn: () => ticketsApi.getByTechnician(period),
    staleTime: 5 * 60 * 1000,
    gcTime: 30 * 60 * 1000,
    ...options,
  });
};

export const useTicketHeatmap = (
  period: string = 'ALL',
  options?: UseQueryOptions<TicketHeatmapData[]>
) => {
  return useQuery({
    queryKey: ['tickets', 'heatmap', period],
    retry: 2,
    queryFn: () => ticketsApi.getHeatmap(period),
    staleTime: 5 * 60 * 1000,
    gcTime: 30 * 60 * 1000,
    ...options,
  });
};
