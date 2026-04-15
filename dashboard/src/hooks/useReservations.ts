import { useQuery, type UseQueryOptions } from '@tanstack/react-query';
import { reservationsApi } from '../services/endpoints';
import type {
  ReservationGlobalKpi,
  ReservationCurrent,
  ReservationByItem,
  ReservationByUser,
  ReservationTrendByMonth,
  ReservationPeakPeriod,
  ReservationDurationDistribution,
} from '../types';

// ================== RESERVATION QUERIES ==================

export const useReservationGlobalKpi = (
  options?: UseQueryOptions<ReservationGlobalKpi>
) => {
  return useQuery({
    queryKey: ['reservations', 'global-kpi'],
    queryFn: () => reservationsApi.getGlobalKpi(),
    retry: 2,
    staleTime: 5 * 60 * 1000,
    gcTime: 30 * 60 * 1000,
    ...options,
  });
};

export const useReservationCurrent = (
  limit: number = 500,
  options?: UseQueryOptions<ReservationCurrent[]>
) => {
  return useQuery({
    queryKey: ['reservations', 'current', limit],
    queryFn: () => reservationsApi.getCurrent(limit),
    retry: 2,
    staleTime: 5 * 60 * 1000,
    gcTime: 30 * 60 * 1000,
    ...options,
  });
};

export const useReservationByItem = (
  limit: number = 500,
  options?: UseQueryOptions<ReservationByItem[]>
) => {
  return useQuery({
    queryKey: ['reservations', 'by-item', limit],
    queryFn: () => reservationsApi.getByItem(limit),
    retry: 2,
    staleTime: 5 * 60 * 1000,
    gcTime: 30 * 60 * 1000,
    ...options,
  });
};

export const useReservationByUser = (
  limit: number = 10,
  options?: UseQueryOptions<ReservationByUser[]>
) => {
  return useQuery({
    queryKey: ['reservations', 'by-user', limit],
    queryFn: () => reservationsApi.getByUser(limit),
    retry: 2,
    staleTime: 5 * 60 * 1000,
    gcTime: 30 * 60 * 1000,
    ...options,
  });
};

export const useReservationTrendsByMonth = (
  options?: UseQueryOptions<ReservationTrendByMonth[]>
) => {
  return useQuery({
    queryKey: ['reservations', 'trends-by-month'],
    queryFn: () => reservationsApi.getTrendsByMonth(),
    retry: 2,
    staleTime: 5 * 60 * 1000,
    gcTime: 30 * 60 * 1000,
    ...options,
  });
};

export const useReservationPeakPeriods = (
  options?: UseQueryOptions<ReservationPeakPeriod[]>
) => {
  return useQuery({
    queryKey: ['reservations', 'peak-periods'],
    queryFn: () => reservationsApi.getPeakPeriods(),
    retry: 2,
    staleTime: 5 * 60 * 1000,
    gcTime: 30 * 60 * 1000,
    ...options,
  });
};

export const useReservationDurationDistribution = (
  options?: UseQueryOptions<ReservationDurationDistribution[]>
) => {
  return useQuery({
    queryKey: ['reservations', 'duration-distribution'],
    queryFn: () => reservationsApi.getDurationDistribution(),
    retry: 2,
    staleTime: 5 * 60 * 1000,
    gcTime: 30 * 60 * 1000,
    ...options,
  });
};

export const useReservationByUserId = (
  userId: number,
  limit: number = 500,
  options?: UseQueryOptions<ReservationCurrent[]>
) => {
  return useQuery({
    queryKey: ['reservations', 'by-user-id', userId, limit],
    queryFn: () => reservationsApi.getByUserId(userId, limit),
    retry: 2,
    staleTime: 5 * 60 * 1000,
    gcTime: 30 * 60 * 1000,
    ...options,
  });
};

export const useReservationByItemId = (
  itemId: number,
  limit: number = 500,
  options?: UseQueryOptions<ReservationCurrent[]>
) => {
  return useQuery({
    queryKey: ['reservations', 'by-item-id', itemId, limit],
    queryFn: () => reservationsApi.getByItemId(itemId, limit),
    retry: 2,
    staleTime: 5 * 60 * 1000,
    gcTime: 30 * 60 * 1000,
    ...options,
  });
};