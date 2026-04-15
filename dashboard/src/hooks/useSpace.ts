import { useQuery, type UseQueryOptions } from '@tanstack/react-query';
import { spaceApi } from '../services/endpoints';
import type {
  SpaceKpiGlobal,
  SpaceKpiBySite,
  SpaceKpiBySpaceType,
  SpaceKpiByOrgType,
  SpaceRoom,
} from '../types';

// ================== SPACE QUERIES ==================

export const useSpaceGlobalKpi = (
  options?: UseQueryOptions<SpaceKpiGlobal>
) => {
  return useQuery({
    queryKey: ['space', 'global'],
    queryFn: () => spaceApi.getGlobalKpi(),
    retry: 2,
    staleTime: 5 * 60 * 1000,
    gcTime: 30 * 60 * 1000,
    ...options,
  });
};

export const useSpaceKpiBySize = (
  options?: UseQueryOptions<SpaceKpiBySite[]>
) => {
  return useQuery({
    queryKey: ['space', 'by-site'],
    queryFn: () => spaceApi.getKpiBySize(),
    retry: 2,
    staleTime: 5 * 60 * 1000,
    gcTime: 30 * 60 * 1000,
    ...options,
  });
};

export const useSpaceKpiBySpaceType = (
  site: string,
  options?: UseQueryOptions<SpaceKpiBySpaceType[]>
) => {
  return useQuery({
    queryKey: ['space', 'by-site-space-type', site],
    queryFn: () => spaceApi.getKpiBySpaceType(site),
    enabled: !!site,
    retry: 1,
    staleTime: 5 * 60 * 1000,
    gcTime: 30 * 60 * 1000,
    ...options,
  });
};

export const useSpaceKpiByOrgType = (
  site: string,
  options?: UseQueryOptions<SpaceKpiByOrgType[]>
) => {
  return useQuery({
    queryKey: ['space', 'by-site-org-type', site],
    queryFn: () => spaceApi.getKpiByOrgType(site),
    enabled: !!site,
    retry: 1,
    staleTime: 5 * 60 * 1000,
    gcTime: 30 * 60 * 1000,
    ...options,
  });
};

export const useSpaceRooms = (
  limit: number = 500,
  options?: UseQueryOptions<SpaceRoom[]>
) => {
  return useQuery({
    queryKey: ['space', 'rooms', limit],
    queryFn: () => spaceApi.getRooms(limit),
    retry: 1,
    staleTime: 5 * 60 * 1000,
    gcTime: 30 * 60 * 1000,
    ...options,
  });
};
