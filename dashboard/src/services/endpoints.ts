import axiosInstance from './api';
import type {
  SpaceKpiGlobal,
  SpaceKpiBySite,
  SpaceKpiBySpaceType,
  SpaceKpiByOrgType,
  SpaceRoom,
  TicketCard,
  TicketByMonth,
  TicketByYear,
  TicketPriority,
  TicketCategory,
  TicketLocation,
  TicketTechnician,
  TicketHeatmapData,
  ReservationGlobalKpi,
  ReservationCurrent,
  ReservationByItem,
  ReservationByUser,
  ReservationTrendByMonth,
  ReservationPeakPeriod,
  ReservationDurationDistribution,
} from '../types';

// Axios already unwraps the response.data for us, so just return it directly
const extractData = <T>(response: T): T => {
  return response;
};

// ================== SPACE ENDPOINTS ==================

export const spaceApi = {
  async getGlobalKpi(): Promise<SpaceKpiGlobal> {
    const { data } = await axiosInstance.get('/api/space/global');
    return extractData(data);
  },

  async getKpiBySize(): Promise<SpaceKpiBySite[]> {
    const { data } = await axiosInstance.get('/api/space/by-site');
    return extractData(data);
  },

  async getKpiBySpaceType(site: string): Promise<SpaceKpiBySpaceType[]> {
    const { data } = await axiosInstance.get('/api/space/by-site-space-type', {
      params: { site },
    });
    return extractData(data);
  },

  async getKpiByOrgType(site: string): Promise<SpaceKpiByOrgType[]> {
    const { data } = await axiosInstance.get('/api/space/by-site-org-type', {
      params: { site },
    });
    return extractData(data);
  },

  async getRooms(limit: number = 500): Promise<SpaceRoom[]> {
    const { data } = await axiosInstance.get('/api/space/rooms', {
      params: { limit },
    });
    return extractData(data);
  },
};

// ================== TICKETS ENDPOINTS ==================

export const ticketsApi = {
  async getCards(period: string = 'ALL'): Promise<TicketCard> {
    const { data } = await axiosInstance.get('/api/tickets/cards', {
      params: { period },
    });
    return extractData(data);
  },

  async getByMonth(period: string = 'ALL'): Promise<TicketByMonth[]> {
    const { data } = await axiosInstance.get('/api/tickets/by-month', {
      params: { period },
    });
    return extractData(data);
  },

  async getByYear(period: string = 'ALL'): Promise<TicketByYear[]> {
    const { data } = await axiosInstance.get('/api/tickets/by-year', {
      params: { period },
    });
    return extractData(data);
  },

  async getPriority(period: string = 'ALL'): Promise<TicketPriority[]> {
    const { data } = await axiosInstance.get('/api/tickets/priority', {
      params: { period },
    });
    const result = extractData(data);
    return Array.isArray(result) ? result.sort((a, b) => b.priority_code - a.priority_code) : [];
  },

  async getCategory(period: string = 'ALL'): Promise<TicketCategory[]> {
    const { data } = await axiosInstance.get('/api/tickets/category', {
      params: { period },
    });
    const result = extractData(data);
    // Sort by ticket_count descending
    return Array.isArray(result) ? result.sort((a, b) => b.ticket_count - a.ticket_count) : [];
  },

  async getLocation(period: string = 'ALL'): Promise<TicketLocation[]> {
    const { data } = await axiosInstance.get('/api/tickets/location', {
      params: { period },
    });
    return extractData(data);
  },

  async getByTechnician(period: string = 'ALL'): Promise<TicketTechnician[]> {
    const { data } = await axiosInstance.get('/api/tickets/by-technician', {
      params: { period },
    });
    const result = extractData(data);
    // Sort by ticket_count descending, show top technicians first
    return Array.isArray(result) ? result.sort((a, b) => b.ticket_count - a.ticket_count) : [];
  },

  async getHeatmap(period: string = 'ALL'): Promise<TicketHeatmapData[]> {
    const { data } = await axiosInstance.get('/api/tickets/heatmap', {
      params: { period },
    });
    return extractData(data);
  },
};

// ================== RESERVATIONS ENDPOINTS ==================

export const reservationsApi = {
  async getGlobalKpi(): Promise<ReservationGlobalKpi> {
    const { data } = await axiosInstance.get('/api/reservations/global-kpi');
    return extractData(data);
  },

  async getCurrent(limit: number = 500): Promise<ReservationCurrent[]> {
    const { data } = await axiosInstance.get('/api/reservations/current', {
      params: { limit },
    });
    return extractData(data);
  },

  async getByItem(limit: number = 500): Promise<ReservationByItem[]> {
    const { data } = await axiosInstance.get('/api/reservations/by-item', {
      params: { limit },
    });
    return extractData(data);
  },

  async getByUser(limit: number = 10): Promise<ReservationByUser[]> {
    const { data } = await axiosInstance.get('/api/reservations/by-user', {
      params: { limit },
    });
    return extractData(data);
  },

  async getTrendsByMonth(): Promise<ReservationTrendByMonth[]> {
    const { data } = await axiosInstance.get('/api/reservations/trends-by-month');
    return extractData(data);
  },

  async getPeakPeriods(): Promise<ReservationPeakPeriod[]> {
    const { data } = await axiosInstance.get('/api/reservations/peak-periods');
    return extractData(data);
  },

  async getDurationDistribution(): Promise<ReservationDurationDistribution[]> {
    const { data } = await axiosInstance.get('/api/reservations/duration-distribution');
    return extractData(data);
  },

  async getByUserId(userId: number, limit: number = 500): Promise<ReservationCurrent[]> {
    const { data } = await axiosInstance.get(`/api/reservations/by-user/${userId}`, {
      params: { limit },
    });
    return extractData(data);
  },

  async getByItemId(itemId: number, limit: number = 500): Promise<ReservationCurrent[]> {
    const { data } = await axiosInstance.get(`/api/reservations/by-item/${itemId}`, {
      params: { limit },
    });
    return extractData(data);
  },
};
