// API Response Types
export interface ApiResponse<T> {
  data: T;
  last_refresh?: string;
}

// ================== SPACE TYPES ==================

export interface SpaceKpiGlobal {
  villages_count: number;
  total_spaces: number;
  total_area_m2: number;
  total_capacity: number;
  total_employees: number;
  leased_spaces: number;
  non_leased_spaces: number;
  reservation_spaces: number;
  per_person_spaces: number;
  underutilized_spaces_lt_40pct: number;
  overoccupied_spaces_gt_100pct: number;
  weighted_occupancy_rate_pct: number;
  avg_density_emp_per_100m2: number;
  as_of_ts: string;
}

export interface SpaceKpiBySite {
  site: string;
  total_spaces: number;
  total_area_m2: number;
  total_capacity: number;
  total_employees: number;
  leased_spaces: number;
  non_leased_spaces: number;
  underutilized_spaces_lt_40pct: number;
  overoccupied_spaces_gt_100pct: number;
  avg_density_emp_per_100m2: number;
  weighted_occupancy_rate_pct: number;
  as_of_ts: string;
}

export interface SpaceKpiBySpaceType {
  site: string;
  space_type: string;
  spaces_count: number;
  sum_area_m2: number;
  sum_capacity: number;
  sum_employees: number;
  as_of_ts: string;
}

export interface SpaceKpiByOrgType {
  site: string;
  organization_type: string;
  spaces_count: number;
  sum_employees: number;
  sum_area_m2: number;
  sum_capacity: number;
  as_of_ts: string;
}

export interface SpaceRoom {
  room_nk: string;
  site: string;
  floor: string;
  room: string;
  occupancy_status: string;
  organization_name: string;
  organization_type: string;
  space_type: string;
  activity: string;
  employee_count: number;
  total_capacity: number;
  area_m2: number;
  occupancy_rate_pct: number;
  density_emp_per_100m2: number;
  is_over_capacity: boolean;
  ingestion_date: string;
  last_updated_ts: string;
}

// ================== TICKETS TYPES ==================

export interface TicketCard {
  period: string;
  total_tickets: number;
  is_open: number;
  is_resolved: number;
  technicians: number;
  open_rate: number;
  gold_kpi_build_ts: string;
}

export interface TicketByMonth {
  period: string;
  ticket_count: number;
  gold_kpi_build_ts: string;
}

export interface TicketByYear {
  year: number;
  ticket_count: number;
  gold_kpi_build_ts: string;
}

export interface TicketPriority {
  period: string;
  priority_code: number;
  priority: string;
  ticket_count: number;
  gold_kpi_build_ts: string;
}

export interface TicketCategory {
  period: string;
  category_id: number;
  category: string | null;
  ticket_count: number;
  total_tickets: number;
  share: number;
  gold_kpi_build_ts: string;
}

export interface TicketLocation {
  period: string;
  location_id: number;
  location: string | null;
  ticket_count: number;
  total_tickets: number;
  share: number;
  gold_kpi_build_ts: string;
}

export interface TicketTechnician {
  period: string;
  last_updater_user_id: number;
  last_updater_full_name: string;
  ticket_count: number;
  gold_kpi_build_ts: string;
}

export interface TicketHeatmapData {
  period: string;
  last_updater_user_id: number;
  last_updater_full_name: string;
  priority_code: number;
  priority: string;
  ticket_count: number;
  gold_kpi_build_ts: string;
}

export interface TicketEnriched {
  ticket_id: string;
  title: string;
  description?: string;
  status: string;
  priority: number;
  category: string;
  location: string;
  assigned_technician: string;
  created_date: string;
  last_updated: string;
  resolution_date?: string;
}

// ================== FILTER TYPES ==================

export interface FilterState {
  period: 'ALL' | string; // ALL or YYYY-MM
  site: string | null;
}

// ================== PAGINATION ==================

export interface PaginationParams {
  page: number;
  page_size: number;
  sort_by?: string;
  sort_order?: 'asc' | 'desc';
}

// ================== RESERVATION TYPES ==================

export interface ReservationGlobalKpi {
  total_reservations: number;
  reserved_hours: number;
  avg_duration_hours: number;
  invalid_duration_count: number;
  distinct_items: number;
  distinct_users: number;
  gold_kpi_build_ts: string;
}

export interface ReservationCurrent {
  reservation_id: number;
  user_id: number;
  user_login: string;
  user_full_name: string;
  reservation_item_id: number;
  reservation_item_name: string;
  group_id: number;
  start_ts: string;
  end_ts: string;
  duration_hours: number;
  is_invalid_duration: boolean;
  comment: string;
  start_date: string;
  start_month: string;
  start_year: number;
  start_hour: number;
  start_hour_2h_window: string;
  ingestion_date: string;
}

export interface ReservationByItem {
  reservation_item_id: number;
  reservation_item_name: string;
  reservations_count: number;
  reserved_hours: number;
  avg_duration_hours: number;
  gold_kpi_build_ts: string;
}

export interface ReservationByUser {
  user_id: number;
  user_login: string;
  user_full_name: string;
  reservations_count: number;
  reserved_hours: number;
  avg_duration_hours: number;
  gold_kpi_build_ts: string;
}

export interface ReservationTrendByMonth {
  start_month: string;
  reservations_count: number;
  reserved_hours: number;
  avg_duration_hours: number;
  invalid_duration_count: number;
  gold_kpi_build_ts: string;
}

export interface ReservationPeakPeriod {
  start_hour_2h_window: string;
  reservations_count: number;
  gold_kpi_build_ts: string;
}

export interface ReservationDurationDistribution {
  duration_bucket: string;
  reservations_count: number;
  gold_kpi_build_ts: string;
}
