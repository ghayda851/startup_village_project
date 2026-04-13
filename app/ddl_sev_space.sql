CREATE SCHEMA IF NOT EXISTS srv;

CREATE TABLE IF NOT EXISTS srv.space_room_current (
  room_nk TEXT PRIMARY KEY,
  site TEXT,
  floor TEXT,
  room TEXT,
  occupancy_status TEXT,
  organization_name TEXT,
  organization_type TEXT,
  space_type TEXT,
  activity TEXT,
  employee_count INT,
  total_capacity INT,
  area_m2 DOUBLE PRECISION,
  occupancy_rate_pct DOUBLE PRECISION,
  density_emp_per_100m2 DOUBLE PRECISION,
  is_over_capacity BOOLEAN,
  _ingestion_date DATE,
  last_updated_ts TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS srv.space_kpi_global_current (
  villages_count INT,
  total_spaces INT,
  total_area_m2 DOUBLE PRECISION,
  total_capacity BIGINT,
  total_employees BIGINT,
  leased_spaces BIGINT,
  non_leased_spaces BIGINT,
  reservation_spaces BIGINT,
  per_person_spaces BIGINT,
  underutilized_spaces_lt_40pct BIGINT,
  overoccupied_spaces_gt_100pct BIGINT,
  avg_density_emp_per_100m2 DOUBLE PRECISION,
  weighted_occupancy_rate_pct DOUBLE PRECISION,
  _as_of_ts TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS srv.space_kpi_by_site_current (
  site TEXT PRIMARY KEY,
  total_spaces INT,
  total_area_m2 DOUBLE PRECISION,
  total_capacity BIGINT,
  total_employees BIGINT,
  leased_spaces BIGINT,
  non_leased_spaces BIGINT,
  underutilized_spaces_lt_40pct BIGINT,
  overoccupied_spaces_gt_100pct BIGINT,
  avg_density_emp_per_100m2 DOUBLE PRECISION,
  weighted_occupancy_rate_pct DOUBLE PRECISION,
  _as_of_ts TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS srv.space_kpi_by_site_space_type_current (
  site TEXT,
  space_type TEXT,
  spaces_count INT,
  sum_area_m2 DOUBLE PRECISION,
  sum_capacity BIGINT,
  sum_employees BIGINT,
  _as_of_ts TIMESTAMPTZ,
  PRIMARY KEY (site, space_type)
);

CREATE TABLE IF NOT EXISTS srv.space_kpi_by_site_org_type_current (
  site TEXT,
  organization_type TEXT,
  spaces_count INT,
  sum_employees BIGINT,
  sum_area_m2 DOUBLE PRECISION,
  sum_capacity BIGINT,
  _as_of_ts TIMESTAMPTZ,
  PRIMARY KEY (site, organization_type)
);

CREATE INDEX IF NOT EXISTS ix_space_room_site ON srv.space_room_current (site);
CREATE INDEX IF NOT EXISTS ix_space_room_site_floor ON srv.space_room_current (site, floor);
CREATE INDEX IF NOT EXISTS ix_space_room_occupancy_status ON srv.space_room_current (occupancy_status);
CREATE INDEX IF NOT EXISTS ix_space_room_space_type ON srv.space_room_current (space_type);
CREATE INDEX IF NOT EXISTS ix_space_room_org_type ON srv.space_room_current (organization_type);



