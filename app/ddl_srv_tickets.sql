-- ============================================================
-- StartupVillage Serving DB (PostgreSQL)
-- DDL for Tickets/GLPI Gold -> Serving (schema: srv)
-- Matches Delta column names 1:1 (as provided).
-- ============================================================

CREATE SCHEMA IF NOT EXISTS srv;

-- ----------------------------
-- 1) tickets_enriched
-- ----------------------------
CREATE TABLE IF NOT EXISTS srv.tickets_enriched (
  ticket_id              BIGINT,
  title                  TEXT,
  category_id            BIGINT,
  category               TEXT,
  location_id            BIGINT,
  location               TEXT,
  requester_user_id      BIGINT,
  requester_login        TEXT,
  requester_full_name    TEXT,
  last_updater_user_id   BIGINT,
  last_updater_login     TEXT,
  last_updater_full_name TEXT,
  type_code              BIGINT,
  type                   TEXT,
  status_code            BIGINT,
  status                 TEXT,
  is_open                BOOLEAN,
  is_deleted             BOOLEAN,
  priority_code          BIGINT,
  priority               TEXT,
  created_ts             TIMESTAMPTZ,
  updated_ts             TIMESTAMPTZ,
  solved_ts              TIMESTAMPTZ,
  closed_ts              TIMESTAMPTZ,
  time_to_solve_hours    DOUBLE PRECISION,
  time_to_close_hours    DOUBLE PRECISION,
  _source_file           TEXT,
  _source_system         TEXT,
  ingestion_date         DATE,
  _ingest_bronze_ts      TIMESTAMPTZ,
  _ingest_silver_ts      TIMESTAMPTZ,
  _ingest_gold_ts        TIMESTAMPTZ
);

-- Helpful indexes for typical API filters
CREATE INDEX IF NOT EXISTS ix_tickets_enriched_created_ts
  ON srv.tickets_enriched (created_ts);

CREATE INDEX IF NOT EXISTS ix_tickets_enriched_status_priority
  ON srv.tickets_enriched (status_code, priority_code);

CREATE INDEX IF NOT EXISTS ix_tickets_enriched_location_category
  ON srv.tickets_enriched (location_id, category_id);

CREATE INDEX IF NOT EXISTS ix_tickets_enriched_requester
  ON srv.tickets_enriched (requester_user_id);

CREATE INDEX IF NOT EXISTS ix_tickets_enriched_last_updater
  ON srv.tickets_enriched (last_updater_user_id);

-- ----------------------------
-- 2) kpi_load_by_technician_priority_period
-- ----------------------------
CREATE TABLE IF NOT EXISTS srv.kpi_load_by_technician_priority_period (
  period                TEXT,
  last_updater_user_id  BIGINT,
  last_updater_full_name TEXT,
  priority_code         BIGINT,
  priority              TEXT,
  ticket_count          BIGINT,
  gold_kpi_build_ts     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS ix_kpi_load_tp_period
  ON srv.kpi_load_by_technician_priority_period (period);

CREATE INDEX IF NOT EXISTS ix_kpi_load_tp_tech
  ON srv.kpi_load_by_technician_priority_period (last_updater_user_id);

CREATE INDEX IF NOT EXISTS ix_kpi_load_tp_priority
  ON srv.kpi_load_by_technician_priority_period (priority_code);

-- ----------------------------
-- 3) kpi_priority_distribution_period
-- ----------------------------
CREATE TABLE IF NOT EXISTS srv.kpi_priority_distribution_period (
  period            TEXT,
  priority_code     BIGINT,
  priority          TEXT,
  ticket_count      BIGINT,
  gold_kpi_build_ts TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS ix_kpi_prio_dist_period
  ON srv.kpi_priority_distribution_period (period);

CREATE INDEX IF NOT EXISTS ix_kpi_prio_dist_priority
  ON srv.kpi_priority_distribution_period (priority_code);

-- ----------------------------
-- 4) kpi_tickets_by_category_period
-- ----------------------------
CREATE TABLE IF NOT EXISTS srv.kpi_tickets_by_category_period (
  period            TEXT,
  category_id       BIGINT,
  category          TEXT,
  ticket_count      BIGINT,
  total_tickets     BIGINT,
  share             DOUBLE PRECISION,
  gold_kpi_build_ts TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS ix_kpi_by_cat_period
  ON srv.kpi_tickets_by_category_period (period);

CREATE INDEX IF NOT EXISTS ix_kpi_by_cat_category
  ON srv.kpi_tickets_by_category_period (category_id);

-- ----------------------------
-- 5) kpi_tickets_by_month
-- ----------------------------
CREATE TABLE IF NOT EXISTS srv.kpi_tickets_by_month (
  period            TEXT,
  ticket_count      BIGINT,
  gold_kpi_build_ts TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS ix_kpi_by_month_period
  ON srv.kpi_tickets_by_month (period);

-- ----------------------------
-- 6) kpi_tickets_by_requester_period
-- ----------------------------
CREATE TABLE IF NOT EXISTS srv.kpi_tickets_by_requester_period (
  period              TEXT,
  requester_user_id   BIGINT,
  requester_login     TEXT,
  requester_full_name TEXT,
  ticket_count        BIGINT,
  gold_kpi_build_ts   TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS ix_kpi_by_requester_period
  ON srv.kpi_tickets_by_requester_period (period);

CREATE INDEX IF NOT EXISTS ix_kpi_by_requester_user
  ON srv.kpi_tickets_by_requester_period (requester_user_id);

-- ----------------------------
-- 7) kpi_tickets_by_technician_period
-- ----------------------------
CREATE TABLE IF NOT EXISTS srv.kpi_tickets_by_technician_period (
  period                TEXT,
  last_updater_user_id  BIGINT,
  last_updater_full_name TEXT,
  ticket_count          BIGINT,
  gold_kpi_build_ts     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS ix_kpi_by_tech_period
  ON srv.kpi_tickets_by_technician_period (period);

CREATE INDEX IF NOT EXISTS ix_kpi_by_tech_user
  ON srv.kpi_tickets_by_technician_period (last_updater_user_id);

-- ----------------------------
-- 8) kpi_tickets_by_year
-- ----------------------------
CREATE TABLE IF NOT EXISTS srv.kpi_tickets_by_year (
  year             BIGINT,
  ticket_count     BIGINT,
  gold_kpi_build_ts TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS ix_kpi_by_year_year
  ON srv.kpi_tickets_by_year (year);

-- ----------------------------
-- 9) kpi_tickets_by_location_period
-- ----------------------------
CREATE TABLE IF NOT EXISTS srv.kpi_tickets_by_location_period (
  period            TEXT,
  location_id       BIGINT,
  location          TEXT,
  ticket_count      BIGINT,
  total_tickets     BIGINT,
  share             DOUBLE PRECISION,
  gold_kpi_build_ts TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS ix_kpi_by_loc_period
  ON srv.kpi_tickets_by_location_period (period);

CREATE INDEX IF NOT EXISTS ix_kpi_by_loc_location
  ON srv.kpi_tickets_by_location_period (location_id);

-- ----------------------------
-- 10) tickets_cards
-- ----------------------------
CREATE TABLE IF NOT EXISTS srv.tickets_cards (
  period            TEXT,
  total_tickets     BIGINT,
  is_open           BIGINT,
  is_resolved       BIGINT,
  technicians       BIGINT,
  open_rate         DOUBLE PRECISION,
  gold_kpi_build_ts TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS ix_tickets_cards_period
  ON srv.tickets_cards (period);