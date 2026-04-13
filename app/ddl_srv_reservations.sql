CREATE SCHEMA IF NOT EXISTS srv;

CREATE TABLE IF NOT EXISTS srv.reservations_enriched_current (
    reservation_id BIGINT,
    user_id BIGINT,
    user_login TEXT,
    user_full_name TEXT,
    reservation_item_id BIGINT,
    reservation_item_name TEXT,
    group_id BIGINT,
    start_ts TIMESTAMPTZ,
    end_ts TIMESTAMPTZ,
    duration_hours DOUBLE PRECISION,
    is_invalid_duration BOOLEAN,
    comment TEXT,
    start_date DATE,
    start_month TEXT,
    start_year BIGINT,
    start_hour BIGINT,
    start_hour_2h_window TEXT,
    ingestion_date DATE,
    _source_file TEXT,
    _source_system TEXT,
    _ingest_bronze_ts TIMESTAMPTZ,
    _ingest_silver_ts TIMESTAMPTZ,
    _ingest_gold_ts TIMESTAMPTZ
);

CREATE INDEX idx_res_enriched_start_ts ON srv.reservations_enriched_current(start_ts);
CREATE INDEX idx_res_enriched_item_id ON srv.reservations_enriched_current(reservation_item_id);
CREATE INDEX idx_res_enriched_user_id ON srv.reservations_enriched_current(user_id);
CREATE INDEX idx_res_enriched_start_month ON srv.reservations_enriched_current(start_month);
CREATE INDEX idx_res_enriched_ingestion_date ON srv.reservations_enriched_current(ingestion_date);

CREATE TABLE IF NOT EXISTS srv.reservations_kpi_cards_current (
    total_reservations BIGINT,
    reserved_hours DOUBLE PRECISION,
    avg_duration_hours DOUBLE PRECISION,
    invalid_duration_count BIGINT,
    distinct_items BIGINT,
    distinct_users BIGINT,
    gold_kpi_build_ts TIMESTAMPTZ
);

CREATE INDEX idx_kpi_cards_start_month ON srv.reservations_kpi_cards_current(total_reservations);

CREATE TABLE IF NOT EXISTS srv.reservations_kpi_peak_period_current (
    start_hour_2h_window TEXT,
    reservations_count BIGINT,
    gold_kpi_build_ts TIMESTAMPTZ
);

CREATE INDEX idx_kpi_peak_start_hour ON srv.reservations_kpi_peak_period_current(start_hour_2h_window);

CREATE TABLE IF NOT EXISTS srv.reservations_kpi_trends_by_month (
    start_month TEXT,
    reservations_count BIGINT,
    reserved_hours DOUBLE PRECISION,
    avg_duration_hours DOUBLE PRECISION,
    invalid_duration_count BIGINT,
    gold_kpi_build_ts TIMESTAMPTZ
);

CREATE INDEX idx_kpi_trends_start_month ON srv.reservations_kpi_trends_by_month(start_month);

CREATE TABLE IF NOT EXISTS srv.reservations_kpi_by_item_current (
    reservation_item_id BIGINT,
    reservation_item_name TEXT,
    reservations_count BIGINT,
    reserved_hours DOUBLE PRECISION,
    avg_duration_hours DOUBLE PRECISION,
    gold_kpi_build_ts TIMESTAMPTZ
);

CREATE INDEX idx_kpi_by_item_item_id ON srv.reservations_kpi_by_item_current(reservation_item_id);

CREATE TABLE IF NOT EXISTS srv.reservations_kpi_by_user_current (
    user_id BIGINT,
    user_login TEXT,
    user_full_name TEXT,
    reservations_count BIGINT,
    reserved_hours DOUBLE PRECISION,
    avg_duration_hours DOUBLE PRECISION,
    gold_kpi_build_ts TIMESTAMPTZ
);

CREATE INDEX idx_kpi_by_user_user_id ON srv.reservations_kpi_by_user_current(user_id);

CREATE TABLE IF NOT EXISTS srv.reservations_kpi_duration_distribution_current (
    duration_bucket TEXT,
    reservations_count BIGINT,
    gold_kpi_build_ts TIMESTAMPTZ
);

CREATE INDEX idx_kpi_duration_bucket ON srv.reservations_kpi_duration_distribution_current(duration_bucket);

CREATE TABLE IF NOT EXISTS srv.reservations_kpi_invalid_current (
    reservation_id BIGINT,
    user_id BIGINT,
    user_login TEXT,
    user_full_name TEXT,
    reservation_item_id BIGINT,
    reservation_item_name TEXT,
    start_ts TIMESTAMPTZ,
    end_ts TIMESTAMPTZ,
    ingestion_date DATE,
    _source_file TEXT,
    gold_kpi_build_ts TIMESTAMPTZ
);