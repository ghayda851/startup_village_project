from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class ReservationEnrichedCurrentRow(BaseModel):
    reservation_id: int
    user_id: int
    user_login: str
    user_full_name: str
    reservation_item_id: int
    reservation_item_name: str
    group_id: Optional[int] = None
    start_ts: str
    end_ts: str
    duration_hours: float
    is_invalid_duration: bool
    comment: Optional[str] = None
    start_date: str
    start_month: str
    start_year: int
    start_hour: int
    start_hour_2h_window: str
    ingestion_date: str
    _source_file: Optional[str] = None
    _source_system: Optional[str] = None
    _ingest_bronze_ts: Optional[str] = None
    _ingest_silver_ts: Optional[str] = None
    _ingest_gold_ts: Optional[str] = None

class ReservationKpiCards(BaseModel):
    total_reservations: int
    reserved_hours: float
    avg_duration_hours: float
    invalid_duration_count: int
    distinct_items: int
    distinct_users: int
    gold_kpi_build_ts: str

class ReservationKpiPeakPeriod(BaseModel):
    start_hour_2h_window: str
    reservations_count: int
    gold_kpi_build_ts: str

class ReservationKpiTrendByMonth(BaseModel):
    start_month: str
    reservations_count: int
    reserved_hours: float
    avg_duration_hours: float
    invalid_duration_count: int
    gold_kpi_build_ts: str

class ReservationKpiByItem(BaseModel):
    reservation_item_id: int
    reservation_item_name: Optional[str] = None
    reservations_count: int
    reserved_hours: float
    avg_duration_hours: float
    gold_kpi_build_ts: str

class ReservationKpiByUser(BaseModel):
    user_id: int
    user_login: Optional[str] = None
    user_full_name: Optional[str] = None
    reservations_count: int
    reserved_hours: float
    avg_duration_hours: float
    gold_kpi_build_ts: str

class ReservationKpiDurationDistribution(BaseModel):
    duration_bucket: str
    reservations_count: int
    gold_kpi_build_ts: str
