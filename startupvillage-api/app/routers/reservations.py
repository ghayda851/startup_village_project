from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from app.core.db import get_db
from app.schemas.reservations import (
    ReservationEnrichedCurrentRow, ReservationKpiCards, ReservationKpiPeakPeriod,
    ReservationKpiTrendByMonth, ReservationKpiByItem, ReservationKpiByUser,
    ReservationKpiDurationDistribution
)

router = APIRouter(prefix="/reservations", tags=["reservations"])

@router.get("/global-kpi", response_model=ReservationKpiCards)
async def global_kpi(db: AsyncSession = Depends(get_db)):
    """Get global reservation KPIs including total reservations, hours, and metrics."""
    q = text("""
        SELECT
            total_reservations,
            reserved_hours,
            avg_duration_hours,
            invalid_duration_count,
            distinct_items,
            distinct_users,
            gold_kpi_build_ts::text
        FROM srv.reservations_kpi_cards_current
        LIMIT 1;
    """)
    row = (await db.execute(q)).mappings().first()
    return row

@router.get("/current", response_model=list[ReservationEnrichedCurrentRow])
async def current_reservations(
    user_id: int | None = None,
    reservation_item_id: int | None = None,
    start_month: str | None = None,
    start_hour: int | None = None,
    is_invalid_duration: bool | None = None,
    limit: int = Query(500, ge=1, le=10000),
    db: AsyncSession = Depends(get_db),
):
    """Get current reservations with optional filtering."""
    where = []
    params = {"limit": limit}

    if user_id is not None:
        where.append("user_id = :user_id")
        params["user_id"] = user_id
    if reservation_item_id is not None:
        where.append("reservation_item_id = :reservation_item_id")
        params["reservation_item_id"] = reservation_item_id
    if start_month is not None:
        where.append("start_month = :start_month")
        params["start_month"] = start_month
    if start_hour is not None:
        where.append("start_hour = :start_hour")
        params["start_hour"] = start_hour
    if is_invalid_duration is not None:
        where.append("is_invalid_duration = :is_invalid_duration")
        params["is_invalid_duration"] = is_invalid_duration

    where_sql = "WHERE " + " AND ".join(where) if where else ""

    q = text(f"""
        SELECT
            reservation_id,
            user_id,
            user_login,
            user_full_name,
            reservation_item_id,
            reservation_item_name,
            group_id,
            start_ts::text,
            end_ts::text,
            duration_hours,
            is_invalid_duration,
            comment,
            start_date::text,
            start_month,
            start_year,
            start_hour,
            start_hour_2h_window,
            ingestion_date::text,
            _source_file,
            _source_system,
            _ingest_bronze_ts::text,
            _ingest_silver_ts::text,
            _ingest_gold_ts::text
        FROM srv.reservations_enriched_current
        {where_sql}
        ORDER BY start_ts DESC
        LIMIT :limit;
    """)
    return (await db.execute(q, params)).mappings().all()

@router.get("/by-item", response_model=list[ReservationKpiByItem])
async def by_item(
    limit: int = Query(500, ge=1, le=10000),
    db: AsyncSession = Depends(get_db),
):
    """Get reservation KPIs grouped by item."""
    q = text("""
        SELECT
            reservation_item_id,
            reservation_item_name,
            reservations_count,
            reserved_hours,
            avg_duration_hours,
            gold_kpi_build_ts::text
        FROM srv.reservations_kpi_by_item_current
        ORDER BY reservations_count DESC
        LIMIT :limit;
    """)
    return (await db.execute(q, {"limit": limit})).mappings().all()

@router.get("/by-user", response_model=list[ReservationKpiByUser])
async def by_user(
    limit: int = Query(500, ge=1, le=10000),
    db: AsyncSession = Depends(get_db),
):
    """Get reservation KPIs grouped by user."""
    q = text("""
        SELECT
            user_id,
            user_login,
            user_full_name,
            reservations_count,
            reserved_hours,
            avg_duration_hours,
            gold_kpi_build_ts::text
        FROM srv.reservations_kpi_by_user_current
        ORDER BY reservations_count DESC
        LIMIT :limit;
    """)
    return (await db.execute(q, {"limit": limit})).mappings().all()

@router.get("/trends-by-month", response_model=list[ReservationKpiTrendByMonth])
async def trends_by_month(db: AsyncSession = Depends(get_db)):
    """Get reservation trends by month."""
    q = text("""
        SELECT
            start_month,
            reservations_count,
            reserved_hours,
            avg_duration_hours,
            invalid_duration_count,
            gold_kpi_build_ts::text
        FROM srv.reservations_kpi_trends_by_month
        ORDER BY start_month DESC;
    """)
    return (await db.execute(q)).mappings().all()

@router.get("/peak-periods", response_model=list[ReservationKpiPeakPeriod])
async def peak_periods(db: AsyncSession = Depends(get_db)):
    """Get reservation peak periods by 2-hour windows."""
    q = text("""
        SELECT
            start_hour_2h_window,
            reservations_count,
            gold_kpi_build_ts::text
        FROM srv.reservations_kpi_peak_period_current
        ORDER BY reservations_count DESC;
    """)
    return (await db.execute(q)).mappings().all()

@router.get("/duration-distribution", response_model=list[ReservationKpiDurationDistribution])
async def duration_distribution(db: AsyncSession = Depends(get_db)):
    """Get distribution of reservations by duration bucket."""
    q = text("""
        SELECT
            duration_bucket,
            reservations_count,
            gold_kpi_build_ts::text
        FROM srv.reservations_kpi_duration_distribution_current
        ORDER BY duration_bucket;
    """)
    return (await db.execute(q)).mappings().all()

@router.get("/by-user/{user_id}", response_model=list[ReservationEnrichedCurrentRow])
async def user_reservations(
    user_id: int,
    limit: int = Query(500, ge=1, le=10000),
    db: AsyncSession = Depends(get_db),
):
    """Get all reservations for a specific user."""
    q = text("""
        SELECT
            reservation_id,
            user_id,
            user_login,
            user_full_name,
            reservation_item_id,
            reservation_item_name,
            group_id,
            start_ts::text,
            end_ts::text,
            duration_hours,
            is_invalid_duration,
            comment,
            start_date::text,
            start_month,
            start_year,
            start_hour,
            start_hour_2h_window,
            ingestion_date::text,
            _source_file,
            _source_system,
            _ingest_bronze_ts::text,
            _ingest_silver_ts::text,
            _ingest_gold_ts::text
        FROM srv.reservations_enriched_current
        WHERE user_id = :user_id
        ORDER BY start_ts DESC
        LIMIT :limit;
    """)
    return (await db.execute(q, {"user_id": user_id, "limit": limit})).mappings().all()

@router.get("/by-item/{item_id}", response_model=list[ReservationEnrichedCurrentRow])
async def item_reservations(
    item_id: int,
    limit: int = Query(500, ge=1, le=10000),
    db: AsyncSession = Depends(get_db),
):
    """Get all reservations for a specific item."""
    q = text("""
        SELECT
            reservation_id,
            user_id,
            user_login,
            user_full_name,
            reservation_item_id,
            reservation_item_name,
            group_id,
            start_ts::text,
            end_ts::text,
            duration_hours,
            is_invalid_duration,
            comment,
            start_date::text,
            start_month,
            start_year,
            start_hour,
            start_hour_2h_window,
            ingestion_date::text,
            _source_file,
            _source_system,
            _ingest_bronze_ts::text,
            _ingest_silver_ts::text,
            _ingest_gold_ts::text
        FROM srv.reservations_enriched_current
        WHERE reservation_item_id = :item_id
        ORDER BY start_ts DESC
        LIMIT :limit;
    """)
    return (await db.execute(q, {"item_id": item_id, "limit": limit})).mappings().all()
