from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from app.core.db import get_db
from app.schemas.tickets import (
    TicketsCardsRow, TicketsByMonthRow, TicketsByYearRow,
    TicketsPriorityDistRow, TicketsByCategoryRow, TicketsByLocationRow,
    TicketsByRequesterRow, TicketsByTechnicianRow, TicketsHeatmapRow
)

router = APIRouter(prefix="/tickets", tags=["tickets"])

@router.get("/cards", response_model=TicketsCardsRow)
async def cards(period: str = Query("ALL"), db: AsyncSession = Depends(get_db)):
    q = text("""
        SELECT
          period, total_tickets, is_open, is_resolved, technicians, open_rate,
          gold_kpi_build_ts::text AS gold_kpi_build_ts
        FROM srv.tickets_cards
        WHERE period = :period
        LIMIT 1;
    """)
    return (await db.execute(q, {"period": period})).mappings().first()

@router.get("/by-month", response_model=list[TicketsByMonthRow])
async def by_month(db: AsyncSession = Depends(get_db)):
    q = text("""
        SELECT period, ticket_count, gold_kpi_build_ts::text AS gold_kpi_build_ts
        FROM srv.kpi_tickets_by_month
        ORDER BY period;
    """)
    return (await db.execute(q)).mappings().all()

@router.get("/by-year", response_model=list[TicketsByYearRow])
async def by_year(db: AsyncSession = Depends(get_db)):
    q = text("""
        SELECT year, ticket_count, gold_kpi_build_ts::text AS gold_kpi_build_ts
        FROM srv.kpi_tickets_by_year
        ORDER BY year;
    """)
    return (await db.execute(q)).mappings().all()

@router.get("/priority", response_model=list[TicketsPriorityDistRow])
async def priority(period: str = Query("ALL"), db: AsyncSession = Depends(get_db)):
    q = text("""
        SELECT
          period, priority_code, priority, ticket_count,
          gold_kpi_build_ts::text AS gold_kpi_build_ts
        FROM srv.kpi_priority_distribution_period
        WHERE period = :period
        ORDER BY priority_code DESC;
    """)
    return (await db.execute(q, {"period": period})).mappings().all()

@router.get("/category", response_model=list[TicketsByCategoryRow])
async def category(period: str = Query("ALL"), db: AsyncSession = Depends(get_db)):
    q = text("""
        SELECT
          period, category_id, category, ticket_count, total_tickets, share,
          gold_kpi_build_ts::text AS gold_kpi_build_ts
        FROM srv.kpi_tickets_by_category_period
        WHERE period = :period
        ORDER BY ticket_count DESC;
    """)
    return (await db.execute(q, {"period": period})).mappings().all()

@router.get("/location", response_model=list[TicketsByLocationRow])
async def location(period: str = Query("ALL"), db: AsyncSession = Depends(get_db)):
    q = text("""
        SELECT
          period, location_id, location, ticket_count, total_tickets, share,
          gold_kpi_build_ts::text AS gold_kpi_build_ts
        FROM srv.kpi_tickets_by_location_period
        WHERE period = :period
        ORDER BY ticket_count DESC;
    """)
    return (await db.execute(q, {"period": period})).mappings().all()

@router.get("/by-requester", response_model=list[TicketsByRequesterRow])
async def by_requester(period: str = Query("ALL"), db: AsyncSession = Depends(get_db)):
    q = text("""
        SELECT
          period, requester_user_id, requester_login, requester_full_name, ticket_count,
          gold_kpi_build_ts::text AS gold_kpi_build_ts
        FROM srv.kpi_tickets_by_requester_period
        WHERE period = :period
        ORDER BY ticket_count DESC;
    """)
    return (await db.execute(q, {"period": period})).mappings().all()

@router.get("/by-technician", response_model=list[TicketsByTechnicianRow])
async def by_technician(period: str = Query("ALL"), db: AsyncSession = Depends(get_db)):
    q = text("""
        SELECT
          period, last_updater_user_id, last_updater_full_name, ticket_count,
          gold_kpi_build_ts::text AS gold_kpi_build_ts
        FROM srv.kpi_tickets_by_technician_period
        WHERE period = :period
        ORDER BY ticket_count DESC;
    """)
    return (await db.execute(q, {"period": period})).mappings().all()

@router.get("/heatmap", response_model=list[TicketsHeatmapRow])
async def heatmap(period: str = Query("ALL"), db: AsyncSession = Depends(get_db)):
    q = text("""
        SELECT
          period, last_updater_user_id, last_updater_full_name,
          priority_code, priority, ticket_count,
          gold_kpi_build_ts::text AS gold_kpi_build_ts
        FROM srv.kpi_load_by_technician_priority_period
        WHERE period = :period
        ORDER BY last_updater_full_name, priority_code DESC;
    """)
    return (await db.execute(q, {"period": period})).mappings().all()