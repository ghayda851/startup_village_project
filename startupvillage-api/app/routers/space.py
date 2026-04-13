from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from app.core.db import get_db
from app.schemas.space import (
    SpaceGlobalKpi, SpaceBySiteKpi, SpaceBySiteSpaceTypeKpi, SpaceBySiteOrgTypeKpi, SpaceRoomRow
)

router = APIRouter(prefix="/space", tags=["space"])

@router.get("/global", response_model=SpaceGlobalKpi)
async def global_kpis(db: AsyncSession = Depends(get_db)):
    q = text("""
        SELECT
          villages_count, total_spaces, total_area_m2, total_capacity, total_employees,
          leased_spaces, non_leased_spaces, reservation_spaces, per_person_spaces,
          underutilized_spaces_lt_40pct, overoccupied_spaces_gt_100pct,
          avg_density_emp_per_100m2, weighted_occupancy_rate_pct,
          _as_of_ts::text AS as_of_ts
        FROM srv.space_kpi_global_current
        LIMIT 1;
    """)
    row = (await db.execute(q)).mappings().first()
    return row

@router.get("/by-site", response_model=list[SpaceBySiteKpi])
async def by_site(db: AsyncSession = Depends(get_db)):
    q = text("""
        SELECT
          site, total_spaces, total_area_m2, total_capacity, total_employees,
          leased_spaces, non_leased_spaces,
          underutilized_spaces_lt_40pct, overoccupied_spaces_gt_100pct,
          avg_density_emp_per_100m2, weighted_occupancy_rate_pct,
          _as_of_ts::text AS as_of_ts
        FROM srv.space_kpi_by_site_current
        ORDER BY weighted_occupancy_rate_pct DESC;
    """)
    return (await db.execute(q)).mappings().all()

@router.get("/by-site-space-type", response_model=list[SpaceBySiteSpaceTypeKpi])
async def by_site_space_type(site: str = Query(...), db: AsyncSession = Depends(get_db)):
    q = text("""
        SELECT
        site,
        space_type,
        COALESCE(spaces_count, 0)::int AS spaces_count,
        COALESCE(sum_area_m2, 0)::double precision AS sum_area_m2,
        COALESCE(sum_capacity, 0)::int AS sum_capacity,
        COALESCE(sum_employees, 0)::int AS sum_employees,
        _as_of_ts::text AS as_of_ts
        FROM srv.space_kpi_by_site_space_type_current
        WHERE site = :site
        ORDER BY spaces_count DESC;
    """)
    return (await db.execute(q, {"site": site})).mappings().all()

@router.get("/by-site-org-type", response_model=list[SpaceBySiteOrgTypeKpi])
async def by_site_org_type(site: str = Query(...), db: AsyncSession = Depends(get_db)):
    q = text("""
        SELECT
        site,
        organization_type,
        COALESCE(spaces_count, 0)::int AS spaces_count,
        COALESCE(sum_employees, 0)::int AS sum_employees,
        COALESCE(sum_area_m2, 0)::double precision AS sum_area_m2,
        COALESCE(sum_capacity, 0)::int AS sum_capacity,
        _as_of_ts::text AS as_of_ts
        FROM srv.space_kpi_by_site_org_type_current
        WHERE site = :site
        ORDER BY spaces_count DESC;
    """)
    return (await db.execute(q, {"site": site})).mappings().all()

@router.get("/rooms", response_model=list[SpaceRoomRow])
async def rooms(
    site: str | None = None,
    floor: str | None = None,
    space_type: str | None = None,
    organization_type: str | None = None,
    occupancy_status: str | None = None,
    limit: int = 500,
    db: AsyncSession = Depends(get_db),
):
    where = []
    params = {"limit": limit}

    if site:
        where.append("site = :site")
        params["site"] = site
    if floor:
        where.append("floor = :floor")
        params["floor"] = floor
    if space_type:
        where.append("space_type = :space_type")
        params["space_type"] = space_type
    if organization_type:
        where.append("organization_type = :organization_type")
        params["organization_type"] = organization_type
    if occupancy_status:
        where.append("occupancy_status = :occupancy_status")
        params["occupancy_status"] = occupancy_status

    where_sql = "WHERE " + " AND ".join(where) if where else ""

    q = text(f"""
        SELECT
        room_nk, site, floor, room, occupancy_status,
        organization_name, organization_type, space_type, activity,
        COALESCE(employee_count, 0)::int AS employee_count,
        COALESCE(total_capacity, 0)::int AS total_capacity,
        COALESCE(area_m2, 0)::double precision AS area_m2,
        COALESCE(occupancy_rate_pct, 0)::double precision AS occupancy_rate_pct,
        COALESCE(density_emp_per_100m2, 0)::double precision AS density_emp_per_100m2,
        COALESCE(is_over_capacity, false)::boolean AS is_over_capacity,
        _ingestion_date::text AS ingestion_date,
        last_updated_ts::text AS last_updated_ts
        FROM srv.space_room_current
        {where_sql}
        ORDER BY site, floor, room
        LIMIT :limit;
    """)
    return (await db.execute(q, params)).mappings().all()