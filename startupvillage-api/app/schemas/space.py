from pydantic import BaseModel
from typing import Optional

class SpaceGlobalKpi(BaseModel):
    villages_count: int
    total_spaces: int
    total_area_m2: float
    total_capacity: int
    total_employees: int
    leased_spaces: int
    non_leased_spaces: int
    reservation_spaces: Optional[int] = None
    per_person_spaces: Optional[int] = None
    underutilized_spaces_lt_40pct: int
    overoccupied_spaces_gt_100pct: int
    avg_density_emp_per_100m2: float
    weighted_occupancy_rate_pct: float
    as_of_ts: str

class SpaceBySiteKpi(BaseModel):
    site: str
    total_spaces: int
    total_area_m2: float
    total_capacity: int
    total_employees: int
    leased_spaces: Optional[int] = None
    non_leased_spaces: Optional[int] = None
    underutilized_spaces_lt_40pct: int
    overoccupied_spaces_gt_100pct: int
    avg_density_emp_per_100m2: float
    weighted_occupancy_rate_pct: float
    as_of_ts: str

class SpaceBySiteSpaceTypeKpi(BaseModel):
    site: str
    space_type: str
    spaces_count: int
    sum_area_m2: float
    sum_capacity: int
    sum_employees: int
    as_of_ts: str

class SpaceBySiteOrgTypeKpi(BaseModel):
    site: str
    organization_type: str
    spaces_count: int
    sum_employees: int
    sum_area_m2: float
    sum_capacity: int
    as_of_ts: str

class SpaceRoomRow(BaseModel):
    room_nk: str
    site: str
    floor: str
    room: str
    occupancy_status: str
    organization_name: str
    organization_type: str
    space_type: str
    activity: str
    employee_count: int
    total_capacity: int
    area_m2: float
    occupancy_rate_pct: float
    density_emp_per_100m2: float
    is_over_capacity: bool
    ingestion_date: str
    last_updated_ts: str