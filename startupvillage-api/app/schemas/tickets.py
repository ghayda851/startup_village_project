from pydantic import BaseModel
from typing import Optional

class TicketsCardsRow(BaseModel):
    period: str
    total_tickets: int
    is_open: int
    is_resolved: int
    technicians: int
    open_rate: float
    gold_kpi_build_ts: str

class TicketsByMonthRow(BaseModel):
    period: str
    ticket_count: int
    gold_kpi_build_ts: str

class TicketsByYearRow(BaseModel):
    year: int
    ticket_count: int
    gold_kpi_build_ts: str

class TicketsPriorityDistRow(BaseModel):
    period: str
    priority_code: int
    priority: str
    ticket_count: int
    gold_kpi_build_ts: str

class TicketsByCategoryRow(BaseModel):
    period: str
    category_id: int
    category: Optional[str]
    ticket_count: int
    total_tickets: int
    share: float
    gold_kpi_build_ts: str

class TicketsByLocationRow(BaseModel):
    period: str
    location_id: int
    location: Optional[str]
    ticket_count: int
    total_tickets: int
    share: float
    gold_kpi_build_ts: str

class TicketsByRequesterRow(BaseModel):
    period: str
    requester_user_id: int
    requester_login: Optional[str]
    requester_full_name: Optional[str]
    ticket_count: int
    gold_kpi_build_ts: str

class TicketsByTechnicianRow(BaseModel):
    period: str
    last_updater_user_id: int
    last_updater_full_name: Optional[str]
    ticket_count: int
    gold_kpi_build_ts: str

class TicketsHeatmapRow(BaseModel):
    period: str
    last_updater_user_id: int
    last_updater_full_name: Optional[str]
    priority_code: int
    priority: str
    ticket_count: int
    gold_kpi_build_ts: str