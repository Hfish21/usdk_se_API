from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel, field_validator


# ---------------------------------------------------------------------------
# Lookup tables — shared by routers and validators
# ---------------------------------------------------------------------------

DAY_NAME_TO_INT: dict[str, int] = {
    "Monday": 0,
    "Tuesday": 1,
    "Wednesday": 2,
    "Thursday": 3,
    "Friday": 4,
    "Saturday": 5,
    "Sunday": 6,
}

PERIOD_NAME_TO_ID: dict[str, int] = {
    "Overnight": 1,
    "Early Morning": 2,
    "AM Peak": 3,
    "Midday": 4,
    "Early Afternoon": 5,
    "PM Peak": 6,
    "Evening": 7,
}


# ---------------------------------------------------------------------------
# Shared geometry schema
# ---------------------------------------------------------------------------

class GeometrySchema(BaseModel):
    type: str
    coordinates: Any  # list of [lon, lat] pairs for LINESTRING

    model_config = {"from_attributes": True}


# ---------------------------------------------------------------------------
# Response schemas
# ---------------------------------------------------------------------------

class LinkAggregateResponse(BaseModel):
    """Returned by GET /aggregates/ and GET /aggregates/{link_id}."""

    link_id: str
    road_name: str | None
    length: float | None
    average_speed: float
    geometry: GeometrySchema

    model_config = {"from_attributes": True}

    @field_validator("geometry", mode="before")
    @classmethod
    def parse_geometry(cls, v: Any) -> dict:
        """ST_AsGeoJSON returns a JSON string; parse it into a dict."""
        if isinstance(v, str):
            return json.loads(v)
        return v


class SlowLinkResponse(BaseModel):
    """Returned by GET /patterns/slow_links/."""

    link_id: str
    road_name: str | None
    length: float | None
    average_speed: float
    days_slow: int
    geometry: GeometrySchema

    model_config = {"from_attributes": True}

    @field_validator("geometry", mode="before")
    @classmethod
    def parse_geometry(cls, v: Any) -> dict:
        if isinstance(v, str):
            return json.loads(v)
        return v


# ---------------------------------------------------------------------------
# Request schemas
# ---------------------------------------------------------------------------

class BBoxFilterRequest(BaseModel):
    day: str
    period: str
    bbox: list[float]  # [minx, miny, maxx, maxy]

    @field_validator("day")
    @classmethod
    def validate_day(cls, v: str) -> str:
        if v not in DAY_NAME_TO_INT:
            raise ValueError(
                f"Invalid day '{v}'. Must be one of {list(DAY_NAME_TO_INT)}"
            )
        return v

    @field_validator("period")
    @classmethod
    def validate_period(cls, v: str) -> str:
        if v not in PERIOD_NAME_TO_ID:
            raise ValueError(
                f"Invalid period '{v}'. Must be one of {list(PERIOD_NAME_TO_ID)}"
            )
        return v

    @field_validator("bbox")
    @classmethod
    def validate_bbox(cls, v: list[float]) -> list[float]:
        if len(v) != 4:
            raise ValueError("bbox must have exactly 4 elements: [minx, miny, maxx, maxy]")
        minx, miny, maxx, maxy = v
        if minx >= maxx or miny >= maxy:
            raise ValueError("bbox must satisfy minx < maxx and miny < maxy")
        return v
