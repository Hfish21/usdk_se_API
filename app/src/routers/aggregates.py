from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import text

from src.database import get_db
from src.schemas import (
    BBoxFilterRequest,
    DAY_NAME_TO_INT,
    LinkAggregateResponse,
    PERIOD_NAME_TO_ID,
)

router = APIRouter()

# ---------------------------------------------------------------------------
# Shared SQL template
# ST_AsGeoJSON returns text; the Pydantic validator in LinkAggregateResponse
# parses it with json.loads(). The {extra_where} placeholder is filled with
# a hardcoded string literal — never with user input — so there is no
# SQL injection risk here.
# ---------------------------------------------------------------------------

_BASE_SQL = """
    SELECT
        l.link_id,
        l.road_name,
        l.length,
        AVG(sr.speed)                   AS average_speed,
        ST_AsGeoJSON(l.geometry)::text  AS geometry
    FROM links l
    JOIN speed_records sr ON sr.link_id = l.link_id
    WHERE sr.day_of_week = :day_of_week
      AND sr.period_id   = :period_id
      {extra_where}
    GROUP BY l.link_id, l.road_name, l.length, l.geometry
    ORDER BY l.link_id
"""


def _resolve_day_period(day: str, period: str) -> tuple[int, int]:
    """Translate string day/period names to their integer DB values."""
    if day not in DAY_NAME_TO_INT:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid day '{day}'. Valid values: {list(DAY_NAME_TO_INT)}",
        )
    if period not in PERIOD_NAME_TO_ID:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid period '{period}'. Valid values: {list(PERIOD_NAME_TO_ID)}",
        )
    return DAY_NAME_TO_INT[day], PERIOD_NAME_TO_ID[period]


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/", response_model=list[LinkAggregateResponse])
def get_aggregates(
    day: str = Query(..., description="Day of week, e.g. 'Monday'"),
    period: str = Query(..., description="Time period name, e.g. 'AM Peak'"),
    db: Session = Depends(get_db),
) -> list[dict]:
    """
    Returns average speed per road segment for the given day and time period.
    """
    day_int, period_int = _resolve_day_period(day, period)

    sql = text(_BASE_SQL.format(extra_where=""))
    rows = db.execute(sql, {"day_of_week": day_int, "period_id": period_int}).mappings().all()
    return [dict(r) for r in rows]


@router.post("/spatial_filter/", response_model=list[LinkAggregateResponse])
def spatial_filter(
    request: BBoxFilterRequest,
    db: Session = Depends(get_db),
) -> list[dict]:
    """
    Returns road segments intersecting a bounding box with average speed for
    the given day and time period.

    Body: {"day": "Monday", "period": "AM Peak", "bbox": [minx, miny, maxx, maxy]}
    """
    day_int = DAY_NAME_TO_INT[request.day]
    period_int = PERIOD_NAME_TO_ID[request.period]
    minx, miny, maxx, maxy = request.bbox

    sql = text("""
        SELECT
            l.link_id,
            l.road_name,
            l.length,
            AVG(sr.speed)                   AS average_speed,
            ST_AsGeoJSON(l.geometry)::text  AS geometry
        FROM links l
        JOIN speed_records sr ON sr.link_id = l.link_id
        WHERE sr.day_of_week = :day_of_week
          AND sr.period_id   = :period_id
          AND ST_Intersects(
                l.geometry,
                ST_MakeEnvelope(:minx, :miny, :maxx, :maxy, 4326)
              )
        GROUP BY l.link_id, l.road_name, l.length, l.geometry
        ORDER BY l.link_id
    """)

    rows = db.execute(sql, {
        "day_of_week": day_int,
        "period_id": period_int,
        "minx": minx,
        "miny": miny,
        "maxx": maxx,
        "maxy": maxy,
    }).mappings().all()

    return [dict(r) for r in rows]


@router.get("/{link_id}", response_model=LinkAggregateResponse)
def get_aggregate_for_link(
    link_id: str,
    day: str = Query(..., description="Day of week, e.g. 'Monday'"),
    period: str = Query(..., description="Time period name, e.g. 'AM Peak'"),
    db: Session = Depends(get_db),
) -> dict:
    """
    Returns speed and metadata for a single road segment.
    """
    day_int, period_int = _resolve_day_period(day, period)

    sql = text(_BASE_SQL.format(extra_where="AND l.link_id = :link_id"))
    row = db.execute(
        sql,
        {"day_of_week": day_int, "period_id": period_int, "link_id": link_id},
    ).mappings().first()

    if not row:
        raise HTTPException(
            status_code=404,
            detail=f"No data found for link_id='{link_id}', day='{day}', period='{period}'",
        )

    return dict(row)
