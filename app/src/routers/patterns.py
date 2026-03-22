from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import text

from src.database import get_db
from src.schemas import PERIOD_NAME_TO_ID, SlowLinkResponse

router = APIRouter()


@router.get("/slow_links/", response_model=list[SlowLinkResponse])
def get_slow_links(
    period: str = Query(..., description="Time period name, e.g. 'AM Peak'"),
    threshold: float = Query(25.0, description="Speed threshold in mph"),
    min_days: int = Query(3, ge=1, le=7, description="Minimum days below threshold"),
    db: Session = Depends(get_db),
) -> list[dict]:
    """
    Returns road segments that are consistently slow (average speed below threshold)
    across at least min_days distinct days of the week within the given time period.

    Note: The source dataset covers Jan 1, 2024 (Monday only). Use min_days=1 when
    querying this single-day dataset to get meaningful results.
    """
    if period not in PERIOD_NAME_TO_ID:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid period '{period}'. Valid values: {list(PERIOD_NAME_TO_ID)}",
        )
    period_int = PERIOD_NAME_TO_ID[period]

    # CTE breakdown:
    # 1. daily_averages — avg speed per (link, day) pair within the period
    # 2. slow_days — links where that daily avg is below threshold; count qualifying days
    # 3. Final SELECT — enrich with geometry, overall avg, ordered worst-first
    sql = text("""
        WITH daily_averages AS (
            SELECT
                sr.link_id,
                sr.day_of_week,
                AVG(sr.speed) AS daily_avg_speed
            FROM speed_records sr
            WHERE sr.period_id = :period_id
            GROUP BY sr.link_id, sr.day_of_week
        ),
        slow_days AS (
            SELECT
                link_id,
                COUNT(DISTINCT day_of_week) AS days_slow
            FROM daily_averages
            WHERE daily_avg_speed < :threshold
            GROUP BY link_id
            HAVING COUNT(DISTINCT day_of_week) >= :min_days
        )
        SELECT
            l.link_id,
            l.road_name,
            l.length,
            avg_speeds.overall_avg              AS average_speed,
            sd.days_slow,
            ST_AsGeoJSON(l.geometry)::text      AS geometry
        FROM slow_days sd
        JOIN links l ON l.link_id = sd.link_id
        JOIN (
            SELECT link_id, AVG(speed) AS overall_avg
            FROM speed_records
            WHERE period_id = :period_id
            GROUP BY link_id
        ) avg_speeds ON avg_speeds.link_id = sd.link_id
        ORDER BY sd.days_slow DESC, avg_speeds.overall_avg ASC
    """)

    rows = db.execute(sql, {
        "period_id": period_int,
        "threshold": threshold,
        "min_days": min_days,
    }).mappings().all()

    return [dict(r) for r in rows]
