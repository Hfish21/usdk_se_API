"""
Ingestion script for Urban SDK geospatial traffic data.

Downloads link geometry and speed records from the Urban SDK CDN,
loads them into PostgreSQL + PostGIS, and creates spatial/temporal indexes.

Usage:
    # Inside Docker (recommended — uses db hostname)
    docker-compose exec api python -m src.scripts.ingest

    # Locally (requires DATABASE_URL pointing to localhost:5432)
    cd app && python -m src.scripts.ingest
"""

import logging
import sys

import geopandas as gpd
import pandas as pd
from sqlalchemy import text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

LINK_INFO_URL = "https://cdn.urbansdk.com/data-engineering-interview/link_info.parquet.gz"
SPEED_DATA_URL = "https://cdn.urbansdk.com/data-engineering-interview/duval_jan1_2024.parquet.gz"

# pd.cut bins: total minutes since midnight, right=False means left-inclusive [start, end)
# [0,240) = 00:00-03:59 Overnight (1)
# [240,420) = 04:00-06:59 Early Morning (2)
# [420,600) = 07:00-09:59 AM Peak (3)
# [600,780) = 10:00-12:59 Midday (4)
# [780,960) = 13:00-15:59 Early Afternoon (5)
# [960,1140) = 16:00-18:59 PM Peak (6)
# [1140,1440] = 19:00-23:59 Evening (7)
PERIOD_BINS = [0, 240, 420, 600, 780, 960, 1140, 1440]
PERIOD_LABELS = [1, 2, 3, 4, 5, 6, 7]

CHUNK_SIZE = 10_000


def _find_col(columns: list[str], *candidates: str) -> str | None:
    """Return the first column name matching any candidate (case-insensitive)."""
    lower_cols = {c.lower(): c for c in columns}
    for candidate in candidates:
        if candidate.lower() in lower_cols:
            return lower_cols[candidate.lower()]
    return None


def ingest_links(conn, gdf: gpd.GeoDataFrame) -> int:
    log.info("Preparing link records...")

    if gdf.crs is None:
        log.warning("link_info has no CRS — assuming EPSG:4326")
        gdf = gdf.set_crs("EPSG:4326")
    elif gdf.crs.to_epsg() != 4326:
        log.info(f"Reprojecting from {gdf.crs} to EPSG:4326")
        gdf = gdf.to_crs("EPSG:4326")

    cols = list(gdf.columns)
    link_col = _find_col(cols, "link_id", "linkid", "id")
    name_col = _find_col(cols, "road_name", "street_name", "name", "roadname", "streetname")
    len_col = _find_col(cols, "length", "seg_length", "shape_length", "shapeLength")

    if not link_col:
        log.error(f"Cannot find link_id column. Available: {cols}")
        sys.exit(1)

    log.info(f"Column mapping → link_id='{link_col}', road_name='{name_col}', length='{len_col}'")

    records = []
    for _, row in gdf.iterrows():
        geom_wkt = row.geometry.wkt if row.geometry and not row.geometry.is_empty else None
        if not geom_wkt:
            continue
        records.append({
            "link_id": str(row[link_col]),
            "road_name": str(row[name_col]) if name_col and pd.notna(row.get(name_col)) else None,
            "length": float(row[len_col]) if len_col and pd.notna(row.get(len_col)) else None,
            "geometry": f"SRID=4326;{geom_wkt}",
        })

    conn.execute(
        text("""
            INSERT INTO links (link_id, road_name, length, geometry)
            VALUES (:link_id, :road_name, :length, ST_GeomFromEWKT(:geometry))
            ON CONFLICT (link_id) DO NOTHING
        """),
        records,
    )
    log.info(f"Inserted {len(records)} link records")
    return len(records)


def ingest_speed_records(conn, df: pd.DataFrame) -> int:
    log.info(f"Processing {len(df)} speed records...")

    cols = list(df.columns)
    ts_col = _find_col(cols, "timestamp", "datetime", "time", "date_time")
    speed_col = _find_col(cols, "speed", "avg_speed", "average_speed")
    link_col = _find_col(cols, "link_id", "linkid", "id")

    if not all([ts_col, speed_col, link_col]):
        log.error(f"Cannot identify required columns. Found: {cols}")
        sys.exit(1)

    log.info(f"Column mapping → timestamp='{ts_col}', speed='{speed_col}', link_id='{link_col}'")

    # Parse timestamps as UTC
    df[ts_col] = pd.to_datetime(df[ts_col], utc=True)

    # Vectorized period classification
    total_minutes = df[ts_col].dt.hour * 60 + df[ts_col].dt.minute
    period_ids = pd.cut(
        total_minutes,
        bins=PERIOD_BINS,
        labels=PERIOD_LABELS,
        right=False,
        include_lowest=True,
    ).astype(int)

    day_of_week = df[ts_col].dt.weekday  # 0=Monday

    # Build a clean DataFrame for bulk insert
    insert_df = pd.DataFrame({
        "link_id": df[link_col].astype(str),
        "timestamp": df[ts_col],
        "speed": df[speed_col].astype(float),
        "day_of_week": day_of_week.astype(int),
        "period_id": period_ids,
    })

    records = insert_df.to_dict(orient="records")

    total = 0
    for i in range(0, len(records), CHUNK_SIZE):
        chunk = records[i : i + CHUNK_SIZE]
        conn.execute(
            text("""
                INSERT INTO speed_records (link_id, timestamp, speed, day_of_week, period_id)
                VALUES (:link_id, :timestamp, :speed, :day_of_week, :period_id)
            """),
            chunk,
        )
        total += len(chunk)
        log.info(f"  Inserted {total}/{len(records)} speed records...")

    log.info(f"Inserted {total} speed records total")
    return total


def create_indexes(conn) -> None:
    log.info("Creating spatial and composite indexes...")
    conn.execute(text(
        "CREATE INDEX IF NOT EXISTS ix_links_geometry ON links USING GIST (geometry);"
    ))
    conn.execute(text(
        "CREATE INDEX IF NOT EXISTS ix_sr_link_day_period "
        "ON speed_records (link_id, day_of_week, period_id);"
    ))
    conn.execute(text(
        "CREATE INDEX IF NOT EXISTS ix_sr_day_period "
        "ON speed_records (day_of_week, period_id);"
    ))
    log.info("Indexes created")


def main() -> None:
    from src.database import init_db, engine

    log.info("Initializing database schema...")
    init_db()

    log.info(f"Downloading link info from:\n  {LINK_INFO_URL}")
    gdf = gpd.read_parquet(LINK_INFO_URL)
    log.info(f"  Loaded {len(gdf)} links | columns: {list(gdf.columns)}")

    log.info(f"Downloading speed data from:\n  {SPEED_DATA_URL}")
    df = pd.read_parquet(SPEED_DATA_URL)
    log.info(f"  Loaded {len(df)} speed records | columns: {list(df.columns)}")

    with engine.begin() as conn:
        ingest_links(conn, gdf)
        ingest_speed_records(conn, df)
        create_indexes(conn)

    log.info("Ingestion complete.")


if __name__ == "__main__":
    main()
