from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, SmallInteger, Index
from sqlalchemy.orm import relationship
from geoalchemy2 import Geometry
from src.database import Base


class Link(Base):
    __tablename__ = "links"

    link_id = Column(String, primary_key=True, index=True)
    road_name = Column(String, nullable=True)
    length = Column(Float, nullable=True)
    # Geometry stored as WKB, SRID 4326 (WGS84)
    # Using generic GEOMETRY type — actual data contains MULTILINESTRING
    geometry = Column(
        Geometry(geometry_type="GEOMETRY", srid=4326),
        nullable=False,
    )

    speed_records = relationship("SpeedRecord", back_populates="link")


class SpeedRecord(Base):
    __tablename__ = "speed_records"

    id = Column(Integer, primary_key=True, autoincrement=True)
    link_id = Column(String, ForeignKey("links.link_id"), nullable=False)
    timestamp = Column(DateTime(timezone=True), nullable=False)
    speed = Column(Float, nullable=False)
    # Pre-computed at ingest time using Python datetime.weekday(): 0=Monday, 6=Sunday
    day_of_week = Column(SmallInteger, nullable=False)
    # Pre-computed at ingest time using pd.cut: 1=Overnight ... 7=Evening
    period_id = Column(SmallInteger, nullable=False)

    link = relationship("Link", back_populates="speed_records")

    __table_args__ = (
        # Primary query pattern: filter by link + day + period
        Index("ix_speed_records_link_day_period", "link_id", "day_of_week", "period_id"),
        # Cross-link aggregate queries: filter by day + period only
        Index("ix_speed_records_day_period", "day_of_week", "period_id"),
    )
