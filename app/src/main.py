from contextlib import asynccontextmanager
from fastapi import FastAPI
from src.database import init_db
from src.routers import aggregates, patterns


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(
    title="Urban SDK Geospatial Traffic API",
    version="0.1.0",
    description="Geospatial microservice for traffic speed analysis over Duval County, FL.",
    lifespan=lifespan,
)

app.include_router(aggregates.router, prefix="/aggregates", tags=["aggregates"])
app.include_router(patterns.router, prefix="/patterns", tags=["patterns"])


@app.get("/health", tags=["health"])
def health():
    return {"status": "ok"}
