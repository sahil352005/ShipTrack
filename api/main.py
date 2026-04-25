"""
FastAPI — Enterprise Logistics Analytics & Intelligence Platform
Upgraded with Authentication, Role-Based Access, Geo-Mapping, and Management Reports.
"""

import logging
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Depends, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import databases

# Import Routers
from auth.router import router as auth_router
from users.router import router as users_router
from reports.router import router as reports_router
from maps.router import router as maps_router

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://logistics_user:logistics_pass@postgres:5432/logistics")
database = databases.Database(DATABASE_URL)

@asynccontextmanager
async def lifespan(app: FastAPI):
    await database.connect()
    logger.info("Database connected")
    yield
    await database.disconnect()
    logger.info("Database disconnected")

app = FastAPI(
    title="Logistics Intelligence API",
    description="Enterprise Analytics & Fleet Visibility Platform",
    version="2.1.0",
    lifespan=lifespan,
)

# Setup Templates and Static Files
templates = Jinja2Templates(directory="/app/templates")
# app.mount("/static", StaticFiles(directory="/app/templates"), name="static")

# Include Routers
app.include_router(auth_router)
app.include_router(users_router)
app.include_router(reports_router)
app.include_router(maps_router)

# ── Role-Based UI Routes ──────────────────────────────────────────────────────

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard_page(request: Request):
    return templates.TemplateResponse("dashboard.html", {"request": request})

# ── Health ────────────────────────────────────────────────────────────────────
@app.get("/health")
async def health():
    return {"status": "ok", "version": "2.1.0"}

# ── Shipments ─────────────────────────────────────────────────────────────────
@app.get("/shipments")
async def get_shipments(status: str = None, region: str = None, limit: int = 100):
    conditions = []
    values = {"limit": limit}
    if status:
        conditions.append("status = :status")
        values["status"] = status
    if region:
        conditions.append("region = :region")
        values["region"] = region
    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    query = f"SELECT * FROM shipments {where} ORDER BY event_timestamp DESC LIMIT :limit"
    rows = await database.fetch_all(query=query, values=values)
    return {"total": len(rows), "data": [dict(r) for r in rows]}

# ── Executive Summary ──────────────────────────────────────────────────────────
@app.get("/executive-summary")
async def get_executive_summary():
    metrics = await database.fetch_one(query="SELECT COUNT(*), SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END), ROUND(100.0 * SUM(CASE WHEN NOT is_delayed THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) FROM shipments")
    sla = await database.fetch_one(query="SELECT COUNT(*) FROM sla_violations WHERE created_at > NOW() - INTERVAL '24 hours'")
    
    return {
        "total_shipments": metrics[0] if metrics else 0,
        "delayed_shipments": metrics[1] if metrics else 0,
        "on_time_rate": metrics[2] if metrics else 0,
        "sla_violations_24h": sla[0] if sla else 0
    }

@app.get("/")
async def root():
    return {"message": "Enterprise Logistics Intelligence Platform", "version": "2.1.0", "ui": "/login", "docs": "/docs"}
