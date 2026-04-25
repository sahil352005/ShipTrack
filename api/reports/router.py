from fastapi import APIRouter, Depends, Response, HTTPException
from auth.service import get_current_user, check_role
from reports.service import ReportGenerator
from datetime import datetime

router = APIRouter(prefix="/reports", tags=["Reports"])

@router.get("/daily.csv")
async def get_daily_csv(user: dict = Depends(check_role(["Admin", "Manager"]))):
    from main import database
    query = "SELECT * FROM shipments WHERE event_timestamp > NOW() - INTERVAL '24 hours'"
    rows = await database.fetch_all(query)
    csv_data = ReportGenerator.to_csv(rows)
    return Response(content=csv_data, media_type="text/csv", headers={"Content-Disposition": "attachment; filename=daily_report.csv"})

@router.get("/daily.pdf")
async def get_daily_pdf(user: dict = Depends(check_role(["Admin", "Manager"]))):
    from main import database
    query = "SELECT shipment_id, status, region, distance_km, event_timestamp FROM shipments WHERE event_timestamp > NOW() - INTERVAL '24 hours' LIMIT 50"
    rows = await database.fetch_all(query)
    pdf_data = ReportGenerator.to_pdf("Daily Shipment Report", rows)
    return Response(content=pdf_data, media_type="application/pdf", headers={"Content-Disposition": "attachment; filename=daily_report.pdf"})

@router.get("/delays.csv")
async def get_delays_csv(user: dict = Depends(check_role(["Admin", "Manager"]))):
    from main import database
    query = "SELECT * FROM shipments WHERE is_delayed = TRUE AND event_timestamp > NOW() - INTERVAL '7 days'"
    rows = await database.fetch_all(query)
    csv_data = ReportGenerator.to_csv(rows)
    return Response(content=csv_data, media_type="text/csv", headers={"Content-Disposition": "attachment; filename=delay_report.csv"})

@router.get("/executive.pdf")
async def get_executive_pdf(user: dict = Depends(check_role(["Admin", "Manager"]))):
    from main import database
    # Get summary metrics
    summary_query = "SELECT * FROM executive_summary ORDER BY calculated_at DESC LIMIT 1"
    summary = await database.fetch_one(summary_query)
    
    # Get recent alerts for context
    alerts_query = "SELECT type, severity, message, created_at FROM alerts WHERE resolved = FALSE LIMIT 5"
    alerts = await database.fetch_all(alerts_query)
    
    pdf_data = ReportGenerator.to_pdf("Executive KPI Summary", alerts, dict(summary) if summary else None)
    return Response(content=pdf_data, media_type="application/pdf", headers={"Content-Disposition": "attachment; filename=executive_summary.pdf"})

@router.get("/warehouse.csv")
async def get_warehouse_csv(user: dict = Depends(check_role(["Admin", "Manager"]))):
    from main import database
    query = "SELECT * FROM warehouse_health"
    rows = await database.fetch_all(query)
    csv_data = ReportGenerator.to_csv(rows)
    return Response(content=csv_data, media_type="text/csv", headers={"Content-Disposition": "attachment; filename=warehouse_performance.csv"})

@router.get("/regional.pdf")
async def get_regional_pdf(user: dict = Depends(check_role(["Admin", "Manager"]))):
    from main import database
    query = "SELECT region, total_shipments, delivered, delayed, on_time_rate, trend FROM region_performance"
    rows = await database.fetch_all(query)
    pdf_data = ReportGenerator.to_pdf("Regional Performance Report", rows)
    return Response(content=pdf_data, media_type="application/pdf", headers={"Content-Disposition": "attachment; filename=regional_performance.pdf"})
