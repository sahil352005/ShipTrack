from fastapi import APIRouter, Depends, Query
from typing import List, Optional
from auth.service import get_current_user

router = APIRouter(prefix="/map", tags=["Maps"])

@router.get("/shipments")
async def get_map_shipments(
    region:    Optional[str] = Query(None),
    status:    Optional[str] = Query(None),
    warehouse: Optional[str] = Query(None),
    user: dict = Depends(get_current_user)
):
    """
    Return coordinates and status for map visualization.
    This would typically query the 'shipments' table.
    """
    from main import database
    
    conditions = []
    values = {}
    
    if region:
        conditions.append("region = :region")
        values["region"] = region
    if status:
        conditions.append("status = :status")
        values["status"] = status
    if warehouse:
        conditions.append("warehouse_id = :warehouse")
        values["warehouse"] = warehouse
        
    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    
    # We select the latest state for each shipment to avoid duplicates on the map
    query = f"""
        SELECT DISTINCT ON (shipment_id)
            shipment_id, latitude, longitude, status, region, warehouse_id, 
            distance_km, is_delayed
        FROM shipments
        {where_clause}
        ORDER BY shipment_id, event_timestamp DESC
    """
    
    rows = await database.fetch_all(query=query, values=values)
    
    # Enrich with risk level (calculated in real-time or from risk table)
    results = []
    for row in rows:
        r = dict(row)
        # Try to join with risk assessment
        risk_query = "SELECT risk_level FROM shipment_risk WHERE shipment_id = :sid"
        risk = await database.fetch_one(risk_query, {"sid": r["shipment_id"]})
        r["risk_level"] = risk["risk_level"] if risk else "Low"
        results.append(r)
        
    return results
