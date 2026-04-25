"""
Auto Alert Engine
Generates alerts based on rule violations and thresholds
"""

from dataclasses import dataclass
from typing import List, Optional
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


@dataclass
class Alert:
    """Alert data structure"""
    id: Optional[int]
    type: str  # delayed_shipment / warehouse_bottleneck / critical_risk / region_performance
    severity: str  # Low / Medium / High / Critical
    message: str
    created_at: datetime
    resolved: bool = False
    shipment_id: Optional[str] = None
    warehouse_id: Optional[str] = None
    region: Optional[str] = None


class AlertEngine:
    """Rule-based alert generation engine"""
    
    # Thresholds
    DELAYED_SHIPMENT_THRESHOLD = 10
    CRITICAL_RISK_THRESHOLD = 80
    REGION_PERFORMANCE_DROP = 15  # percent
    
    def __init__(self):
        self.active_alerts = []
    
    def check_delayed_shipments(self, delayed_count: int, total: int) -> Optional[Alert]:
        """Generate alert if delayed shipments exceed threshold"""
        
        if delayed_count > self.DELAYED_SHIPMENT_THRESHOLD:
            percentage = (delayed_count / total * 100) if total > 0 else 0
            
            severity = "Critical" if percentage > 20 else "High" if percentage > 10 else "Medium"
            
            return Alert(
                id=None,
                type="delayed_shipment",
                severity=severity,
                message=f"Delayed shipments ({delayed_count}) exceed threshold ({self.DELAYED_SHIPMENT_THRESHOLD})",
                created_at=datetime.utcnow(),
                resolved=False
            )
        return None
    
    def check_warehouse_bottleneck(self, warehouse_id: str, severity: str) -> Optional[Alert]:
        """Generate alert for warehouse bottleneck"""
        
        if severity in ["Critical", "High"]:
            return Alert(
                id=None,
                type="warehouse_bottleneck",
                severity=severity,
                message=f"Warehouse {warehouse_id} has bottleneck: {severity} severity",
                created_at=datetime.utcnow(),
                resolved=False,
                warehouse_id=warehouse_id
            )
        return None
    
    def check_critical_risk(self, shipment_id: str, risk_score: int) -> Optional[Alert]:
        """Generate alert for critical risk shipment"""
        
        if risk_score >= self.CRITICAL_RISK_THRESHOLD:
            return Alert(
                id=None,
                type="critical_risk",
                severity="Critical",
                message=f"Shipment {shipment_id} has critical risk score: {risk_score}",
                created_at=datetime.utcnow(),
                resolved=False,
                shipment_id=shipment_id
            )
        return None
    
    def check_region_performance(self, region: str, on_time_rate: float, 
                                 previous_rate: float) -> Optional[Alert]:
        """Generate alert if region performance drops"""
        
        drop = previous_rate - on_time_rate
        
        if drop > self.REGION_PERFORMANCE_DROP:
            return Alert(
                id=None,
                type="region_performance",
                severity="High",
                message=f"Region {region} on-time rate dropped from {previous_rate}% to {on_time_rate}%",
                created_at=datetime.utcnow(),
                resolved=False,
                region=region
            )
        return None
    
    def generate_alerts(self, metrics: dict, warehouse_bottlenecks: list,
                       risk_assessments: list) -> List[Alert]:
        """Generate all applicable alerts"""
        
        alerts = []
        
        # Check delayed shipments
        delayed_alert = self.check_delayed_shipments(
            metrics.get("delayed_count", 0),
            metrics.get("total_shipments", 0)
        )
        if delayed_alert:
            alerts.append(delayed_alert)
        
        # Check warehouse bottlenecks
        for wb in warehouse_bottlenecks:
            if wb.bottleneck:
                alert = self.check_warehouse_bottleneck(wb.warehouse_id, wb.severity)
                if alert:
                    alerts.append(alert)
        
        # Check critical risk shipments
        for ra in risk_assessments:
            if ra.risk_score >= self.CRITICAL_RISK_THRESHOLD:
                alert = self.check_critical_risk(ra.shipment_id, ra.risk_score)
                if alert:
                    alerts.append(alert)
        
        # Check region performance
        region_stats = metrics.get("region_stats", {})
        for region, stats in region_stats.items():
            if "previous_on_time_rate" in stats:
                alert = self.check_region_performance(
                    region,
                    stats.get("on_time_rate", 0),
                    stats.get("previous_on_time_rate", 0)
                )
                if alert:
                    alerts.append(alert)
        
        self.active_alerts = alerts
        return alerts


def generate_alerts(metrics: dict, warehouse_bottlenecks: list,
                   risk_assessments: list) -> List[Alert]:
    """Convenience function to generate alerts"""
    engine = AlertEngine()
    return engine.generate_alerts(metrics, warehouse_bottlenecks, risk_assessments)


if __name__ == "__main__":
    # Test the alert engine
    test_metrics = {
        "total_shipments": 100,
        "delayed_count": 15,
        "region_stats": {
            "north": {"on_time_rate": 75, "previous_on_time_rate": 90}
        }
    }
    
    engine = AlertEngine()
    alerts = engine.generate_alerts(test_metrics, [], [])
    
    for alert in alerts:
        print(f"[{alert.severity}] {alert.type}: {alert.message}")