"""
SLA Violation Monitoring Engine
Tracks and reports Service Level Agreement failures
"""

from dataclasses import dataclass
from typing import List, Dict, Optional
import logging

logger = logging.getLogger(__name__)


@dataclass
class SLAViolation:
    """SLA violation record"""
    shipment_id: str
    warehouse_id: str
    region: str
    estimated_hours: int
    actual_hours: int
    violation_hours: float
    severity: str  # Low / Medium / High / Critical


@dataclass
class SLAMetrics:
    """SLA metrics summary"""
    total_violations: int
    violation_rate: float
    worst_warehouse: Optional[str]
    worst_region: Optional[str]
    avg_violation_hours: float
    violations_by_warehouse: Dict[str, int]
    violations_by_region: Dict[str, int]


class SLAMonitor:
    """Monitors SLA violations"""
    
    GRACE_PERIOD_HOURS = 2  # Allow 2 hours grace
    
    def __init__(self):
        pass
    
    def check_violation(self, shipment: dict) -> Optional[SLAViolation]:
        """Check if a shipment violates SLA"""
        
        estimated = shipment.get("estimated_delivery_hours", 0)
        actual = shipment.get("actual_delivery_hours")
        
        if actual is None:
            return None
        
        violation = actual - estimated - self.GRACE_PERIOD_HOURS
        
        if violation > 0:
            # Determine severity
            if violation > 24:
                severity = "Critical"
            elif violation > 12:
                severity = "High"
            elif violation > 6:
                severity = "Medium"
            else:
                severity = "Low"
            
            return SLAViolation(
                shipment_id=shipment.get("shipment_id", "Unknown"),
                warehouse_id=shipment.get("warehouse_id", "Unknown"),
                region=shipment.get("region", "Unknown"),
                estimated_hours=estimated,
                actual_hours=actual,
                violation_hours=violation,
                severity=severity
            )
        
        return None
    
    def analyze_violations(self, shipments: List[dict]) -> SLAMetrics:
        """Analyze all shipments for SLA violations"""
        
        violations = []
        warehouse_violations = {}
        region_violations = {}
        
        for shipment in shipments:
            violation = self.check_violation(shipment)
            if violation:
                violations.append(violation)
                
                # Count by warehouse
                wh = violation.warehouse_id
                warehouse_violations[wh] = warehouse_violations.get(wh, 0) + 1
                
                # Count by region
                reg = violation.region
                region_violations[reg] = region_violations.get(reg, 0) + 1
        
        total = len(shipments)
        total_violations = len(violations)
        
        # Find worst warehouse
        worst_warehouse = max(warehouse_violations, key=warehouse_violations.get) if warehouse_violations else None
        
        # Find worst region
        worst_region = max(region_violations, key=region_violations.get) if region_violations else None
        
        # Calculate average violation hours
        avg_violation = sum(v.violation_hours for v in violations) / total_violations if total_violations > 0 else 0
        
        return SLAMetrics(
            total_violations=total_violations,
            violation_rate=(total_violations / total * 100) if total > 0 else 0,
            worst_warehouse=worst_warehouse,
            worst_region=worst_region,
            avg_violation_hours=round(avg_violation, 2),
            violations_by_warehouse=warehouse_violations,
            violations_by_region=region_violations
        )


def check_sla_violations(shipments: List[dict]) -> SLAMetrics:
    """Convenience function to check SLA violations"""
    monitor = SLAMonitor()
    return monitor.analyze_violations(shipments)


if __name__ == "__main__":
    # Test the SLA monitor
    test_shipments = [
        {"shipment_id": "SHP-001", "warehouse_id": "WH-NYC", "region": "north", "estimated_delivery_hours": 10, "actual_delivery_hours": 15},
        {"shipment_id": "SHP-002", "warehouse_id": "WH-LAX", "region": "south", "estimated_delivery_hours": 8, "actual_delivery_hours": 12},
        {"shipment_id": "SHP-003", "warehouse_id": "WH-NYC", "region": "north", "estimated_delivery_hours": 12, "actual_delivery_hours": 14},
        {"shipment_id": "SHP-004", "warehouse_id": "WH-CHI", "region": "east", "estimated_delivery_hours": 6, "actual_delivery_hours": 7},
    ]
    
    monitor = SLAMonitor()
    metrics = monitor.analyze_violations(test_shipments)
    
    print(f"Total Violations: {metrics.total_violations}")
    print(f"Violation Rate: {metrics.violation_rate:.1f}%")
    print(f"Worst Warehouse: {metrics.worst_warehouse}")
    print(f"Worst Region: {metrics.worst_region}")