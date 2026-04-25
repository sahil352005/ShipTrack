"""
Warehouse Bottleneck Detection Engine
Rule-based intelligence for warehouse performance analysis
"""

from dataclasses import dataclass
from typing import List, Dict, Optional
import logging

logger = logging.getLogger(__name__)


@dataclass
class WarehouseBottleneck:
    """Warehouse bottleneck detection result"""
    warehouse_id: str
    bottleneck: bool
    severity: str  # Low / Medium / High / Critical
    reason: str
    metrics: dict


class WarehouseBottleneckDetector:
    """Rule-based warehouse bottleneck detection"""
    
    # Thresholds
    PROCESSING_TIME_THRESHOLD = 4  # hours
    DELAY_RATE_THRESHOLD = 15  # percent
    VOLUME_CAPACITY_RATIO = 0.85  # 85% capacity = stress
    
    def __init__(self):
        self.rules = [
            self._rule_processing_time,
            self._rule_delay_rate,
            self._rule_capacity_stress,
        ]
    
    def detect_bottlenecks(self, warehouse_stats: List[dict]) -> List[WarehouseBottleneck]:
        """Detect bottlenecks for all warehouses"""
        
        results = []
        
        for warehouse in warehouse_stats:
            result = self._assess_warehouse(warehouse)
            results.append(result)
        
        return results
    
    def _assess_warehouse(self, warehouse: dict) -> WarehouseBottleneck:
        """Assess a single warehouse for bottlenecks"""
        
        bottleneck = False
        severity = "None"
        reasons = []
        severity_score = 0
        
        # Check processing time
        proc_result = self._rule_processing_time(warehouse)
        if proc_result["triggered"]:
            bottleneck = True
            reasons.append(proc_result["reason"])
            severity_score = max(severity_score, proc_result["severity"])
        
        # Check delay rate
        delay_result = self._rule_delay_rate(warehouse)
        if delay_result["triggered"]:
            bottleneck = True
            reasons.append(delay_result["reason"])
            severity_score = max(severity_score, delay_result["severity"])
        
        # Check capacity
        cap_result = self._rule_capacity_stress(warehouse)
        if cap_result["triggered"]:
            bottleneck = True
            reasons.append(cap_result["reason"])
            severity_score = max(severity_score, cap_result["severity"])
        
        # Determine severity
        if severity_score >= 3:
            severity = "Critical"
        elif severity_score >= 2:
            severity = "High"
        elif severity_score >= 1:
            severity = "Medium"
        else:
            severity = "Low"
        
        return WarehouseBottleneck(
            warehouse_id=warehouse.get("warehouse_id", "Unknown"),
            bottleneck=bottleneck,
            severity=severity,
            reason="; ".join(reasons) if reasons else "Operating normally",
            metrics={
                "avg_processing_hours": warehouse.get("avg_processing_hours", 0),
                "delay_rate": warehouse.get("delay_rate", 0),
                "volume": warehouse.get("total_shipments", 0),
                "capacity": warehouse.get("capacity", 100)
            }
        )
    
    def _rule_processing_time(self, warehouse: dict) -> dict:
        """Rule: If avg processing time > threshold → Bottleneck"""
        avg_time = warehouse.get("avg_processing_hours", 0)
        
        triggered = avg_time > self.PROCESSING_TIME_THRESHOLD
        severity = 2 if triggered else 0
        
        return {
            "triggered": triggered,
            "reason": f"Avg processing time {avg_time}h exceeds {self.PROCESSING_TIME_THRESHOLD}h",
            "severity": severity
        }
    
    def _rule_delay_rate(self, warehouse: dict) -> dict:
        """Rule: If delayed shipments > 15% → Bottleneck"""
        delay_rate = warehouse.get("delay_rate", 0)
        
        triggered = delay_rate > self.DELAY_RATE_THRESHOLD
        severity = 3 if triggered else 0
        
        return {
            "triggered": triggered,
            "reason": f"Delay rate {delay_rate}% exceeds {self.DELAY_RATE_THRESHOLD}%",
            "severity": severity
        }
    
    def _rule_capacity_stress(self, warehouse: dict) -> dict:
        """Rule: If shipment volume exceeds capacity → Bottleneck"""
        volume = warehouse.get("total_shipments", 0)
        capacity = warehouse.get("capacity", 100)
        
        ratio = volume / capacity if capacity > 0 else 0
        triggered = ratio > self.VOLUME_CAPACITY_RATIO
        
        severity = 1 if triggered else 0
        
        return {
            "triggered": triggered,
            "reason": f"Volume {volume} at {int(ratio*100)}% capacity",
            "severity": severity
        }


def detect_warehouse_bottlenecks(warehouse_stats: List[dict]) -> List[WarehouseBottleneck]:
    """Convenience function to detect warehouse bottlenecks"""
    detector = WarehouseBottleneckDetector()
    return detector.detect_bottlenecks(warehouse_stats)


if __name__ == "__main__":
    # Test the detector
    test_warehouses = [
        {"warehouse_id": "WH-NYC", "avg_processing_hours": 5, "delay_rate": 25, "total_shipments": 120, "capacity": 100},
        {"warehouse_id": "WH-LAX", "avg_processing_hours": 2, "delay_rate": 8, "total_shipments": 80, "capacity": 100},
        {"warehouse_id": "WH-CHI", "avg_processing_hours": 3, "delay_rate": 12, "total_shipments": 95, "capacity": 100},
    ]
    
    detector = WarehouseBottleneckDetector()
    results = detector.detect_bottlenecks(test_warehouses)
    
    for r in results:
        print(f"{r.warehouse_id}: {r.bottleneck} ({r.severity}) - {r.reason}")