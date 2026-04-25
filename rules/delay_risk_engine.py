"""
Smart Delay Risk Prediction Engine
Rule-based intelligence for shipment delay prediction
"""

from dataclasses import dataclass
from typing import Optional, List
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


@dataclass
class RiskAssessment:
    """Risk assessment result"""
    shipment_id: str
    risk_score: int  # 0-100
    risk_level: str  # Low / Medium / High / Critical
    reason: str
    rules_triggered: List[str]


class DelayRiskEngine:
    """Rule-based delay risk prediction engine"""
    
    def __init__(self):
        self.rules = [
            self._rule_distance_sla,
            self._rule_warehouse_history,
            self._rule_region_volume,
            self._rule_previous_delay,
            self._rule_unrealistic_sla,
        ]
    
    def assess_risk(self, shipment: dict, warehouse_stats: dict = None, 
                    region_stats: dict = None) -> RiskAssessment:
        """Assess delay risk for a shipment"""
        
        rules_triggered = []
        risk_score = 0
        
        # Run all rules
        for rule_func in self.rules:
            result = rule_func(shipment, warehouse_stats, region_stats)
            if result["triggered"]:
                rules_triggered.append(result["rule_name"])
                risk_score += result["score"]
        
        # Cap risk score at 100
        risk_score = min(risk_score, 100)
        
        # Determine risk level
        if risk_score >= 80:
            risk_level = "Critical"
        elif risk_score >= 60:
            risk_level = "High"
        elif risk_score >= 30:
            risk_level = "Medium"
        else:
            risk_level = "Low"
        
        # Generate reason
        reason = "; ".join(rules_triggered) if rules_triggered else "No risk factors detected"
        
        return RiskAssessment(
            shipment_id=shipment.get("shipment_id", "Unknown"),
            risk_score=risk_score,
            risk_level=risk_level,
            reason=reason,
            rules_triggered=rules_triggered
        )
    
    def _rule_distance_sla(self, shipment: dict, warehouse_stats: dict = None, 
                          region_stats: dict = None) -> dict:
        """Rule: If distance_km > 1000 AND status = in_transit → High Risk"""
        distance = shipment.get("distance_km", 0)
        status = shipment.get("status", "")
        
        triggered = distance > 1000 and status == "in_transit"
        return {
            "rule_name": "Long-distance shipment in transit",
            "triggered": triggered,
            "score": 30 if triggered else 0
        }
    
    def _rule_warehouse_history(self, shipment: dict, warehouse_stats: dict = None,
                                 region_stats: dict = None) -> dict:
        """Rule: If warehouse has historical delay rate > 20% → Medium Risk"""
        warehouse_id = shipment.get("warehouse_id", "")
        
        if warehouse_stats and warehouse_id in warehouse_stats:
            delay_rate = warehouse_stats[warehouse_id].get("delay_rate", 0)
            triggered = delay_rate > 20
            return {
                "rule_name": f"Warehouse {warehouse_id} delay rate {delay_rate}%",
                "triggered": triggered,
                "score": 25 if triggered else 0
            }
        
        return {"rule_name": "Warehouse history check", "triggered": False, "score": 0}
    
    def _rule_region_volume(self, shipment: dict, warehouse_stats: dict = None,
                           region_stats: dict = None) -> dict:
        """Rule: If region = north and shipment volume high → Medium Risk"""
        region = shipment.get("region", "").lower()
        
        if region_stats and region in region_stats:
            volume = region_stats[region].get("volume", 0)
            triggered = region == "north" and volume > 100
            return {
                "rule_name": f"High volume in {region} region",
                "triggered": triggered,
                "score": 20 if triggered else 0
            }
        
        return {"rule_name": "Region volume check", "triggered": False, "score": 0}
    
    def _rule_previous_delay(self, shipment: dict, warehouse_stats: dict = None,
                            region_stats: dict = None) -> dict:
        """Rule: If shipment already delayed once → Critical Risk"""
        is_delayed = shipment.get("is_delayed", False)
        
        triggered = is_delayed
        return {
            "rule_name": "Shipment already delayed",
            "triggered": triggered,
            "score": 35 if triggered else 0
        }
    
    def _rule_unrealistic_sla(self, shipment: dict, warehouse_stats: dict = None,
                             region_stats: dict = None) -> dict:
        """Rule: If estimated_delivery_hours < 5 and distance > 800 → Unrealistic SLA Risk"""
        estimated = shipment.get("estimated_delivery_hours", 0)
        distance = shipment.get("distance_km", 0)
        
        triggered = estimated < 5 and distance > 800
        return {
            "rule_name": "Unrealistic SLA (short time, long distance)",
            "triggered": triggered,
            "score": 25 if triggered else 0
        }


def assess_shipment_risk(shipment: dict, warehouse_stats: dict = None,
                        region_stats: dict = None) -> RiskAssessment:
    """Convenience function to assess shipment risk"""
    engine = DelayRiskEngine()
    return engine.assess_risk(shipment, warehouse_stats, region_stats)


if __name__ == "__main__":
    # Test the engine
    test_shipment = {
        "shipment_id": "SHP-001",
        "distance_km": 1200,
        "status": "in_transit",
        "warehouse_id": "WH-NYC",
        "region": "north",
        "is_delayed": False,
        "estimated_delivery_hours": 4,
    }
    
    engine = DelayRiskEngine()
    result = engine.assess_risk(test_shipment)
    print(f"Risk Score: {result.risk_score}")
    print(f"Risk Level: {result.risk_level}")
    print(f"Reason: {result.reason}")