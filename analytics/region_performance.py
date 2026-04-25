"""
Region Performance Intelligence
Analyzes regional performance and trends
"""

from dataclasses import dataclass
from typing import List, Dict, Optional
import logging

logger = logging.getLogger(__name__)


@dataclass
class RegionPerformance:
    """Region performance data"""
    region: str
    total_shipments: int
    delivered: int
    delayed: int
    in_transit: int
    on_time_rate: float
    avg_delivery_hours: float
    trend: str  # improving / declining / stable


class RegionPerformanceAnalyzer:
    """Analyzes region performance and identifies trends"""
    
    def __init__(self):
        self.previous_stats = {}
    
    def analyze_regions(self, shipments: List[dict]) -> List[RegionPerformance]:
        """Analyze performance for all regions"""
        
        # Group by region
        region_data = {}
        for shipment in shipments:
            region = shipment.get("region", "unknown")
            if region not in region_data:
                region_data[region] = {
                    "total": 0,
                    "delivered": 0,
                    "delayed": 0,
                    "in_transit": 0,
                    "delivery_hours": []
                }
            
            region_data[region]["total"] += 1
            status = shipment.get("status", "")
            
            if status == "delivered":
                region_data[region]["delivered"] += 1
            elif status == "delayed":
                region_data[region]["delayed"] += 1
            elif status == "in_transit":
                region_data[region]["in_transit"] += 1
            
            actual_hours = shipment.get("actual_delivery_hours")
            if actual_hours:
                region_data[region]["delivery_hours"].append(actual_hours)
        
        # Calculate metrics
        results = []
        for region, data in region_data.items():
            total = data["total"]
            delivered = data["delivered"]
            delayed = data["delayed"]
            
            on_time = delivered  # delivered = on time
            on_time_rate = (on_time / total * 100) if total > 0 else 0
            
            avg_hours = sum(data["delivery_hours"]) / len(data["delivery_hours"]) if data["delivery_hours"] else 0
            
            # Determine trend
            trend = self._calculate_trend(region, on_time_rate)
            
            results.append(RegionPerformance(
                region=region,
                total_shipments=total,
                delivered=delivered,
                delayed=delayed,
                in_transit=data["in_transit"],
                on_time_rate=round(on_time_rate, 2),
                avg_delivery_hours=round(avg_hours, 2),
                trend=trend
            ))
        
        return sorted(results, key=lambda x: x.on_time_rate, reverse=True)
    
    def _calculate_trend(self, region: str, current_rate: float) -> str:
        """Calculate performance trend for a region"""
        
        if region in self.previous_stats:
            previous_rate = self.previous_stats[region]
            diff = current_rate - previous_rate
            
            if diff > 5:
                return "improving"
            elif diff < -5:
                return "declining"
            else:
                return "stable"
        
        self.previous_stats[region] = current_rate
        return "stable"
    
    def get_best_worst_regions(self, region_performances: List[RegionPerformance]) -> tuple:
        """Get best and worst performing regions"""
        
        if not region_performances:
            return None, None
        
        sorted_by_rate = sorted(region_performances, key=lambda x: x.on_time_rate, reverse=True)
        
        return sorted_by_rate[0], sorted_by_rate[-1]


def analyze_region_performance(shipments: List[dict]) -> List[RegionPerformance]:
    """Convenience function to analyze region performance"""
    analyzer = RegionPerformanceAnalyzer()
    return analyzer.analyze_regions(shipments)


if __name__ == "__main__":
    # Test the analyzer
    test_shipments = [
        {"region": "north", "status": "delivered", "actual_delivery_hours": 10},
        {"region": "north", "status": "delivered", "actual_delivery_hours": 12},
        {"region": "north", "status": "delayed", "actual_delivery_hours": 24},
        {"region": "south", "status": "delivered", "actual_delivery_hours": 8},
        {"region": "south", "status": "in_transit", "actual_delivery_hours": None},
        {"region": "east", "status": "delivered", "actual_delivery_hours": 15},
        {"region": "east", "status": "delayed", "actual_delivery_hours": 30},
    ]
    
    analyzer = RegionPerformanceAnalyzer()
    results = analyzer.analyze_regions(test_shipments)
    
    for r in results:
        print(f"{r.region}: {r.on_time_rate}% on-time ({r.trend})")