"""
CloudWatch metrics emission for MarketFlow. All components use this module
to emit counters, latencies, and gauges without blocking on the CloudWatch API.
"""

from src.monitoring.cloudwatch_metrics import CloudWatchMetrics, get_metrics

__all__ = ["CloudWatchMetrics", "get_metrics"]
