from abc import ABC, abstractmethod
from typing import List, Optional, Union

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    Summary,
    start_http_server,
)


class BasePrometheusMetrics(ABC):
    """The metrics base class. Provides registry, http endpoint and metric setters"""

    def __init__(self, port: int = 8080, address: str = "0.0.0.0"):
        self.registry = CollectorRegistry()
        start_http_server(port, address, registry=self.registry)

        self.create_metrics()

    @abstractmethod
    def create_metrics(self):
        raise NotImplementedError

    def record_metric(
        self,
        metric_name: str,
        labels: List[str],
        value: Optional[Union[int, float]] = None,
    ):
        metric = self._metrics.get(metric_name)

        if isinstance(metric, Counter):
            metric.labels(*labels).inc()
            # err failed to increment counter

        elif isinstance(metric, Histogram):
            metric.labels(*labels).observe(value)
            # err

        elif isinstance(metric, Summary):
            metric.labels(*labels).observe(value)
            # err

        elif isinstance(metric, Gauge):
            metric.labels(*labels).set(value)
            # err


class NoMetrics(BasePrometheusMetrics):
    """An empty metrics class. Instantiate this as a services metrics object
    if the service has metrics integrated, but not used."""

    def create_metrics(self):
        pass

    def record_metric(
        self,
        metric_name: str,
        labels: List[str],
        value: Optional[Union[int, float]] = None,
    ):
        pass
