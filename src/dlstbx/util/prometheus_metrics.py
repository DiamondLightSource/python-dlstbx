import logging
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

logger = logging.getLogger(__name__)


class BasePrometheusMetrics(ABC):
    """The metrics abstract base class. Provides registry, http endpoint and a
    method to record metrics.

    Use case:
        For services running on Kubernetes, create subclass containing
        the method "create_metrics" which creates prometheus style
        metrics as class attributes.

    class PrometheusMetrics(Metrics.BasePrometheusMetrics):
        def create_metrics(self):
            self.job_triggered = Counter(
                name="job_triggered",
                documentation="Counts each job as they are triggered",
                labelnames=["job"],
                registry=self.registry,
            )

    For services not running on kubernetes the ports and address' used for
    the Prometheus endpoint may need changing.
    """

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
        metric = getattr(self, metric_name)
        if metric:
            if isinstance(metric, Counter):
                metric.labels(*labels).inc()

            elif isinstance(metric, Histogram):
                metric.labels(*labels).observe(value)

            elif isinstance(metric, Summary):
                metric.labels(*labels).observe(value)

            elif isinstance(metric, Gauge):
                metric.labels(*labels).set(value)
        else:
            logger.error("Named metric not present as class attribute")


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


#    def record_metric(
#        self,
#        metric_name: str,
#        labels: List[str],
#        value: Optional[Union[int, float]] = None,
#    ):
#        metric = getattr(self, metric_name, False)
#        if metric:
#            if isinstance(metric, Counter):
#                metric.labels(*labels).inc()
#
#            elif isinstance(metric, Histogram):
#                metric.labels(*labels).observe(value)
#
#            elif isinstance(metric, Summary):
#                metric.labels(*labels).observe(value)
#
#            elif isinstance(metric, Gauge):
#                metric.labels(*labels).set(value)
#        else:
#            logger.error("Named metric not present as class attribute")
