from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import List, Optional, Union

from prometheus_client import Counter, Gauge, Histogram, Summary

logger = logging.getLogger(__name__)


class BasePrometheusMetrics(ABC):
    """The metrics abstract base class. Provides registry, http endpoint and a
    method to record metrics.

    Use case:
        For services running on Kubernetes, create a subclass containing
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

    Do not use on services not running on kubernetes.
    """

    def __init__(self, port: int = 8080, address: str = "0.0.0.0"):
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
        metric = False
        try:
            metric = getattr(self, metric_name)
        except AttributeError:
            logger.exception("Named metric not present as class attribute")
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
            logger.error("Metric not recorded")


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
