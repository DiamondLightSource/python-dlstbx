from __future__ import annotations

import logging

import zocalo.wrapper
from prometheus_client import CollectorRegistry, Counter, Histogram, push_to_gateway

from dlstbx.util.version import dlstbx_version

logger = logging.getLogger(__name__)


HISTOGRAM_BUCKETS = [10, 20, 30, 60, 90, 120, 180, 300, 600, 3600, 14400]


class Wrapper(zocalo.wrapper.BaseWrapper):
    name: str | None = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._registry = CollectorRegistry()
        if self.name:
            self._runtime_hist = Histogram(
                "zocalo_wrapper_runtime_seconds",
                "Run time of fast_dp (seconds)",
                registry=self._registry,
                buckets=HISTOGRAM_BUCKETS,
            )
            self._failure_counter = Counter(
                "zocalo_wrap_latency_seconds_failed_total",
                "Total number of failed jobs",
                registry=self._registry,
            )
            self._success_counter = Counter(
                "zocalo_wrap_succeeded_total",
                "Total number of successful jobs",
                registry=self._registry,
            )
            self._timeout_counter = Counter(
                "zocalo_wrap_timed_out_total",
                "Total number of timed out jobs",
                registry=self._registry,
            )

    def prepare(self, payload):
        super().prepare(payload)
        if getattr(self, "status_thread"):
            self.status_thread.set_static_status_field("dlstbx", dlstbx_version())

    def done(self, payload):
        if pushgateway := self.config.storage.get("zocalo.prometheus_pushgateway"):
            logger.info(f"Sending metrics to {pushgateway}")
            try:
                push_to_gateway(
                    pushgateway,
                    job=self.name or "dlstbx.wrap",
                    registry=self._registry,
                )
            except Exception:
                logger.exception(f"Failed sending metrics to {pushgateway}")

        super().done(payload=payload)
