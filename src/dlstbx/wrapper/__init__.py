from __future__ import annotations

import zocalo.wrapper
from prometheus_client import CollectorRegistry, Counter, Histogram, push_to_gateway

from dlstbx.util.version import dlstbx_version

HISTOGRAM_BUCKETS = [10, 20, 30, 60, 90, 120, 180, 300, 600, 3600, 14400]


class Wrapper(zocalo.wrapper.BaseWrapper):
    name: str | None = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._registry = CollectorRegistry()
        if self.name:
            self._runtime_hist = Histogram(
                "zocalo_wrap_runtime_seconds",
                "Run time of zocalo wrapper (seconds)",
                labelnames=("name",),
                registry=self._registry,
                buckets=HISTOGRAM_BUCKETS,
            ).labels(name=self.name)
            self._failure_counter = Counter(
                "zocalo_wrap_failed_total",
                "Total number of failed jobs",
                labelnames=("name",),
                registry=self._registry,
            ).labels(name=self.name)
            self._success_counter = Counter(
                "zocalo_wrap_succeeded_total",
                "Total number of successful jobs",
                labelnames=("name",),
                registry=self._registry,
            ).labels(name=self.name)
            self._timeout_counter = Counter(
                "zocalo_wrap_timed_out_total",
                "Total number of timed out jobs",
                labelnames=("name",),
                registry=self._registry,
            ).labels(name=self.name)

    def prepare(self, payload):
        super().prepare(payload)
        if getattr(self, "status_thread"):
            self.status_thread.set_static_status_field("dlstbx", dlstbx_version())

    def done(self, payload):
        if pushgateway := self.config.storage.get("zocalo.prometheus_pushgateway"):
            self.log.info(f"Sending metrics to {pushgateway}")
            try:
                push_to_gateway(
                    pushgateway,
                    job="dlstbx.wrap",
                    registry=self._registry,
                )
            except Exception:
                self.log.exception(f"Failed sending metrics to {pushgateway}")

        super().done(payload=payload)
