import logging
import time
from typing import Callable, Dict, Optional, Union

import zocalo.wrapper

from dlstbx.prometheus_cluster_monitor.parse_db import DBParser

logger = logging.getLogger("dlstbx.wrap.cluster_monitor")


class Counter:
    def __init__(
        self,
        name: str,
        labels: Optional[list] = None,
        triggers: Optional[list] = None,
        behaviour: Dict[str, Callable] = {
            "start": lambda x, **kwargs: x,
        },
        value_key: Optional[str] = None,
        event_based_params: Optional[Dict[str, Dict]] = {
            "end": {"cluster_end_timestamp": time.time()}
        },
    ):
        self.name = name
        self.labels = labels or []
        self.triggers = triggers or []
        self.event_based_params = event_based_params or {}
        self.behaviour = behaviour
        self.value_key = value_key

    def value(
        self, event: str, value: Union[int, float], **kwargs
    ) -> Union[int, float]:
        if self.behaviour.get(event) is None:
            return 0
        return self.behaviour[event](value, **kwargs)

    def validate(self, params: dict) -> bool:
        for t in self.triggers:
            if params.get(t) is None:
                return False
        return True

    def labels(self, params: dict) -> str:
        as_str = ""
        for l in self.labels:
            if params.get(l) is not None:
                as_str += f'{l}="{params[l]}",'
        return as_str[:-1]

    def send_to_db(
        self, event: str, params: dict, dbparser: DBParser, **kwargs
    ) -> bool:
        if not self.validate(params):
            return False
        if self.value_key is None:
            value = 1
        else:
            value = params[self.value_key]
        # counter cannot decrease (unless via a reset to 0)
        if not value or value < 0:
            return False
        extra_params = self.event_based_params.get(event, {})
        dbparser.insert(
            metric=self.name,
            metric_labels=self.labels(params),
            metric_type="gauge",
            metric_value=self.value(event, value, **kwargs),
            cluster_id=params.get("cluster_job_id"),
            auto_proc_program_id=params.get("auto_proc_program_id"),
            timestamp=params.get("timestamp"),
            **extra_params,
        )
        return True


class Gauge(Counter):
    def __init__(
        self,
        name: str,
        labels: Optional[list] = None,
        triggers: Optional[list] = None,
        behaviour: Dict[str, Callable] = {
            "start": lambda x, **kwargs: x,
            "end": lambda x, **kwargs: -x,
        },
        value_key: Optional[str] = None,
    ):
        super().__init__(
            name,
            labels=labels,
            triggers=triggers,
            behaviour=behaviour,
            value_key=value_key,
        )

    def send_to_db(
        self, event: str, params: dict, dbparser: DBParser, **kwargs
    ) -> bool:
        if not self.validate(params):
            return False
        if self.value_key is None:
            value = 1
        else:
            value = params[self.value_key]
        if not value:
            return False
        dbparser.insert(
            metric=self.name,
            metric_labels=self.labels(params),
            metric_type="gauge",
            metric_value=self.value(event, value, **kwargs),
            cluster_id=params.get("cluster_job_id"),
            auto_proc_program_id=params.get("auto_proc_program_id"),
            timestamp=params.get("timestamp"),
        )
        return True


class ClusterMonitorPrometheusWrapper(zocalo.wrapper.BaseWrapper):
    def _metrics(self, params: dict) -> list:
        standard_gauge_labels = [
            "cluster",
            "host_name",
            "auto_proc_program_id",
            "cluster_job_id",
            "command",
        ]
        standard_counter_labels = [
            "cluster",
            "host_name",
            "command",
        ]
        metrics = [
            Gauge(
                "cluster_current_num_jobs",
                labels=standard_gauge_labels,
                triggers=["cluster", "cluster_job_id"],
            ),
            Gauge(
                "cluster_current_num_gpus_in_use",
                labels=standard_gauge_labels,
                triggers=["cluster", "cluster_job_id", "num_gpus"],
                value_key="num_gpus",
            ),
            Gauge(
                "cluster_current_num_mpi_ranks_in_use",
                labels=standard_gauge_labels,
                triggers=["cluster", "cluster_job_id", "num_mpi_ranks"],
                value_key="num_mpi_ranks",
            ),
            Counter(
                "cluster_total_num_jobs",
                labels=standard_counter_labels,
                triggers=["cluster", "cluster_job_id"],
            ),
        ]
        return metrics

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        db_parser = DBParser()

        params = self.recwrap.recipe_step["parameters"]
        event = params["event"]

        metrics = self._metrics(params)

        for m in metrics:
            m.send_to_db(event, params, db_parser)
