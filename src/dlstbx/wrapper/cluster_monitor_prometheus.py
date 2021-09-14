import logging
import time
from datetime import datetime
from typing import Callable, Dict, Optional, Union

import zocalo.wrapper

from dlstbx.prometheus_cluster_monitor.parse_db import DBParser

logger = logging.getLogger("dlstbx.wrap.cluster_monitor")


class Metric:
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
        **kwargs,
    ):
        self.name = name
        self.labels = labels or []
        self.triggers = triggers or []
        self.event_based_params = event_based_params or {}
        self.behaviour = behaviour
        self.value_key = value_key
        self.extra_arguments = kwargs

    def value(
        self, event: str, value: Union[int, float], params: dict, **kwargs
    ) -> Union[int, float]:
        if self.behaviour.get(event) is None:
            return 0
        return self.behaviour[event](value, **{**params, **kwargs})

    def validate(self, params: dict) -> bool:
        for t in self.triggers:
            if params.get(t) is None:
                return False
        return True

    def parse_labels(self, params: dict) -> str:
        as_str = ""
        for l in self.labels:
            if params.get(l) is not None:
                as_str += f'{l}="{params[l]}",'
        return as_str[:-1]

    def send_to_db(self, event: str, params: dict, dbparser: DBParser):
        raise NotImplementedError(
            f"No method to insert into database backend for {self}"
        )


class Counter(Metric):
    def send_to_db(self, event: str, params: dict, dbparser: DBParser) -> bool:
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
            metric_labels=self.parse_labels(params),
            metric_type="counter",
            metric_value=self.value(event, value, params, **self.extra_arguments),
            timestamp=params.get("timestamp"),
            **extra_params,
        )
        return True


class Gauge(Metric):
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
            metric_labels=self.parse_labels(params),
            metric_type="gauge",
            metric_value=self.value(event, value, params, **kwargs),
            timestamp=params.get("timestamp"),
        )
        return True


class Histogram(Metric):
    def __init__(
        self,
        name: str,
        boundaries: list,
        labels: Optional[list] = None,
        triggers: Optional[list] = None,
        behaviour: Dict[str, Callable] = {
            "start": lambda x, **kwargs: x,
        },
        value_key: Optional[str] = None,
        event_based_params: Optional[Dict[str, Dict]] = {
            "end": {"cluster_end_timestamp": time.time()}
        },
        **kwargs,
    ):
        super().__init__(
            name,
            labels=labels,
            triggers=triggers,
            behaviour=behaviour,
            value_key=value_key,
        )
        self.boundaries = boundaries
        self.labels.append("le")

    def send_to_db(
        self, event: str, params: dict, dbparser: DBParser, **kwargs
    ) -> bool:
        if not self.validate(params):
            return False
        if self.value_key is None:
            raise ValueError("Must specify a value key for histogram metric")
        value = params[self.value_key]
        if not isinstance(value, (int, float)):
            raise ValueError("Must specify a numeric value for histogram metric")
        captured = False
        value_for_sum = 0
        metric_value = self.value(event, value, params, **kwargs)
        for b in self.boundaries:
            if metric_value < b and not captured:
                value_for_sum = metric_value
                bin_value = 1
                captured = True
            else:
                bin_value = 0
            dbparser.insert(
                metric=self.name + "_bucket",
                metric_labels=self.parse_labels({**params, "le": b}),
                metric_type="histogram",
                metric_value=bin_value,
                timestamp=params.get("timestamp"),
            )
        if captured:
            bin_value = 0
        else:
            bin_value = 1
            value_for_sum = metric_value
        dbparser.insert(
            metric=self.name + "_bucket",
            metric_labels=self.parse_labels({**params, "le": "+Inf"}),
            metric_type="histogram",
            metric_value=bin_value,
            timestamp=params.get("timestamp"),
        )
        dbparser.insert(
            metric=self.name + "_count",
            metric_labels=self.parse_labels(params),
            metric_type="histogram",
            metric_value=1,
            timestamp=params.get("timestamp"),
        )
        dbparser.insert(
            metric=self.name + "_sum",
            metric_labels=self.parse_labels(params),
            metric_type="histogram",
            metric_value=value_for_sum,
            timestamp=params.get("timestamp"),
        )


class ClusterMonitorPrometheusWrapper(zocalo.wrapper.BaseWrapper):
    db_parser = DBParser()

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
        image_labels = [
            "cluster",
            "host_name",
            "command",
            "num_pixels",
        ]
        metrics = [
            # this must happen before time stamps are updated as it needs to
            # collect the last time stamp information
            Counter(
                "cluster_cumulative_job_time",
                labels=standard_counter_labels,
                triggers=["cluster", "cluster_job_id"],
                value_key="timestamp",
                behaviour={"end": self._timestamp_diff},
            ),
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
            Histogram(
                "cluster_motion_correction_time_per_micrograph",
                [2, 4, 6, 8, 10, 12],
                labels=image_labels,
                triggers=["cluster", "cluster_job_id", "num_corrected_micrographs"],
                value_key="num_corrected_micrographs",
                behaviour={"info": self._duration_ratio},
            ),
        ]
        return metrics

    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        params = self.recwrap.recipe_step["parameters"]
        event = params["event"]

        metrics = self._metrics(params)

        if (
            params.get("cluster_job_id") is not None
            and params.get("cluster") is not None
        ):
            if event == "start":
                self.db_parser.insert_cluster_info(
                    params["cluster"],
                    params["cluster_job_id"],
                    start_time=params.get("timestamp"),
                    appid=params.get("auto_proc_program_id"),
                )
            if event == "end":
                self.db_parser.insert_cluster_info(
                    params["cluster"],
                    params["cluster_job_id"],
                    end_time=params.get("timestamp"),
                    appid=params.get("auto_proc_program_id"),
                )

        for m in metrics:
            m.send_to_db(event, params, self.db_parser)

        return True

    def _timestamp_diff(self, value: float, **kwargs) -> float:
        cluster_id = kwargs["cluster_job_id"]
        cluster = kwargs["cluster"]
        try:
            rows = self.db_parser.lookup_cluster_info(
                {"cluster_id": cluster_id, "cluster": cluster}
            )
            row = rows[0]
        except IndexError:
            logger.error(
                f"No cluster jobs found in ClusterJobInfo table for cluster {cluster}, ID {cluster_id}"
            )
            raise
        start_time = datetime.timestamp(row.start_time)
        if not start_time:
            return 0
        return value - start_time

    def _duration_ratio(self, value: float, **kwargs) -> float:
        cluster_id = kwargs["cluster_job_id"]
        cluster = kwargs["cluster"]
        if not value:
            logger.warning(
                f"Value passed to duration ratio calculation was {value} for cluster {cluster}, ID {cluster_id}: returning 0."
            )
            return 0
        try:
            rows = self.db_parser.lookup_cluster_info(
                {"cluster_id": cluster_id, "cluster": cluster}
            )
            row = rows[0]
        except IndexError:
            logger.error(
                f"No cluster jobs found in ClusterJobInfo table for cluster {cluster}, ID {cluster_id}"
            )
            raise
        duration = datetime.timestamp(row.end_time) - datetime.timestamp(row.start_time)
        return duration / value
