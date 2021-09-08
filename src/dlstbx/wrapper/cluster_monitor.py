import logging
import time

import zocalo.wrapper

from dlstbx.prometheus_cluster_monitor import parse_db

logger = logging.getLogger("dlstbx.wrap.cluster_monitor")


class ClusterMonitorPrometheusWrapper(zocalo.wrapper.BaseWrapper):
    def run(self):
        assert hasattr(self, "recwrap"), "No recipewrapper object found"

        db_parser = parse_db.DBParser()

        params = self.recwrap.recipe_step["parameters"]
        event = params["event"]
        labels = {
            "cluster": params.get("cluster"),
            "host_name": params.get("host"),
            "auto_proc_program_id": params.get("program_id"),
            "cluster_job_id": params.get("job_id"),
            "command": params.get("command"),
        }
        labels_string = self._parse_labels_to_string(labels)
        metrics = ["clusters_current_job_count"]
        metric_types = ["gauge"]
        if params.get("num_gpus") is not None:
            metrics.extend(
                ["current_gpus_in_use_count", "current_mpi_ranks_in_use_count"]
            )
            metric_types.extend(["gauge", "gauge"])
        if event == "start":
            metrics.append("clusters_total_job_count")
            metric_types.append("counter")
            values = [1]
            if params.get("num_gpus") is not None:
                values.extend([params.get("num_gpus"), params.get("num_mpi_ranks")])
            values.append(1)
            for met, met_type, val in zip(metrics, metric_types, values):
                db_parser.insert(
                    metric=met,
                    metric_labels=labels_string,
                    metric_type=met_type,
                    metric_value=val,
                    cluster_id=params.get("job_id"),
                    auto_proc_program_id=params.get("program_id"),
                    timestamp=params.get("timestamp"),
                )
        elif event == "end":
            values = [1]
            if params.get("num_gpus") is not None:
                values.extend([-params.get("num_gpus"), -params.get("num_mpi_ranks")])
            for met, met_type, val in zip(metrics, metric_types, values):
                db_parser.insert(
                    metric=met,
                    metric_labels=labels_string,
                    metric_type=met_type,
                    metric_value=val,
                    cluster_id=params.get("job_id"),
                    auto_proc_program_id=params.get("program_id"),
                    timestamp=params.get("timestamp"),
                    cluster_end_timestamp=time.time(),
                )

    @staticmethod
    def _parse_labels_to_string(labels):
        as_str = ""
        for k, v in labels.items():
            as_str += f'{k}="{v}",'
        return as_str[:-1]
