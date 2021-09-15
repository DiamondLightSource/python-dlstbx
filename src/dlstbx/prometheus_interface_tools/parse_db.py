import json
import pathlib
import time
from datetime import datetime
from typing import Any, Dict, Optional, Union

import sqlalchemy
from sqlalchemy.dialects.mysql import insert

from dlstbx.prometheus_interface_tools import ClusterJobInfo, PrometheusInterface


class DBParser:
    def __init__(self):
        try:
            configuration = pathlib.Path(
                "/dls_sw/apps/zocalo/secrets/sql-zocalo-profiling.json"
            ).read_text()
        except PermissionError:
            configuration = pathlib.Path(
                "/dls_sw/apps/zocalo/secrets/sql-zocalo-readonly.json"
            ).read_text()
        secret_ingredients = json.loads(configuration)
        sqlalchemy_url = (
            "mysql+mysqlconnector://{user}:{passwd}@{host}:{port}/{db}".format(
                **secret_ingredients
            )
        )
        self._sessionmaker = sqlalchemy.orm.sessionmaker(
            bind=sqlalchemy.create_engine(
                sqlalchemy_url, connect_args={"use_pure": True}
            )
        )

    def lookup(self, filter_by: Dict[str, Any]):
        with self._sessionmaker() as session:
            query = session.query(PrometheusInterface).filter(
                *[
                    getattr(PrometheusInterface, col) == val
                    for col, val in filter_by.items()
                ]
            )
        return query.all()

    def lookup_cluster_info(self, filter_by: Dict[str, Any]):
        with self._sessionmaker() as session:
            query = session.query(ClusterJobInfo).filter(
                *[getattr(ClusterJobInfo, col) == val for col, val in filter_by.items()]
            )
        return query.all()

    def insert_cluster_info(
        self,
        cluster: str,
        cluster_id: int,
        appid: Optional[int] = None,
        start_time: Optional[float] = None,
        end_time: Optional[float] = None,
    ) -> None:
        if start_time is None:
            start_time = time.time()
        if end_time is None:
            end = None
        else:
            end = datetime.fromtimestamp(end_time)
        insert_cmd = insert(ClusterJobInfo).values(
            cluster=cluster,
            cluster_id=cluster_id,
            auto_proc_program_id=appid,
            start_time=datetime.fromtimestamp(start_time),
            end_time=end,
        )
        update = insert_cmd.on_duplicate_key_update(
            cluster=cluster,
            cluster_id=cluster_id,
            auto_proc_program_id=appid,
            start_time=ClusterJobInfo.start_time,
            end_time=end,
        )
        with self._sessionmaker() as session:
            session.execute(update)
            session.commit()
        return

    def insert(
        self,
        metric: str,
        metric_type: str,
        metric_labels: str = "",
        metric_value: Union[int, float] = 0,
        timestamp: Optional[float] = None,
        cluster_end_timestamp: Optional[float] = None,
    ) -> None:
        if timestamp is None:
            timestamp = time.time()
        insert_cmd = insert(PrometheusInterface).values(
            metric=metric,
            metric_type=metric_type,
            metric_labels=metric_labels,
            metric_value=metric_value,
            timestamp=datetime.fromtimestamp(timestamp),
            cluster_end_timestamp=cluster_end_timestamp,
        )
        update = insert_cmd.on_duplicate_key_update(
            metric_type=metric_type,
            metric_value=PrometheusInterface.metric_value + metric_value,
            timestamp=datetime.fromtimestamp(timestamp),
            cluster_end_timestamp=cluster_end_timestamp,
        )
        with self._sessionmaker() as session:
            session.execute(update)
            session.commit()
        return

    def reset(
        self,
        metric: str,
        metric_type: str,
        metric_labels: str = "",
        timestamp: Optional[float] = None,
        cluster_end_timestamp=None,
    ) -> None:
        insert_cmd = insert(PrometheusInterface).values(
            metric=metric,
            metric_type=metric_type,
            metric_labels=metric_labels,
            metric_value=0,
            timestamp=datetime.fromtimestamp(timestamp),
            cluster_end_timestamp=cluster_end_timestamp,
        )
        update = insert_cmd.on_duplicate_key_update(
            metric_type=metric_type,
            metric_value=0,
            timestamp=datetime.fromtimestamp(timestamp),
            cluster_end_timestamp=cluster_end_timestamp,
        )
        with self._sessionmaker() as session:
            session.execute(update)
            session.commit()
        return

    @property
    def text(self) -> str:
        as_text = ""
        with self._sessionmaker() as session:
            query = session.query(PrometheusInterface).all()
        metrics = {}
        for metric_line in query:
            # do this becasue of histograms
            if metric_line.metric.endswith("_count"):
                metric_name = metric_line.metric[:-6]
            elif metric_line.metric.endswith("_sum"):
                metric_name = metric_line.metric[:-4]
            else:
                metric_name = metric_line.metric
            if metric_name not in metrics.keys():
                metrics[metric_name] = []
            metrics[metric_name].append(metric_line)

        for m, lines in metrics.items():
            as_text += self._type_line(lines[0].metric_type.name, m)
            for l in lines:
                as_text += self._metric_line(l, m)
            as_text += "\n"
        return as_text

    @staticmethod
    def _type_line(metric_type: str, metric_name: str) -> str:
        return f"# TYPE {metric_name} {metric_type}\n"

    @staticmethod
    def _metric_line(db_row, metric_name: str) -> str:
        return f"{metric_name}{{{db_row.metric_labels}}} {db_row.metric_value}\n"

    def prune(self) -> int:
        one_hour_ago = sqlalchemy.sql.expression.text("NOW() - INTERVAL 1 HOUR")
        with self._sessionmaker() as session:
            records_pruned = (
                session.query(PrometheusInterface)
                .filter(PrometheusInterface.cluster_end_timestamp < one_hour_ago)
                .delete(synchronize_session=False)
            )
            session.commit()
            info_records_pruned = (
                session.query(ClusterJobInfo)
                .filter(ClusterJobInfo.end_time < one_hour_ago)
                .delete(synchronize_session=False)
            )
            session.commit()
            records_pruned += info_records_pruned
        return records_pruned
