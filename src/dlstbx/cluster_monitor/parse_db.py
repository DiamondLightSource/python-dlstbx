import json
import pathlib

import sqlalchemy
from sqlalchemy.dialects.mysql import insert

from dlstbx.cluster_monitor import PrometheusClusterMonitor


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

    def insert(
        self,
        metric,
        cluster_id,
        metric_type,
        metric_labels="",
        metric_value=0,
        timestamp=None,
        auto_proc_program_id=None,
        cluster_end_timestamp=None,
    ):
        insert_cmd = insert(PrometheusClusterMonitor).values(
            metric=metric,
            cluster_id=cluster_id,
            metric_type=metric_type,
            metric_labels=metric_labels,
            metric_value=metric_value,
            timestamp=timestamp,
            auto_proc_program_id=auto_proc_program_id,
            cluster_end_timestamp=cluster_end_timestamp,
        )
        update = insert_cmd.on_duplicate_key_update(
            cluster_id=cluster_id,
            metric_type=metric_type,
            metric_value=PrometheusClusterMonitor.metric_value + metric_value,
            timestamp=timestamp,
            auto_proc_program_id=auto_proc_program_id,
            cluster_end_timestamp=cluster_end_timestamp,
        )
        with self._sessionmaker() as session:
            session.execute(update)
            session.commit()
        return

    @property
    def text(self):
        as_text = ""
        with self._sessionmaker() as session:
            query = session.query(PrometheusClusterMonitor).all()
        metrics = {}
        for metric_line in query:
            if metric_line.metric not in metrics.keys():
                metrics[metric_line.metric] = []
            metrics[metric_line.metric].append(metric_line)

        for m, lines in metrics.items():
            as_text += self._type_line(lines[0].metric_type.name, m)
            for l in lines:
                as_text += self._metric_line(l, m)
            as_text += "\n"
        return as_text

    @staticmethod
    def _type_line(metric_type, metric_name):
        return f"# TYPE {metric_name} {metric_type}\n"

    @staticmethod
    def _metric_line(db_row, metric_name):
        if db_row.timestamp is not None:
            return f"{metric_name}{{{db_row.metric_labels}}} {db_row.metric_value} {db_row.timestamp}\n"
        return f"{metric_name}{{{db_row.metric_labels}}} {db_row.metric_value}\n"

    def prune(self):
        one_hour_ago = sqlalchemy.sql.expression.text("NOW() - INTERVAL 1 HOUR")
        with self._sessionmaker() as session:
            records_pruned = (
                session.query(PrometheusClusterMonitor)
                .filter(PrometheusClusterMonitor.cluster_end_timestamp < one_hour_ago)
                .delete(synchronize_session=False)
            )
            session.commit()
            return records_pruned
