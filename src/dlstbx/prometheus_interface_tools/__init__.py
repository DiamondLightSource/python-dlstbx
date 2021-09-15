import enum
import json
import pathlib

import sqlalchemy.ext.declarative
from sqlalchemy import TIMESTAMP, Column, String, text
from sqlalchemy.dialects.mysql import FLOAT, INTEGER
from sqlalchemy.types import Enum

_Base = sqlalchemy.ext.declarative.declarative_base()


class TypeEnum(enum.Enum):
    counter = 1
    gauge = 2
    histogram = 3
    summary = 4


class PrometheusInterface(_Base):
    __tablename__ = "PrometheusInterface"

    metric = Column(
        String(250),
        nullable=False,
        comment="Name of Prometheus metric",
        primary_key=True,
    )
    metric_labels = Column(
        String(512),
        default="",
        comment="Labels to be applied to the metric. String of comma separated key=value pairs",
        primary_key=True,
    )
    metric_value = Column(
        FLOAT(precision=10),
        comment="Value of Prometheus metric with given labels",
        nullable=False,
    )
    metric_type = Column(
        Enum(TypeEnum),
        comment="Type of Prometheus metric",
        nullable=False,
    )
    timestamp = Column(
        TIMESTAMP,
        server_default=text("current_timestamp() ON UPDATE current_timestamp()"),
        comment="Time of last metric update",
    )
    cluster_end_timestamp = Column(
        TIMESTAMP,
        comment="Time that cluster job finished. Used for pruning",
    )


class ClusterJobInfo(_Base):
    __tablename__ = "ClusterJobInfo"

    cluster = Column(
        String(250),
        nullable=False,
        comment="Name of cluster",
        primary_key=True,
    )
    cluster_id = Column(
        INTEGER(10),
        primary_key=True,
        comment="ID of the cluster job",
        autoincrement=False,
        nullable=False,
    )
    auto_proc_program_id = Column(
        INTEGER(10),
        comment="Reference to the AutoProcProgram the cluster job is attached to",
        autoincrement=False,
    )
    start_time = Column(
        TIMESTAMP,
        comment="Start time of cluster job",
    )
    end_time = Column(
        TIMESTAMP,
        comment="End time of cluster job",
    )


def get_sessionmaker():
    try:
        configuration = pathlib.Path(
            "/dls_sw/apps/zocalo/secrets/sql-zocalo-profiling.json"
        ).read_text()
    except PermissionError:
        configuration = pathlib.Path(
            "/dls_sw/apps/zocalo/secrets/sql-zocalo-readonly.json"
        ).read_text()
    secret_ingredients = json.loads(configuration)
    sqlalchemy_url = "mysql+mysqlconnector://{user}:{passwd}@{host}:{port}/{db}".format(
        **secret_ingredients
    )
    _sessionmaker = sqlalchemy.orm.sessionmaker(
        bind=sqlalchemy.create_engine(sqlalchemy_url, connect_args={"use_pure": True})
    )
    return _sessionmaker
