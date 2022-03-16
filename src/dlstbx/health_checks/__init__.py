from __future__ import annotations

import functools
import json
import logging
import pathlib
import types
from datetime import datetime
from typing import Dict, List, NamedTuple

import junit_xml
import sqlalchemy.ext.declarative
import sqlalchemy.orm
import sqlalchemy.sql.expression
from sqlalchemy import TIMESTAMP, Column, LargeBinary, String, Text, select, text
from sqlalchemy.dialects.mysql import TINYINT

# deliberately not declared as an IntEnum so that it can be passed to the sqlalchemy Status() class
REPORT = types.SimpleNamespace(PASS=0, NOTICE=1, WARNING=10, ERROR=20)

_Base = sqlalchemy.ext.declarative.declarative_base()


class CheckFunctionInterface(NamedTuple):
    current_status: Dict[str, Status]
    name: str


class Status(_Base):  # type: ignore
    __tablename__ = "infrastructure_status"
    __table_args__ = {"comment": "reports of DLS infrastructure"}

    Source = Column(
        String(250),
        primary_key=True,
        comment="A unique identifier of the message source, use dots to separate components",
    )
    Level = Column(
        TINYINT(3),
        nullable=False,
        index=True,
        comment="Message level, higher number = worse. 0-9 OK, 10-19 WARN, 20+ FAIL",
    )
    Message = Column(String(140), nullable=False, comment="Tweet-length status message")
    MessageBody = Column(Text, comment="Full status message")
    URL = Column(String(250), comment="Optional URL for further detail")
    Timestamp = Column(
        TIMESTAMP,
        nullable=False,
        index=True,
        server_default=text("current_timestamp() ON UPDATE current_timestamp()"),
        comment="Last status update",
    )
    ExtData = Column(LargeBinary)

    def __repr__(self):
        if self.Timestamp:
            return f"<Status L{self.Level:03d} {self.Source}: {self.Message} ({self.Timestamp})>"
        else:
            return f"<Status L{self.Level:03d} {self.Source}: {self.Message}>"

    @property
    def age(self):
        if self.Timestamp:
            return (datetime.now() - self.Timestamp).seconds
        return 0

    @property
    def age_str(self):
        age = self.age
        if age < 30:
            return "just now"
        elif age < 90:
            return f"{age} sec ago"
        elif age < 90 * 60:
            return f"{round(age / 60)} min ago"
        else:
            return f"{age / 60 / 60:.1f} hrs ago"

    def as_testcase(self, include_timestamp=False):
        s_class = ".".join(self.Source.split(".")[:-1]) or "zocalo"
        if "." not in s_class:
            s_class = f"zocalo.{s_class}"
        s_name = self.Source.split(".")[-1]
        s_message = (self.Message or "").strip()
        s_body = (self.MessageBody or "").rstrip()
        if include_timestamp:
            if s_body:
                s_body += "\n\n"
            s_body += f"Test run at {self.Timestamp}"
        if self.Level >= 10:
            tc = junit_xml.TestCase(
                classname=s_class,
                name=s_name,
            )
            if self.Level >= 20:
                tc.add_failure_info(s_message, s_body)
            else:
                tc.add_skipped_info(s_message, s_body)
        else:
            tc = junit_xml.TestCase(
                classname=s_class,
                name=s_name,
                stdout=f"{s_message}\n\n{s_body}" if s_body else s_message,
            )
        return tc


def limit_level(*, max_increase=REPORT.WARNING):
    """
    A decorator that affects the reported error levels of health checks.

    max_increase: limits the maximum level increase compared to the last run
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(cfc: CheckFunctionInterface):
            def _limit(status):
                current_status = cfc.current_status.get(status.Source)
                if not current_status:
                    return status  # unknown previous status
                if status.Level <= current_status.Level:
                    return status  # things stayed the same or have improved
                # smooth out transient errors by limiting how fast errors are escalated
                status.Level = min(current_status.Level + max_increase, status.Level)
                return status

            results = func(cfc)
            if not results:
                return results
            if isinstance(results, Status):
                return _limit(results)
            return [_limit(r) for r in results]

        return wrapper

    return decorator


class database:
    """API for access to the zocalo health status database (nee profiling database)"""

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

    def get_status(self) -> List[Status]:
        with self._sessionmaker() as session:
            status = session.execute(select(Status)).scalars().all()
        for s in status:
            if s.Level < 10:
                s.Group = "Information"
            elif s.Level < 20:
                s.Group = "Warning"
            else:
                s.Group = "Error"
        return status

    def set_status(
        self,
        status: Status,
    ) -> None:
        if not status.Source:
            raise ValueError("Source of status message undefined")
        if status.Level is None:
            raise ValueError("Warning level of status message undefined")
        if not status.Message:
            raise ValueError("Message undefined")
        if not status.Timestamp:
            status.Timestamp = datetime.now()

        with self._sessionmaker() as session:
            session.merge(status)
            session.commit()

        statlog = logging.getLogger(f"ithealth.{status.Source}")
        logdest = statlog.debug
        if status.Level > 9:
            logdest = statlog.warning
        if status.Level > 19:
            logdest = statlog.error
        logdest(status.Message, extra={"fullmessage": status.MessageBody})

    def set_status_components(
        self,
        source: str = None,
        level: int = None,
        message: str = None,
        *,
        fullmessage=None,
        url=None,
        ext=None,
    ) -> None:
        if ext:
            ext = json.dumps(ext)
        self.set_status(
            Status(
                Source=source,
                Level=level,
                Message=message,
                MessageBody=fullmessage,
                URL=url,
                ExtData=ext,
            )
        )

    def prune(self):
        one_day_ago = sqlalchemy.sql.expression.text("NOW() - INTERVAL 1 DAY")
        with self._sessionmaker() as session:
            session.query(Status).filter(Status.Timestamp < one_day_ago).delete(
                synchronize_session=False
            )
            session.commit()
