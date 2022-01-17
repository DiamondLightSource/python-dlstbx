### Deprecated. Remove after DIALS 3.7 is released and deployed

from __future__ import annotations

import json
import logging
import pathlib

import mysql.connector

# API for access to the zocalo profiling database, which includes DLS
# infrastructure status information.


class database:
    def __init__(self):
        try:
            _configuration = pathlib.Path(
                "/dls_sw/apps/zocalo/secrets/sql-zocalo-profiling.json"
            ).read_text()
        except PermissionError:
            _configuration = pathlib.Path(
                "/dls_sw/apps/zocalo/secrets/sql-zocalo-readonly.json"
            ).read_text()
        _secret_ingredients = json.loads(_configuration)

        self.conn = mysql.connector.connect(
            host=_secret_ingredients["host"],
            port=_secret_ingredients["port"],
            user=_secret_ingredients["user"],
            password=_secret_ingredients["passwd"],
            database=_secret_ingredients["db"],
            use_pure=True,
        )
        self._cursor = self.conn.cursor(dictionary=True)

    def __del__(self):
        if hasattr(self, "conn") and self.conn:
            self.conn.close()

    def cursor(self):
        return self._cursor

    def _execute(self, query, parameters=None):
        cursor = self.cursor()
        if parameters:
            if isinstance(parameters, (str, int)):
                parameters = (parameters,)
            cursor.execute(query, parameters)
        else:
            cursor.execute(query)
        results = [result for result in cursor]
        return results

    def commit(self):
        self.conn.commit()

    def get_infrastructure_status(self):
        status = self._execute("SELECT * FROM infrastructure_status;")
        for s in status:
            if s["Level"] < 10:
                s["Group"] = "Information"
            elif s["Level"] < 20:
                s["Group"] = "Warning"
            else:
                s["Group"] = "Error"
        return status

    def set_infrastructure_status(
        self,
        source=None,
        level=None,
        message=None,
        fullmessage=None,
        url=None,
        ext=None,
    ):
        if ext:
            ext = json.dumps(ext)
        assert source, "Source of status message undefined"
        assert level is not None, "Warning level of status message undefined"
        assert message, "Message undefined"
        self.cursor().execute(
            "REPLACE INTO infrastructure_status (Source, Level, Message, MessageBody, URL, ExtData) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (source, level, message, fullmessage, url, ext),
        )
        self.commit()
        statlog = logging.getLogger("ithealth." + source)
        statlog.setLevel(logging.DEBUG)
        logdest = statlog.debug
        if level > 9:
            logdest = statlog.warning
        if level > 19:
            logdest = statlog.error
        logdest(message, extra={"fullmessage": fullmessage})

    def prune(self):
        self._execute(
            "DELETE FROM infrastructure_status WHERE (TO_SECONDS(NOW()) - TO_SECONDS(Timestamp)) > 24 * 3600;"
        )
        self.commit()
