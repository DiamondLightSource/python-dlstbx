#
# simple API to access Graylog messages
#
# Point to a configuration file to use it, eg:
#  g = GraylogAPI('/dls_sw/apps/zocalo/secrets/credentials-log.cfg')
from __future__ import annotations

import base64
import configparser
import datetime
import http.client
import json
import urllib.request

import dateutil.parser
import dateutil.tz
import pytz

local_timezone = dateutil.tz.gettz("Europe/London")


class GraylogAPI:
    last_seen_message = None
    last_seen_timestamp = None

    def __init__(self, configfile):
        cfgparser = configparser.ConfigParser(allow_no_value=True)
        self.level = 6  # INFO
        self.filters = []
        if not cfgparser.read(configfile):
            raise RuntimeError("Could not read from configuration file %s" % configfile)
        self.url = cfgparser.get("graylog", "url")
        if not self.url.endswith("/"):
            self.url += "/"
        self.authstring = b"Basic " + base64.b64encode(
            cfgparser.get("graylog", "username").encode("utf-8")
            + b":"
            + cfgparser.get("graylog", "password").encode("utf-8")
        )
        self.stream = cfgparser.get("graylog", "stream")

    def _get(self, url):
        complete_url = self.url + url
        req = urllib.request.Request(
            complete_url, headers={"Accept": "application/json"}
        )
        req.add_header("Authorization", self.authstring)
        handler = urllib.request.urlopen(req)

        returncode = handler.getcode()
        success = returncode == 200
        headers = {k: v for k, v in handler.headers.items()}
        while True:
            body = b""
            try:
                body += handler.read()
            except http.client.IncompleteRead as icread:
                body += icread.partial
                continue
            else:
                break
        if success:
            parsed = json.loads(body)
        else:
            parsed = None

        return {
            "success": success,
            "returncode": returncode,
            "headers": headers,
            "body": body,
            "parsed": parsed,
        }

    def cluster_info(self):
        cluster = self._get("cluster")
        if not cluster["success"]:
            return False
        return cluster["parsed"]

    def unprocessed_messages(self, node):
        backlog = self._get(
            f"cluster/{node}/metrics/namespace/org.graylog2.journal.entries-uncommitted"
        )
        if not backlog["success"]:
            return False
        try:
            return backlog["parsed"]["metrics"][0]["metric"]["value"]
        except Exception:
            return False

    @staticmethod
    def epoch_to_graylog(timestamp):
        return datetime.datetime.fromtimestamp(timestamp).isoformat().replace("T", " ")

    def get_messages(self, time=600, query=None):
        if self.last_seen_timestamp:
            update = self.absolute_update(query=query)
        else:
            update = self.relative_update(time=time, query=query)
            self.last_seen_timestamp = update["parsed"]["to"]
        if not update["success"]:
            return
        update = update["parsed"]
        messages = [x.get("message", {}) for x in update.get("messages", [])]
        if self.last_seen_message:
            message_ids = [m["_id"] for m in messages]
            try:
                seen_marker = message_ids.index(self.last_seen_message)
                messages = messages[
                    seen_marker + 1 :
                ]  # skip previously seen message and all preceeding
            except ValueError:
                pass  # last seen message not in selection
        if messages:
            self.last_seen_message = messages[-1]["_id"]
            self.last_seen_timestamp = messages[-1]["timestamp"]
        for m in messages:
            m["localtime"] = dateutil.parser.parse(m["timestamp"]).astimezone(
                local_timezone
            )
            m["timestamp_msec"] = m["timestamp"][-4:-1]
            # alternatively: m['localtime'].strftime('%f')[:-3]
        return messages

    def get_all_messages(self, **kwargs):
        messages = True
        while messages:
            messages = self.get_messages(**kwargs)
            yield from messages

    def absolute_histogram(self, from_time=None, level=None, level_op="%3C="):
        if not from_time:
            from_time = self.last_seen_timestamp
        from_time = from_time.replace(":", "%3A").replace(" ", "%20")
        if not level:
            level = self.level
        return self._get(
            "search/universal/absolute/histogram?"
            "query=level:{level_op}{level}&"
            "interval=minute&"
            "from={from_time}&"
            "to=2031-01-01%2012%3A00%3A00&"
            "filter=streams%3A{stream}".format(
                from_time=from_time, stream=self.stream, level=level, level_op=level_op
            )
        )

    def relative_update(self, time=600, query=None):
        if not query:
            query = "level:<={level}"
        if self.filters:
            query = "({query}) AND ({filters})".format(
                query=query,
                filters=" AND ".join(f"({f})" for f in self.filters),
            )
        return self._get(
            "search/universal/relative?"
            "query={query}&"
            "range={time}&"
            "filter=streams%3A{stream}&"
            "sort=timestamp%3Aasc".format(
                time=time,
                stream=self.stream,
                query=urllib.parse.quote(query.format(level=self.level)),
            )
        )

    def absolute_update(self, from_time=None, query=None):
        if not query:
            query = "level:<={level}"
        if self.filters:
            query = "({query}) AND ({filters})".format(
                query=query,
                filters=" AND ".join(f"({f})" for f in self.filters),
            )
        if not from_time:
            from_time = self.last_seen_timestamp
        from_time = from_time.replace(":", "%3A")
        return self._get(
            "search/universal/absolute?"
            "query={query}&"
            "from={from_time}&"
            "to=2031-01-01%2012%3A00%3A00&"
            "filter=streams%3A{stream}&"
            "sort=timestamp%3Aasc".format(
                from_time=from_time,
                stream=self.stream,
                query=urllib.parse.quote(query.format(level=self.level)),
            )
        )

    def get_history_statistics(self):
        first_message = self._get(
            "search/universal/absolute?"
            "query=%2A&"
            "from=1970-01-01%2000%3A00%3A00&"
            "to=2031-01-01%2012%3A00%3A00&"
            "filter=streams%3A{stream}&"
            "limit=1&"
            "sort=timestamp%3Aasc".format(stream=self.stream)
        )
        if not first_message["success"]:
            return False
        first_timestamp = first_message["parsed"]["messages"][0]["message"]["timestamp"]
        log_range = (
            pytz.utc.localize(datetime.datetime.utcnow())
            - dateutil.parser.parse(first_timestamp)
        ).total_seconds()
        log_range_days = log_range / 24 / 3600
        log_range_weeks = log_range / 24 / 3600 / 7
        stream_message_count = first_message["parsed"]["total_results"]
        return {
            "range": {
                "seconds": log_range,
                "days": log_range_days,
                "weeks": log_range_weeks,
            },
            "message_count": stream_message_count,
        }

    def gather_log_levels_histogram_since(self, from_timestamp):
        ts = self.epoch_to_graylog(from_timestamp)
        global_histdata = {}
        for level in (7, 6, 5, 4, 3, 2):
            hist = self.absolute_histogram(
                from_time=ts, level=level, level_op="" if level > 2 else "%3C="
            )
            assert hist["parsed"], "Could not read histogram for level %d" % level
            for k, v in hist["parsed"]["results"].items():
                k = int(k)
                if k not in global_histdata:
                    global_histdata[k] = {}
                global_histdata[k][level] = v
        return global_histdata
