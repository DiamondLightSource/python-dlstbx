# graylog csv to something that can be worked on in R

import csv
import time
import re
from datetime import datetime
from pprint import pprint

img = re.compile(" ([0-9]+)-([0-9]+)")

with open(
    "Desktop/graylog-search-result-absolute-2019-02-13T11_54_16.000Z-2019-02-13T12_00_57.000Z.csv",
) as fh:
    csvreader = csv.reader(fh)
    fields = next(csvreader)
    data = [
        {
            "time": datetime.strptime(l[0][:-1] + "000", "%Y-%m-%dT%H:%M:%S.%f"),
            "line": int(l[1]),
            "message": l[2],
            "job": int(l[3]),
        }
        for l in csvreader
    ]
    for l in data:
        l["time"] = time.mktime(l["time"].timetuple()) + l["time"].microsecond / 1000000
        s = img.search(l["message"])
        assert s, l["message"]
        l["dcid"], l["image"] = int(s.group(1)), int(s.group(2))

data = sorted(data, key=lambda x: x["time"])

times = {}
for l in data:
    if l["line"] == 100:
        assert l["image"] not in times
        times[l["image"]] = {"start": l["time"]}
    elif l["line"] == 131:
        assert l["image"] in times, l
        assert "time" not in times[l["image"]]
        times[l["image"]]["time"] = l["time"] - times[l["image"]]["start"]
    else:
        print(l)

pprint({k: times[k]["time"] for k in times})
