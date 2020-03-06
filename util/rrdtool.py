from __future__ import absolute_import, division, print_function

import logging
import os
import re

import procrunner

log = logging.getLogger("dlstbx.util.rrdtool")


def run_rrdtool(command):
    stdin = "\n".join(
        [". /etc/profile.d/modules.sh", "module load rrdtool", "rrdtool " + command]
    )
    result = procrunner.run(
        ["/bin/bash"],
        stdin=stdin.encode("latin-1"),
        environment_override={"LD_LIBRARY_PATH": ""},
        print_stdout=False,
    )
    if result["exitcode"] or result["stderr"]:
        log.warning(
            "Command rrdtool %s resulted in exitcode %d with error:\n%s",
            command,
            result["exitcode"],
            result["stderr"],
        )
    else:
        log.debug("Successfully ran %s", command)
    return result


class RRDFile(object):
    def __init__(self, filename):
        self.filename = filename
        self.last_update = self._read_last_update()
        if not self.last_update:
            raise IOError("Could not read rrd file %s" % filename)

    def _read_last_update(self):
        command = ["info", self.filename]
        result = run_rrdtool(" ".join(command))
        if result and "stdout" in result:
            last_update = re.search("last_update = ([0-9]+)", result["stdout"])
            if last_update:
                return int(last_update.group(1))
        return False

    def update(self, data, options=None):
        command = ["update", self.filename]
        if options:
            command.extend(options)
        ordered_data = sorted((entry[0], entry) for entry in data)
        last_update = self.last_update
        for timestamp, entry in ordered_data:
            if timestamp > last_update:
                command.append(":".join(str(num) for num in entry))
                last_update = timestamp
        if last_update == self.last_update:  # No relevant update
            return True
        success = run_rrdtool(" ".join(command))["exitcode"] == 0
        if success:
            self.last_update = last_update
        return success


class RRDTool(object):
    """A wrapper around an rrdtool executable that does not rely on compiling
     rrdtool first."""

    def __init__(self, basepath):
        """Create a wrapper instance. Pass a path in which the .rrd files will be
       stored in and the name of the rrdtool executable."""
        self.basepath = basepath
        if not os.path.isdir(basepath):
            raise IOError("rrdtool base directory %s does not exist" % basepath)

    def create(self, filename, options, start=1000000000):
        rrdfile = os.path.join(self.basepath, filename)
        if os.path.exists(rrdfile):
            return RRDFile(rrdfile)
        command = ["create", os.path.join(self.basepath, filename)]
        if start:
            command.extend(["--start", str(start)])
        command.extend(options)
        if run_rrdtool(" ".join(command))["exitcode"] == 0:
            return RRDFile(rrdfile)
        return False
