import errno
import json
import dataclasses
import os
import threading
import time

import numpy as np

import dlstbx.util.symlink
import dlstbx.util.xray_centering
import workflows.recipe
from workflows.services.common_service import CommonService


@dataclasses.dataclass
class CenteringData:
    steps_x: int
    steps_y: int
    headers: list
    recipewrapper: dict
    last_activity: float = dataclasses.field(default_factory=time.time)

    def __post_init__(self):
        self.data = np.empty(self.image_count, dtype=int)

    @property
    def image_count(self):
        return int(self.steps_x * self.steps_y)

    @property
    def images_seen(self):
        return len(self.headers)


class DLSXRayCentering(CommonService):
    """A service to aggregate per-image-analysis results and identify an X-ray
    centering solution for a data collection."""

    _service_name = "DLS X-Ray Centering"
    _logger_name = "dlstbx.services.xray-centering"

    def initializing(self):
        """Try to exclusively subscribe to the x-ray centering queue. Received messages must be acknowledged.
        Exclusive subscription enables a single process to do the 'reduce' step, aggregating many messages
        that belong together.
        """
        self.log.info("X-Ray centering service starting up")

        self._centering_data = {}
        self._centering_lock = threading.Lock()

        self._next_garbage_collection = time.time() + 60
        self._register_idle(60, self.garbage_collect)
        workflows.recipe.wrap_subscribe(
            self._transport,
            "reduce.xray_centering",
            self.add_pia_result,
            acknowledgement=True,
            exclusive=True,
            log_extender=self.extend_log,
        )

    def garbage_collect(self):
        """Throw away partial scan results after a while."""
        self._next_garbage_collection = time.time() + 60
        with self._centering_lock:
            for dcid in list(self._centering_data):
                age = time.time() - self._centering_data[dcid].last_activity
                if age > 15 * 60:
                    self.log.info("Expiring X-Ray Centering session for DCID %r", dcid)
                    rw = self._centering_data[dcid]["recipewrapper"]
                    txn = rw.transport.transaction_begin()
                    for header in self._centering_data[dcid]["headers"]:
                        rw.transport.ack(header, transaction=txn)
                    rw.send_to("abort", {}, transaction=txn)
                    rw.transport.transaction_commit(txn)
                    del self._centering_data[dcid]

    def add_pia_result(self, rw, header, message):
        """Process incoming PIA result."""

        parameters = rw.recipe_step.get("parameters")
        if not parameters or not parameters.get("dcid"):
            self.log.error("X-ray centering service called without recipe parameters")
            rw.transport.nack(header)
            return
        gridinfo = rw.recipe_step.get("gridinfo")
        if not gridinfo or not isinstance(gridinfo, dict):
            if (
                rw.recipe_step.get("comment")
                and "Diffraction grid scan of 1 by 1 images"
                in rw.recipe_step["comment"]
            ):
                self.log.info(
                    "X-ray centering service received 1x1 grid scan without information"
                )
                ### https://jira.diamond.ac.uk/browse/I04_1-320
                rw.transport.ack(header)
            else:
                self.log.error(
                    "X-ray centering service called without grid information"
                )
                rw.transport.nack(header)
            return
        dcid = int(parameters["dcid"])

        if (
            not message
            or not message.get("file-number")
            or message.get("n_spots_total") is None
        ):
            self.log.error("X-ray centering service called without valid payload")
            rw.transport.nack(header)
            return
        file_number = message["file-number"]
        spots_count = message["n_spots_total"]

        with self._centering_lock:
            if dcid in self._centering_data:
                cd = self._centering_data[dcid]
            else:
                cd = CenteringData(
                    steps_x=int(gridinfo["steps_x"]),
                    steps_y=int(gridinfo["steps_y"]),
                    headers=[],
                    recipewrapper=rw,
                )
                self._centering_data[dcid] = cd
                self.log.info(
                    f"First record arrived for X-ray centering on DCID {dcid}, "
                    "{cd.steps_x} x {cd.steps_y} grid, {cd.image_count} images in total"
                )

            cd.last_activity = time.time()
            cd.headers.append(header)
            self.log.debug(
                "Received PIA result for DCID %d image %d, %d of %d expected results",
                dcid,
                file_number,
                cd.images_seen,
                cd.image_count,
            )
            cd.data[file_number - 1] = spots_count

            if cd.images_seen == cd.image_count:
                self.log.info(
                    "All records arrived for X-ray centering on DCID %d", dcid
                )
                result, output = dlstbx.util.xray_centering.main(
                    cd.data,
                    steps=(cd.steps_x, cd.steps_y),
                    box_size_px=(
                        1000 * gridinfo["dx_mm"] / gridinfo["pixelsPerMicronX"],
                        1000 * gridinfo["dy_mm"] / gridinfo["pixelsPerMicronY"],
                    ),
                    snapshot_offset=(
                        float(gridinfo.get("snapshot_offsetXPixel")),
                        float(gridinfo.get("snapshot_offsetYPixel")),
                    ),
                    snaked=bool(gridinfo.get("snaked")),
                    orientation=dlstbx.util.xray_centering.Orientation[
                        gridinfo["orientation"].upper()
                    ],
                )
                self.log.debug(output)

                # Write result file
                if parameters.get("output"):
                    self.log.info(
                        "Writing X-Ray centering results for DCID %d to %s",
                        dcid,
                        parameters["output"],
                    )
                    path = os.path.dirname(parameters["output"])
                    try:
                        os.makedirs(path)
                    except OSError as exc:
                        if exc.errno == errno.EEXIST and os.path.isdir(path):
                            pass
                        else:
                            raise
                    with open(parameters["output"], "w") as fh:
                        d = dataclasses.asdict(result)
                        self.log.debug(d)
                        json.dump(d, fh, sort_keys=True)
                    if parameters.get("results_symlink"):
                        # Create symbolic link above working directory
                        dlstbx.util.symlink.create_parent_symlink(
                            path, parameters["results_symlink"]
                        )

                # Write human-readable result file
                if parameters.get("log"):
                    path = os.path.dirname(parameters["log"])
                    try:
                        os.makedirs(path)
                    except OSError as exc:
                        if exc.errno == errno.EEXIST and os.path.isdir(path):
                            pass
                        else:
                            raise
                    with open(parameters["log"], "w") as fh:
                        fh.write(output)

                # Acknowledge all messages
                txn = rw.transport.transaction_begin()
                for h in cd.headers:
                    rw.transport.ack(h, transaction=txn)

                # Send results onwards
                rw.set_default_channel("success")
                rw.send_to("success", result, transaction=txn)
                rw.transport.transaction_commit(txn)

                del self._centering_data[dcid]

        if self._next_garbage_collection < time.time():
            self.garbage_collect()
