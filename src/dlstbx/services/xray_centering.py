<<<<<<< HEAD
import dataclasses
import errno
import json
import os
=======
import json
import dataclasses
import pathlib
>>>>>>> 54091e97 (Use pydantic to validate input parameters)
import threading
import time

import numpy as np
<<<<<<< HEAD
import workflows.recipe
from workflows.services.common_service import CommonService
=======
import pydantic
>>>>>>> 54091e97 (Use pydantic to validate input parameters)

import dlstbx.util.symlink
import dlstbx.util.xray_centering


class GridInfo(pydantic.BaseModel):
    steps_x: int
    steps_y: int
    dx_mm: float
    dy_mm: float
    pixelsPerMicronX: float
    pixelsPerMicronY: float
    snapshot_offsetXPixel: float
    snapshot_offsetYPixel: float
    snaked: bool
    orientation: dlstbx.util.xray_centering.Orientation

    @property
    def image_count(self) -> int:
        return self.steps_x * self.steps_y


class Parameters(pydantic.BaseModel):
    dcid: int
    output: pathlib.Path = None
    log: pathlib.Path = None
    results_symlink: pathlib.Path = None


class CenteringData(pydantic.BaseModel):
    image_count: int
    recipewrapper: workflows.recipe.wrapper.RecipeWrapper
    headers: list = pydantic.Field(default_factory=list)
    last_activity: float = pydantic.Field(default_factory=time.time)
    data: np.ndarray = None

    def __init__(self, **data):
        super().__init__(**data)
        self.data = np.empty(self.image_count, dtype=int)

    @property
    def images_seen(self):
        return len(self.headers)

    class Config:
        arbitrary_types_allowed = True


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
                    rw = self._centering_data[dcid].recipewrapper
                    txn = rw.transport.transaction_begin()
                    for header in self._centering_data[dcid].headers:
                        rw.transport.ack(header, transaction=txn)
                    rw.send_to("abort", {}, transaction=txn)
                    rw.transport.transaction_commit(txn)
                    del self._centering_data[dcid]

    def add_pia_result(self, rw, header, message):
        """Process incoming PIA result."""

        try:
            parameters = Parameters(**rw.recipe_step.get("parameters", {}))
            gridinfo = GridInfo(**rw.recipe_step.get("gridinfo", {}))
        except pydantic.ValidationError as e:
            self.log.error(
                "X-ray centering service called with invalid parameters: %s", e
            )
            rw.transport.nack(header)
            return
        if not gridinfo:
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
        dcid = parameters.dcid

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
                    image_count=gridinfo.image_count,
                    recipewrapper=rw,
                )
                self._centering_data[dcid] = cd
                self.log.info(
                    f"First record arrived for X-ray centering on DCID {dcid}, "
                    f"{gridinfo.steps_x} x {gridinfo.steps_y} grid, {gridinfo.image_count} images in total"
                )

            cd.last_activity = time.time()
            cd.headers.append(header)
            self.log.debug(
                "Received PIA result for DCID %d image %d, %d of %d expected results",
                dcid,
                file_number,
                cd.images_seen,
                gridinfo.image_count,
            )
            cd.data[file_number - 1] = spots_count

            if cd.images_seen == gridinfo.image_count:
                self.log.info(
                    "All records arrived for X-ray centering on DCID %d", dcid
                )
                result, output = dlstbx.util.xray_centering.main(
                    cd.data,
                    steps=(gridinfo.steps_x, gridinfo.steps_y),
                    box_size_px=(
                        1000 * gridinfo.dx_mm / gridinfo.pixelsPerMicronX,
                        1000 * gridinfo.dy_mm / gridinfo.pixelsPerMicronY,
                    ),
                    snapshot_offset=(
                        gridinfo.snapshot_offsetXPixel,
                        gridinfo.snapshot_offsetYPixel,
                    ),
                    snaked=gridinfo.snaked,
                    orientation=gridinfo.orientation,
                )
                self.log.debug(output)

                # Write result file
                if parameters.output:
                    self.log.info(
                        "Writing X-Ray centering results for DCID %d to %s",
                        dcid,
                        parameters.output,
                    )
                    parameters.output.parent.mkdir(parents=True, exist_ok=True)
                    with parameters.output.open("w") as fh:

                        def convert(o):
                            if isinstance(o, np.integer):
                                return int(o)
                            raise TypeError

                        json.dump(
                            dataclasses.asdict(result),
                            fh,
                            sort_keys=True,
                            default=convert,
                        )
                    if parameters.results_symlink:
                        # Create symbolic link above working directory
                        dlstbx.util.symlink.create_parent_symlink(
                            parameters.output.parent, parameters.results_symlink
                        )

                # Write human-readable result file
                if parameters.log:
                    parameters.log.parent.mkdir(parents=True, exist_ok=True)
                    parameters.log.write_text(output)

                # Acknowledge all messages
                txn = rw.transport.transaction_begin()
                for h in cd.headers:
                    rw.transport.ack(h, transaction=txn)

                # Send results onwards
                rw.set_default_channel("success")
                rw.send_to("success", dataclasses.asdict(result), transaction=txn)
                rw.transport.transaction_commit(txn)

                del self._centering_data[dcid]

        if self._next_garbage_collection < time.time():
            self.garbage_collect()
