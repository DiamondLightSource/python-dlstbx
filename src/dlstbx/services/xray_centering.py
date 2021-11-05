import dataclasses
import json
import pathlib
import threading
import time
from typing import List

import numpy as np
import pydantic
import workflows.recipe
from workflows.services.common_service import CommonService

import dlstbx.util.symlink
import dlstbx.util.xray_centering
import dlstbx.util.xray_centering_3d

from prometheus_client import Counter, Gauge, Histogram, start_http_server


class GridInfo(pydantic.BaseModel):
    "The subset of GridInfo fields required by the X-ray centering service"

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
    "Recipe parameters used by the X-ray centering service"

    dcid: int
    experiment_type: str
    dcg_dcids: List[int] = None
    output: pathlib.Path = None
    log: pathlib.Path = None
    results_symlink: str = None


class RecipeStep(pydantic.BaseModel):
    parameters: Parameters
    gridinfo: GridInfo


class Message(pydantic.BaseModel):
    file_number: pydantic.PositiveInt = pydantic.Field(alias="file-number")
    n_spots_total: pydantic.NonNegativeInt

    file_detected_timestamp: pydantic.NonNegativeFloat = pydantic.Field(alias="file-detected-timestamp")
    file: str
    #beam_line: str = pydantic.Field(["file"].split("/dls/")[1].split("/data/"[0]))


class CenteringData(pydantic.BaseModel):
    gridinfo: GridInfo
    recipewrapper: workflows.recipe.wrapper.RecipeWrapper
    headers: list = pydantic.Field(default_factory=list)
    last_activity: float = pydantic.Field(default_factory=time.time)
    data: np.ndarray = None

    def __init__(self, **data):
        super().__init__(**data)
        self.data = np.empty(self.gridinfo.image_count, dtype=int)

    @property
    def images_seen(self):
        return len(self.headers)

    class Config:
        arbitrary_types_allowed = True


class prometheus_metrics():

    def __init__(self, metrics_on):
        self._metrics_on = True

    def open_endpoint(port, address=""):
        try:
            start_http_server(port, address)
        except:
            """log its failure"""

    def create_metrics(self):
        self.complete_centering = Counter(
        "complete_centerings",
        "Counts total number of completed x-ray centerings",
        ["beam_line"]
        )

        self.analysis_latency = Histogram(
        "analysis_latency",
        "The time passed (s) from end of data collection to end of x-ray centering",
        ["beam_line"],
        buckets = [ 10, 20, 30, 40, 50, 60, 70, 80, 90, 100],
        unit= "s"
        )

    def set_metrics(self, bl, lat):
        self.complete_centering.labels(bl).inc()
        self.analysis_latency.labels(bl).observe(lat)


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

        self._prom_metrics = prometheus_metrics(True)
        if self._prom_metrics:
            prometheus_metrics.open_endpoint(8000,"localhost")
            self._prom_metrics.create_metrics()


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
            recipe_step = RecipeStep(**rw.recipe_step)
            parameters = recipe_step.parameters
            gridinfo = recipe_step.gridinfo
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
        dcg_dcids = parameters.dcg_dcids
        try:
            message = Message(**message)
        except pydantic.ValidationError as e:
            self.log.error("X-ray centering service called with invalid payload: %s", e)
            rw.transport.nack(header)
            return

        with self._centering_lock:
            if dcid in self._centering_data:
                cd = self._centering_data[dcid]
            else:
                cd = CenteringData(
                    gridinfo=gridinfo,
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
                message.file_number,
                cd.images_seen,
                gridinfo.image_count,
            )
            cd.data[message.file_number - 1] = message.n_spots_total

            # make note of timestamp of last file read in for latency metric
            last_file_read_at = 0.0
            if message.file_detected_timestamp > last_file_read_at:
                last_file_read_at = message.file_detected_timestamp
            # does this check everyone?


            if dcg_dcids and cd.images_seen == gridinfo.image_count:
                data = [cd.data]
                for _dcid in dcg_dcids:
                    _cd = self._centering_data.get(_dcid)
                    if not _cd:
                        break
                    if _cd.images_seen != _cd.gridinfo.image_count:
                        break
                    data.append(_cd.data)
                else:
                    # All results present
                    self.log.info(
                        f"All records arrived for X-ray centering on DCIDs {dcg_dcids + [dcid]}"
                    )

                    result = dlstbx.util.xray_centering_3d.gridscan3d(
                        data=np.array(data),
                        steps=(gridinfo.steps_x, gridinfo.steps_y),
                        snaked=gridinfo.snaked,
                        orientation=gridinfo.orientation,
                        plot=False,
                    )
                    self.log.info(f"3D X-ray centering result: {result}")

                    # Acknowledge all messages
                    txn = rw.transport.transaction_begin()
                    for _dcid in dcg_dcids + [dcid]:
                        cd = self._centering_data[_dcid]
                        for h in cd.headers:
                            rw.transport.ack(h, transaction=txn)

                    # Send results onwards
                    rw.set_default_channel("success")
                    rw.send_to("success", result, transaction=txn)
                    rw.transport.transaction_commit(txn)

                    for _dcid in dcg_dcids + [dcid]:
                        del self._centering_data[_dcid]

            elif (
                parameters.experiment_type != "Mesh3D"
                and cd.images_seen == gridinfo.image_count
            ):
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
                # ---

                # latency calculation & metrics
                r_latency = time.time() - last_file_read_at
                beam_line = message.file.split("/dls/")[1].split("/data/")[0]

                self._prom_metrics.set_metrics(beam_line,r_latency)

                # ---
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
                            str(parameters.output.parent), parameters.results_symlink
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
