from __future__ import annotations

from typing import Annotated

import pydantic
from workflows.recipe.typed import NonEmpty, typed_handler
from workflows.services.common_service import CommonService

import dlstbx.util.gda

# Values forwarded to GDA keep their type, so that eg. integers are not
# reformatted as floats
Number = int | float


class GDAParameters(pydantic.BaseModel):
    dcid: pydantic.PositiveInt
    host: Annotated[str, NonEmpty]
    port: pydantic.PositiveInt

    @pydantic.field_validator("host")
    @classmethod
    def check_hostname(cls, host: str) -> str:
        if "{" in host:
            raise ValueError(f"{host} is not a valid hostname")
        return host


class PerImageAnalysisResult(pydantic.BaseModel):
    file_pattern_index: int | None = pydantic.Field(None, alias="file-pattern-index")
    file_number: int | None = pydantic.Field(None, alias="file-number")
    n_spots_total: Number = 0
    n_spots_no_ice: Number | None = 0
    estimated_d_min: Number | None = 0
    total_intensity: Number | None = 0

    @property
    def image_number(self) -> int | None:
        return self.file_pattern_index or self.file_number

    @pydantic.model_validator(mode="after")
    def check_image_number(self) -> PerImageAnalysisResult:
        if not self.image_number:
            raise ValueError("Image number not specified")
        return self


class DLSNotifyGDA(CommonService):
    """A service that forwards per-image-analysis results to GDA."""

    # Human readable service name
    _service_name = "DLS GDA Bridge"

    # Logger name
    _logger_name = "dlstbx.services.notifygda"

    def initializing(self):
        """
        Subscribe to the GDA notification queue.
        Received messages must be acknowledged.
        """
        self.log.debug("GDA Bridge starting")
        self.subscribe(
            "notify_gda",  # consider transient queue
            self.notify_gda,
        )

    @typed_handler
    def notify_gda(
        self,
        rw,
        header,
        message: PerImageAnalysisResult,
        parameters: GDAParameters,
    ):
        """Forward some information to GDA."""

        dcid = parameters.dcid
        gdahost = parameters.host
        gdaport = parameters.port
        image_number = message.image_number

        record: dict[str, Number | None] = {}
        record["spot_total"] = message.n_spots_total
        record["good_bragg_candidates"] = message.n_spots_no_ice
        record["method1_res"] = message.estimated_d_min
        record["total_integrated_signal"] = message.total_intensity

        self.log.debug(
            "Forwarding PIA record for image %r in DCID %s: %r",
            image_number,
            dcid,
            record,
        )

        if gdahost == "mx-control":
            pass  # skip
        else:
            # We notify according to https://jira.diamond.ac.uk/browse/MXGDA-3243
            # by sending a UDP package containing the actual result record information.
            try:
                dlstbx.util.gda.notify(
                    gdahost,
                    gdaport,
                    (
                        "PIA:{dcid}:{image_number}:{r[spot_total]}"
                        ":{r[good_bragg_candidates]}:{r[method1_res]}:{r[total_integrated_signal]}"
                    )
                    .format(dcid=dcid, image_number=image_number, r=record)
                    .encode("latin-1"),
                )
            except Exception as e:
                self.log.error("Could not notify GDA: %s", e, exc_info=True)
                rw.transport.nack(header)
                return
        rw.transport.ack(header)
