from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Optional

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pydantic
import workflows.recipe
from workflows.services.common_service import CommonService

from dlstbx.util import ChainMapWithReplacement

matplotlib.use("Agg")


class Status(pydantic.BaseModel):
    start_time: Optional[pydantic.PositiveFloat] = None


class PerImageAnalysisResult(pydantic.BaseModel):
    file_number: pydantic.PositiveInt = pydantic.Field(alias="file-number")
    n_spots_total: pydantic.NonNegativeInt


class Payload(pydantic.BaseModel):
    dcid: pydantic.NonNegativeInt
    results_file: Path
    plot_file: Path
    every: int = 1
    spot_count_cutoff: pydantic.NonNegativeInt = 16
    files_expected: Optional[pydantic.NonNegativeInt] = pydantic.Field(
        alias="files-expected"
    )
    images_expected: Optional[pydantic.NonNegativeInt] = pydantic.Field(
        alias="images-expected"
    )
    timeout: pydantic.PositiveFloat = 3600
    status: Optional[Status]

    @pydantic.root_validator
    def check_files_expected_or_images_expected(cls, values):
        if (
            values.get("files_expected") is None
            and values.get("images_expected") is None
        ):
            raise ValueError("Either files-expected or images-expected must be defined")
        return values


class SSXPlotter(CommonService):
    """A service that generates SSX analysis plots"""

    # Human readable service name
    _service_name = "SSX plotter"

    _logger_name = "dlstbx.services.ssx_plotter"

    def initializing(self):
        workflows.recipe.wrap_subscribe(
            self._transport,
            "ssx.plot_pia",
            self.plot_pia,
            acknowledgement=True,
            log_extender=self.extend_log,
        )

    @pydantic.validate_arguments(config=dict(arbitrary_types_allowed=True))
    def plot_pia(
        self,
        rw: workflows.recipe.RecipeWrapper,
        header: dict,
        message: dict,
    ):
        parameters = ChainMapWithReplacement(
            message if isinstance(message, dict) else {},
            rw.recipe_step["parameters"],
            substitutions=rw.environment,
        )
        payload = Payload(**parameters)

        if payload.status is None:
            payload.status = Status(start_time=time.time())

        # Conditionally acknowledge receipt of the message
        txn = rw.transport.transaction_begin(subscription_id=header["subscription"])
        rw.transport.ack(header, transaction=txn)

        expected_total = payload.files_expected or payload.images_expected
        # The pydantic validator should ensure this is the case but assert
        # needed to satisfy mypy
        assert expected_total is not None
        every = payload.every
        expected_result_count = expected_total // every
        message_delay = 1  # second

        lines = None
        if payload.results_file.exists():
            lines = payload.results_file.read_text().splitlines()

        timeout = (payload.status.start_time + payload.timeout) < time.time()
        if timeout and not lines:
            # Give up waiting for results file to appear
            self.log.info(
                f"Timed out waiting for PIA results in {payload.results_file} (dcid={payload.dcid})"
            )
            rw.transport.transaction_commit(txn)
            return
        elif not lines or len(lines) < expected_result_count:
            # Not found all messages, so checkpoint message with a delay
            self.log.debug(
                f"Waiting for PIA results in {payload.results_file} (dcid={payload.dcid})"
            )
            rw.checkpoint(
                json.loads(payload.json(by_alias=True)),
                delay=message_delay,
                transaction=txn,
            )
            rw.transport.transaction_commit(txn)
            return

        pia_results = [PerImageAnalysisResult(**json.loads(line)) for line in lines]
        n_spots_total = {
            result.file_number: result.n_spots_total for result in pia_results
        }

        plt.figure()
        ax = plt.subplot()

        plot_hits(n_spots_total, spot_count_cutoff=payload.spot_count_cutoff, ax=ax)

        filename = payload.plot_file
        filename.parent.mkdir(parents=True, exist_ok=True)
        thumbnail = filename.parent / f"{filename.stem}t{filename.suffix}"
        plt.savefig(filename)
        self.log.info(f"Saved plot to {filename}")
        plt.savefig(thumbnail)
        self.log.info(f"Saved thumbnail plot to {thumbnail}")

        rw.transport.transaction_commit(txn)
        return


def plot_hits(n_spots_total: dict[int, int], spot_count_cutoff: int = 16, ax=plt.Axes):
    if ax is None:
        ax = plt.gca()

    image_numbers = np.array(list(n_spots_total.keys()))
    strong = np.array(list(n_spots_total.values()))

    # sort by image number
    ind = image_numbers.argsort()
    image_numbers = image_numbers[ind]
    strong = strong[ind]

    ax.scatter(image_numbers, strong)
    if spot_count_cutoff:
        hit_rate = 100 * np.count_nonzero(strong > spot_count_cutoff) / len(strong)
        ax.text(
            0.05,
            0.95,
            f"Estimate hits rate: {hit_rate:.1f} %",
            transform=ax.transAxes,
            va="top",
        )
    ax.set_xlabel("Image Number")
    ax.set_ylabel("Spot Counts")
    return ax
