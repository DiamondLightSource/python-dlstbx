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


class IndexedLatticeResult(pydantic.BaseModel):
    unit_cell: tuple[float, float, float, float, float, float]
    space_group: str
    n_indexed: pydantic.NonNegativeInt


class IndexingResult(pydantic.BaseModel):
    lattices: list[IndexedLatticeResult]
    n_unindexed: pydantic.NonNegativeInt


class Payload(pydantic.BaseModel):
    command: str
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
            "ssx.plot",
            self.receive_msg,
            acknowledgement=True,
            log_extender=self.extend_log,
        )

    @pydantic.validate_arguments(config=dict(arbitrary_types_allowed=True))
    def receive_msg(
        self,
        rw: workflows.recipe.RecipeWrapper,
        header: dict,
        message: dict,
    ):
        commands = {
            "pia": self.plot_pia,
            "index": self.plot_index,
        }
        parameters = ChainMapWithReplacement(
            message if isinstance(message, dict) else {},
            rw.recipe_step["parameters"],
            substitutions=rw.environment,
        )

        # Conditionally acknowledge receipt of the message
        txn = rw.transport.transaction_begin(subscription_id=header["subscription"])
        rw.transport.ack(header, transaction=txn)

        try:
            payload = Payload(**parameters)
        except pydantic.ValidationError as e:
            self.log.error(e, exc_info=True)
            rw.transport.transaction_abort(header, transaction=txn)
            rw.transport.nack(header)
            return

        plotter = commands.get(payload.command)
        if not plotter:
            self.log.error(
                f"Unknown command {payload.command} (available commands: {list(commands.keys())}"
            )
            rw.transport.transaction_abort(header, transaction=txn)
            rw.transport.nack(header)
            return

        if payload.status is None:
            payload.status = Status(start_time=time.time())

        expected_total = payload.files_expected or payload.images_expected
        # The pydantic validator should ensure this is the case but assert
        # needed to satisfy mypy
        assert expected_total is not None
        every = payload.every
        expected_result_count = expected_total // every
        message_delay = 1  # second

        self.log.debug(f"{payload.results_file=}")

        lines = None
        if payload.results_file.exists():
            lines = payload.results_file.read_text().splitlines()

        timeout = (payload.status.start_time + payload.timeout) < time.time()
        if timeout and not lines:
            # Give up waiting for results file to appear
            self.log.info(
                f"Timed out waiting for results in {payload.results_file} (dcid={payload.dcid})"
            )
            rw.transport.transaction_commit(txn)
            return
        elif not lines or len(lines) < expected_result_count:
            # Not found all messages, so checkpoint message with a delay
            self.log.debug(
                f"Waiting for results in {payload.results_file} (dcid={payload.dcid})"
            )
            rw.checkpoint(
                json.loads(payload.json(by_alias=True)),
                delay=message_delay,
                transaction=txn,
            )
            rw.transport.transaction_commit(txn)
            return

        try:
            plotter(payload, lines)
        except pydantic.ValidationError as e:
            self.log.error(e, exc_info=True)
            rw.transport.transaction_abort(header, transaction=txn)
            rw.transport.nack(header)
            return

        rw.transport.transaction_commit(txn)
        return

    def plot_pia(self, payload: Payload, lines: list[str]):
        pia_results = [PerImageAnalysisResult(**json.loads(line)) for line in lines]
        n_spots_total = {
            result.file_number: result.n_spots_total for result in pia_results
        }

        plt.figure()
        ax = plt.subplot()

        plot_pia(n_spots_total, spot_count_cutoff=payload.spot_count_cutoff, ax=ax)

        filename = payload.plot_file
        filename.parent.mkdir(parents=True, exist_ok=True)
        thumbnail = filename.parent / f"{filename.stem}t{filename.suffix}"
        plt.savefig(filename)
        self.log.info(f"Saved plot to {filename}")
        plt.savefig(thumbnail)
        self.log.info(f"Saved thumbnail plot to {thumbnail}")

    def plot_index(self, payload: Payload, lines: list[str]):
        indexing_results = [IndexingResult(**json.loads(line)) for line in lines]
        indexed_lattices = [
            lattice for result in indexing_results for lattice in result.lattices
        ]

        fig, axes = plt.subplots(nrows=3, ncols=3)
        for i, (x, y) in enumerate([("a", "b"), ("a", "c"), ("b", "c")]):
            axes[0, i].set_xlabel(x + " (Å)")
            axes[0, i].set_ylabel(y + " (Å)")
        for i, x in enumerate("abc"):
            axes[1, i].set_xlabel(x + " (Å)")
            axes[1, i].set_ylabel("Frequency")
        for i, x in enumerate(("α", "β", "γ")):
            axes[2, i].set_xlabel(x + " (•)")
            axes[2, i].set_ylabel("Frequency")

        # self.fig.subplots_adjust(left=0.1, bottom=0.05, right=0.95, top=0.95)
        plt.tight_layout(h_pad=0.25, w_pad=0.25)

        a = np.fromiter(
            (lattice.unit_cell[0] for lattice in indexed_lattices), dtype=float
        )
        b = np.fromiter(
            (lattice.unit_cell[1] for lattice in indexed_lattices), dtype=float
        )
        c = np.fromiter(
            (lattice.unit_cell[2] for lattice in indexed_lattices), dtype=float
        )
        alpha = np.fromiter(
            (lattice.unit_cell[3] for lattice in indexed_lattices), dtype=float
        )
        beta = np.fromiter(
            (lattice.unit_cell[4] for lattice in indexed_lattices), dtype=float
        )
        gamma = np.fromiter(
            (lattice.unit_cell[5] for lattice in indexed_lattices), dtype=float
        )

        axes[0, 0].scatter(a, b)
        axes[0, 1].scatter(b, c)
        axes[0, 2].scatter(c, a)
        axes[1, 0].hist(a)
        axes[1, 1].hist(b)
        axes[1, 2].hist(c)
        axes[2, 0].hist(alpha)
        axes[2, 1].hist(beta)
        axes[2, 2].hist(gamma)

        filename = payload.plot_file
        filename.parent.mkdir(parents=True, exist_ok=True)
        thumbnail = filename.parent / f"{filename.stem}t{filename.suffix}"
        plt.savefig(filename)
        self.log.info(f"Saved plot to {filename}")
        plt.savefig(thumbnail)
        self.log.info(f"Saved thumbnail plot to {thumbnail}")


def plot_pia(n_spots_total: dict[int, int], spot_count_cutoff: int = 16, ax=plt.Axes):
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
