"""Per-array-task entry point for the standalone PanDDA2 suite.

Invoked once per Slurm array task (by ``submit``). Reads the dataset list
written by the submitter, picks this task's dtag via ``SLURM_ARRAY_TASK_ID``
(1-based), and runs :func:`dlstbx.cli.pandda_array.core.process_pandda_dataset`.

Can also be run by hand for a single dataset, e.g.::

    python -m dlstbx.cli.pandda_array.task \\
        --model-building-dir .../analysis/model_building \\
        --datasets-file .../datasets.json --task-id 1
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

from dlstbx.cli.pandda_array import PANDDA_2_DIR
from dlstbx.cli.pandda_array.core import process_pandda_dataset


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run PanDDA2 hit-identification for a single dtag (one Slurm array task)."
    )
    parser.add_argument(
        "--model-building-dir",
        required=True,
        type=Path,
        help="Shared model_building directory containing per-dtag subdirectories.",
    )
    parser.add_argument(
        "--datasets-file",
        required=True,
        type=Path,
        help="JSON list of dtags (written by the submitter); indexed by task id.",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=None,
        help="PanDDA2 output (panddas) directory. "
        "Default: <model_building>/../pandda2/panddas",
    )
    parser.add_argument(
        "--task-id",
        type=int,
        default=None,
        help="1-based dataset index. Default: $SLURM_ARRAY_TASK_ID.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Delete any existing processed_datasets/<dtag> before running.",
    )
    parser.add_argument(
        "--timeout-minutes",
        type=float,
        default=295,
        help="Wall-clock timeout for the PanDDA2 step (default: 295).",
    )
    parser.add_argument(
        "--pandda-args",
        default="",
        help="Extra --key=value args forwarded to PanDDA2 process_dataset.py.",
    )
    parser.add_argument(
        "--pandda2-dir",
        default=PANDDA_2_DIR,
        help=f"PanDDA2 software install root (default: {PANDDA_2_DIR}).",
    )
    return parser


def _setup_logger(out_dir: Path, dtag: str) -> logging.Logger:
    log_dir = out_dir / "standalone_logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(f"pandda_array.{dtag}")
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    file_handler = logging.FileHandler(log_dir / f"{dtag}.log")
    file_handler.setFormatter(fmt)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(fmt)
    logger.handlers = [file_handler, stream_handler]
    return logger


def main(argv=None) -> int:
    # Keep new files/dirs group-writable for the proposal group.
    os.umask(0o002)

    args = build_parser().parse_args(argv)

    task_id = args.task_id
    if task_id is None:
        env_task_id = os.environ.get("SLURM_ARRAY_TASK_ID")
        if not env_task_id:
            sys.exit(
                "No task id: pass --task-id or run within a Slurm array job "
                "(SLURM_ARRAY_TASK_ID unset)."
            )
        task_id = int(env_task_id)

    datasets = json.loads(args.datasets_file.read_text())
    if not 1 <= task_id <= len(datasets):
        sys.exit(f"Task id {task_id} out of range 1..{len(datasets)}")
    dtag = datasets[task_id - 1]

    model_dir = args.model_building_dir.resolve()
    panddas_dir = (
        args.out_dir.resolve()
        if args.out_dir
        else (model_dir.parent / "pandda2" / "panddas").resolve()
    )

    logger = _setup_logger(panddas_dir, dtag)
    logger.info(f"Task {task_id}/{len(datasets)} -> dtag {dtag}")

    try:
        ok = process_pandda_dataset(
            model_dir=model_dir,
            panddas_dir=panddas_dir,
            dtag=dtag,
            pandda_args=args.pandda_args,
            timeout_minutes=args.timeout_minutes,
            overwrite=args.overwrite,
            pandda2_dir=args.pandda2_dir,
            logger=logger,
        )
    except Exception:
        logger.exception(f"Unhandled error processing dtag {dtag}")
        return 1

    return 0 if ok else 1


def run() -> None:
    sys.exit(main(sys.argv[1:]))


if __name__ == "__main__":
    run()
