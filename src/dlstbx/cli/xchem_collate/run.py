"""Framework-free XChem collate for one labxchem visit.

Standalone equivalent of ``dlstbx.wrapper.xchem_collate.XChemCollateWrapper.run``
with the zocalo / ispyb coupling removed. Takes a processing directory and a
few flags, reuses the same util functions the wrapper calls, and runs the
collate as a single (non-array) job.
"""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
from pathlib import Path

from dlstbx.util.pipedream_xchem_helpers import (
    cleanup_setvar_files,
    write_pipedream_parameters,
)
from dlstbx.util.soakdb import prepare_auto_db, updatable_crystals
from dlstbx.util.xchem_collate_helpers import (
    symlink_score_buckets,
    update_xchem_database,
)

PANDDA_2_DIR = "/dls_sw/i04-1/software/PanDDA2"
XCHEM_PYTHON = "/dls/science/groups/i04-1/software/micromamba/envs/xchem/bin/python"
XCA_PYTHON = (
    "/dls/science/groups/i04-1/software/xchem-align-staging/env_xchem_align/bin/python"
)


def collate(
    *,
    processing_dir: Path,
    pipedream: bool = True,
    overwrite: bool = False,
    timeout_minutes: float = 180,
    pandda2_dir: str = PANDDA_2_DIR,
    logger: logging.Logger,
) -> bool:
    """Collate PanDDA2 & Pipedream results for a labxchem visit, run model
    selection and re-integrate results back into soakDB and the XChem
    environment.

    Args:
        processing_dir: the labxchem ``<visit>/processing`` directory.
        pipedream: also collate Pipedream results to html.
        overwrite: passed to ``updatable_crystals`` model-selection gating.
        timeout_minutes: wall-clock timeout for each external collate step.
        pandda2_dir: PanDDA2 software install root.
        logger: where to send progress/error messages.

    Returns:
        ``True`` once the collate finishes.
    """
    auto_dir = processing_dir / "auto"
    analysis_dir = auto_dir / "analysis"
    pandda_dir = analysis_dir / "pandda2"
    model_dir = analysis_dir / "model_building"
    panddas_dir = pandda_dir / "panddas"
    pipedream_dir = analysis_dir / "pipedream"

    # -------------------------------------------------------
    # Collate PanDDA2 results --> events & sites csv

    if panddas_dir.exists():
        pandda2_command = f"source {pandda2_dir}/venv/bin/activate; \
        python -u {pandda2_dir}/scripts/postrun.py --data_dirs={model_dir} --out_dir={panddas_dir} --use_ligand_data=False --debug=True --local_cpus=4 > {panddas_dir / 'pandda2_postrun.log'}"

        logger.info(f"Running XChemCollate command: {pandda2_command}")

        try:
            subprocess.run(
                pandda2_command,
                shell=True,
                capture_output=True,
                text=True,
                cwd=panddas_dir,
                check=True,
                timeout=timeout_minutes * 60,
            )
        except subprocess.CalledProcessError as e:
            logger.error(f"XChemCollate command: '{pandda2_command}' failed")
            logger.info(e.stdout)
            logger.error(e.stderr)
    else:
        logger.info(f"No panddas directory at {panddas_dir}, skipping PanDDA2 postrun")

    # -------------------------------------------------------
    # Model selection (PanDDA2 | Pipedream) & re-integrate into XChem environment

    try:
        db_copy = prepare_auto_db(processing_dir)
        updatable = updatable_crystals(db_copy, overwrite)
    except Exception as e:
        logger.error(f"Could not prepare auto db for {processing_dir}: {e}")
        db_copy, updatable = None, None

    if updatable is not None:
        try:
            symlink_score_buckets(panddas_dir, pandda_dir, updatable, logger)
        except Exception as e:
            logger.error(f"Exception bucketing scores for {panddas_dir}: {e}")

        try:
            update_xchem_database(
                model_dir, pipedream_dir, panddas_dir, db_copy, updatable, logger
            )
        except Exception as e:
            logger.error(f"Exception updating database for {processing_dir}: {e}")

    # -------------------------------------------------------
    # Pipedream collate --> html output
    if pipedream:
        pipedream_command = f"{XCHEM_PYTHON} /dls/science/groups/i04-1/software/pipedream_xchem/collate_pipedream_results.py \
        --input {pipedream_dir / 'Pipedream_output.json'}  --output-dir {pipedream_dir / 'Pipedream_results'} --no-browser --no-plots -v"

        logger.info(f"Running Collate command: {pipedream_command}")

        try:
            subprocess.run(
                pipedream_command,
                shell=True,
                capture_output=True,
                text=True,
                cwd=pipedream_dir,
                check=True,
                timeout=timeout_minutes * 60,
            )
        except subprocess.CalledProcessError as e:
            logger.error(
                f"Pipedream collate command failed (exit {e.returncode})\n"
                f"--- stdout ---\n{e.stdout}\n--- stderr ---\n{e.stderr}"
            )

        try:
            write_pipedream_parameters(processing_dir, pipedream_dir, logger=logger)
        except Exception as e:
            logger.error(
                f"Could not write pipedream parameters for {pipedream_dir}: {e}"
            )
    else:
        logger.info(f"Skipping collation of Pipedream results for {pipedream_dir}")

    # Clean up orphaned autoBUSTER setvar logs left in the pipedream dir
    try:
        cleanup_setvar_files(pipedream_dir, logger)
    except Exception as e:
        logger.error(f"Could not clean up setvar logs in {pipedream_dir}: {e}")

    # -------------------------------------------------------
    # XChemAlign collate step
    # xca_dir = processing_dir / "analysis" / "xchemalign"
    # config = xca_dir / "config.yaml"
    # assemblies = xca_dir / "assemblies.yaml"

    # if not config.exists() or not assemblies.exists():
    #     logger.info(f"No config/assemblies .yaml in {xca_dir}, skipping autoXCA")
    # else:
    #     autoxca_dir = auto_dir / "xchemalign"
    #     autoxca_dir.mkdir(parents=True, exist_ok=True)
    #     shutil.copy(config, autoxca_dir / "config.yaml")
    #     shutil.copy(assemblies, autoxca_dir / "assemblies.yaml")

    #     xca_command = f"{XCA_PYTHON} -m xchemalign.collator -d {autoxca_dir} && \
    #     {XCA_PYTHON} -m xchemalign.aligner -d {autoxca_dir}"

    #     logger.info(f"Running XCA command: {xca_command}")

    #     try:
    #         subprocess.run(
    #             xca_command,
    #             shell=True,
    #             capture_output=True,
    #             text=True,
    #             cwd=autoxca_dir,
    #             check=True,
    #             timeout=timeout_minutes * 60,
    #         )
    #     except subprocess.CalledProcessError as e:
    #         logger.error(
    #             f"XCA command failed (exit {e.returncode})\n"
    #             f"--- stdout ---\n{e.stdout}\n--- stderr ---\n{e.stderr}"
    #         )

    #     # Push aligner tarball to Fragalysis
    #     try:
    #         upload_to_fragalysis(
    #             autoxca_dir,
    #             target_access_string=processing_dir.parent.name,
    #             logger=logger,
    #         )
    #     except Exception as e:
    #         logger.error(f"Could not upload {autoxca_dir} to Fragalysis: {e}")

    # logger.info("XChemCollate finished successfully")
    return True


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the XChem collate pipeline for a single labxchem visit "
        "(non-array)."
    )
    parser.add_argument(
        "--processing-directory",
        required=True,
        type=Path,
        help="The labxchem <visit>/processing directory.",
    )
    parser.add_argument(
        "--no-pipedream",
        dest="pipedream",
        action="store_false",
        help="Skip the Pipedream collate step (default: run it).",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite semantics for soakDB model-selection gating.",
    )
    parser.add_argument(
        "--timeout-minutes",
        type=float,
        default=60,
        help="Wall-clock timeout for each external collate step (default: 60).",
    )
    parser.add_argument(
        "--pandda2-dir",
        default=PANDDA_2_DIR,
        help=f"PanDDA2 software install root (default: {PANDDA_2_DIR}).",
    )
    return parser


def _setup_logger(processing_dir: Path) -> logging.Logger:
    logger = logging.getLogger("xchem_collate_standalone")
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    log_dir = processing_dir / "auto" / "analysis"
    if log_dir.is_dir():
        handlers.append(logging.FileHandler(log_dir / "xchem_collate_standalone.log"))
    for handler in handlers:
        handler.setFormatter(fmt)
    logger.handlers = handlers
    return logger


def main(argv=None) -> int:
    # Keep new files/dirs group-writable for the proposal group.
    os.umask(0o002)

    args = build_parser().parse_args(argv)
    processing_dir = args.processing_directory.resolve()
    if not processing_dir.is_dir():
        sys.exit(f"processing directory not found: {processing_dir}")

    logger = _setup_logger(processing_dir)
    logger.info(f"Collating {processing_dir} (pipedream={args.pipedream})")

    try:
        ok = collate(
            processing_dir=processing_dir,
            pipedream=args.pipedream,
            overwrite=args.overwrite,
            timeout_minutes=args.timeout_minutes,
            pandda2_dir=args.pandda2_dir,
            logger=logger,
        )
    except Exception:
        logger.exception(f"Unhandled error collating {processing_dir}")
        return 1

    return 0 if ok else 1


def run() -> None:
    sys.exit(main(sys.argv[1:]))


if __name__ == "__main__":
    run()
