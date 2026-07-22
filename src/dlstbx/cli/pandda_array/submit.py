"""Submit a Slurm array job that runs the PanDDA2 hit-identification pipeline
over every dataset in a ``model_building`` directory.

Standalone equivalent of the ispyb ``postprocessing-pandda2-array`` recipe:
enumerate the per-dtag subdirectories, write the dataset list, and ``sbatch``
an array job whose tasks each call :mod:`dlstbx.cli.pandda_array.task`.

Example::

    python -m dlstbx.cli.pandda_array.submit \\
        --model-building-dir \\
        /dls/labxchem/data/lb42888/lb42888-1/processing/auto/analysis/model_building
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import dlstbx
from dlstbx.cli.pandda_array import PANDDA_2_DIR

# Defaults follow the live ispyb-postprocessing-pandda2-array recipe, with time
# and memory raised and concurrency capped for standalone per-user submission.
DEFAULT_PARTITION = "mx_low,cs04r"
DEFAULT_MODULES = ["dials/latest", "buster/20260424"]
DEFAULT_TIME_LIMIT = "5:00:00"
# QOS 'normal' caps a user at cpu=160 AND node=8. At --cpus 4 the CPU cap allows
# 40 concurrent tasks; run 39 to leave 4 CPUs free for other work. Whether we
# actually reach 39 depends on --mem-per-cpu, since memory decides how many tasks
# fit on each of the 8 nodes (see the --mem-per-cpu comment below).
DEFAULT_MAX_PARALLEL = 39


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Submit a Slurm array job running PanDDA2 hit-identification "
        "over every dataset in a model_building directory."
    )
    parser.add_argument(
        "--model-building-dir",
        required=True,
        type=Path,
        help="Shared model_building directory containing per-dtag subdirectories, "
        "each with compound/<code>.smiles (and .cif restraints) already in place.",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=None,
        help="PanDDA2 output (panddas) directory. "
        "Default: <model_building>/../pandda2/panddas",
    )
    parser.add_argument(
        "--dtags",
        nargs="*",
        default=None,
        help="Explicit subset of dtags to process. Default: every subdirectory "
        "with a single compound/*.smiles file.",
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
        help="Wall-clock timeout for the PanDDA2 step in each task (default: 295). "
        "Keep this ~5 minutes under --time-limit so a stuck step is killed by the "
        "task itself, leaving a usable log, rather than by Slurm.",
    )

    # PanDDA2 settings
    group = parser.add_argument_group("PanDDA2 settings")
    group.add_argument(
        "--pandda-args",
        default=None,
        help="Extra --key=value args for process_dataset.py. "
        "Default: read autoprocessing.pandda from --user-yaml if present.",
    )
    group.add_argument(
        "--user-yaml",
        type=Path,
        default=None,
        help="Path to .user.yaml for PanDDA settings. "
        "Default: <visit>/.user.yaml derived from the model_building path.",
    )
    group.add_argument(
        "--pandda2-dir",
        default=PANDDA_2_DIR,
        help=f"PanDDA2 software install root (default: {PANDDA_2_DIR}).",
    )

    # Slurm knobs
    group = parser.add_argument_group("Slurm")
    group.add_argument("--partition", default=DEFAULT_PARTITION)
    group.add_argument(
        "--account", default=None, help="Slurm account (--account). Omitted if unset."
    )
    group.add_argument("--cpus", type=int, default=4, help="--cpus-per-task")
    group.add_argument(
        # Memory, not CPUs, decides how many tasks pack onto a node, and QOS
        # 'normal' caps a user at 8 nodes. Cluster nodes are 40 CPU / 366 GB, so
        # at 4 CPUs and 16 GB/CPU (64 GB/task) 5 tasks fit per node -> 40
        # concurrent, which is also the cpu=160 cap. Raising this much above
        # ~18 GB/CPU drops to 4 tasks/node and *loses* concurrency.
        "--mem-per-cpu",
        type=int,
        default=16384,
        help="--mem-per-cpu in MB",
    )
    group.add_argument("--time-limit", default=DEFAULT_TIME_LIMIT, help="--time")
    group.add_argument(
        "--max-parallel",
        type=int,
        default=DEFAULT_MAX_PARALLEL,
        help="Max concurrent array tasks (the %%N in --array=1-K%%N).",
    )
    group.add_argument("--job-name", default="PanDDA2")
    group.add_argument(
        "--modules",
        nargs="*",
        default=DEFAULT_MODULES,
        help="Environment modules to load before each task.",
    )
    group.add_argument(
        "--python",
        default="python",
        help="Python interpreter used inside the job (default: 'python', as "
        "provided by the loaded modules).",
    )
    group.add_argument(
        "--pythonpath",
        default=None,
        help="Prepended to PYTHONPATH inside the job so this dlstbx checkout is "
        "importable. Default: auto-detected from the running dlstbx.",
    )

    parser.add_argument(
        "--launch-dir",
        type=Path,
        default=None,
        help="Directory for the sbatch script, dataset list and Slurm logs. "
        "Default: <out_dir>/.standalone_launch",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Write the launch files and print the sbatch script, but do not submit.",
    )
    return parser


def find_datasets(model_dir: Path, explicit=None):
    """Return (valid_dtags, skipped) where each dtag has exactly one
    compound/*.smiles. ``skipped`` is a list of (name, n_smiles)."""
    if explicit:
        candidates = list(explicit)
    else:
        candidates = sorted(p.name for p in model_dir.iterdir() if p.is_dir())

    valid, skipped = [], []
    for name in candidates:
        smiles = list((model_dir / name / "compound").glob("*.smiles"))
        if len(smiles) == 1:
            valid.append(name)
        else:
            skipped.append((name, len(smiles)))
    return valid, skipped


def resolve_pandda_args(args, model_dir: Path) -> str:
    """Determine the PanDDA2 extra-args string from the CLI / user.yaml."""
    if args.pandda_args is not None:
        return args.pandda_args

    user_yaml = args.user_yaml
    if user_yaml is None:
        # model_building -> analysis -> auto -> processing -> <visit>
        try:
            user_yaml = model_dir.parents[3] / ".user.yaml"
        except IndexError:
            user_yaml = None

    if user_yaml and user_yaml.exists():
        # Imported lazily so the submitter has no hard gemmi dependency.
        from dlstbx.util.pandda import get_pandda_settings

        return get_pandda_settings(user_yaml)
    return ""


def _detect_pythonpath() -> str:
    # .../python-dlstbx/src/dlstbx/__init__.py -> .../python-dlstbx/src
    return str(Path(dlstbx.__file__).resolve().parents[1])


def build_sbatch_script(
    *,
    args,
    model_dir: Path,
    panddas_dir: Path,
    datasets_file: Path,
    launch_dir: Path,
    n_datasets: int,
    pandda_args: str,
    pythonpath: str,
) -> str:
    directives = [
        f"#SBATCH --job-name={args.job_name}",
        f"#SBATCH --partition={args.partition}",
        f"#SBATCH --cpus-per-task={args.cpus}",
        f"#SBATCH --mem-per-cpu={args.mem_per_cpu}",
        f"#SBATCH --time={args.time_limit}",
        f"#SBATCH --array=1-{n_datasets}%{args.max_parallel}",
        f"#SBATCH --output={launch_dir / 'slurm-%A_%a.out'}",
    ]
    if args.account:
        directives.append(f"#SBATCH --account={args.account}")

    module_lines = [f"module load {m}" for m in args.modules]

    task_cmd = [
        args.python,
        "-m",
        "dlstbx.cli.pandda_array.task",
        "--model-building-dir",
        str(model_dir),
        "--out-dir",
        str(panddas_dir),
        "--datasets-file",
        str(datasets_file),
        "--timeout-minutes",
        str(args.timeout_minutes),
        "--pandda2-dir",
        args.pandda2_dir,
        # Single --opt=value token: the value can start with "--", which
        # argparse would otherwise reject as a missing argument.
        f"--pandda-args={pandda_args}",
    ]
    if args.overwrite:
        task_cmd.append("--overwrite")
    task_line = shlex.join(task_cmd)

    return "\n".join(
        [
            "#!/bin/bash",
            *directives,
            "",
            "set -euo pipefail",
            *module_lines,
            f"export PYTHONPATH={shlex.quote(pythonpath)}${{PYTHONPATH:+:$PYTHONPATH}}",
            "",
            task_line,
            "",
        ]
    )


def main(argv=None) -> int:
    # Keep new files/dirs group-writable for the proposal group.
    os.umask(0o002)

    args = build_parser().parse_args(argv)

    model_dir = args.model_building_dir.resolve()
    if not model_dir.is_dir():
        sys.exit(f"model_building directory not found: {model_dir}")

    panddas_dir = (
        args.out_dir.resolve()
        if args.out_dir
        else (model_dir.parent / "pandda2" / "panddas").resolve()
    )

    datasets, skipped = find_datasets(model_dir, args.dtags)
    if skipped:
        print(f"Skipping {len(skipped)} dir(s) without exactly one compound/*.smiles:")
        for name, n in skipped:
            print(f"  - {name} ({n} .smiles files)")
    if not datasets:
        sys.exit(f"No valid datasets found under {model_dir}")
    print(f"Found {len(datasets)} dataset(s) to process.")

    pandda_args = resolve_pandda_args(args, model_dir)
    if pandda_args:
        print(f"PanDDA2 extra args: {pandda_args}")
    pythonpath = args.pythonpath or _detect_pythonpath()

    launch_dir = (
        args.launch_dir.resolve()
        if args.launch_dir
        else panddas_dir / ".standalone_launch"
    )
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    launch_dir = launch_dir / stamp
    launch_dir.mkdir(parents=True, exist_ok=True)

    datasets_file = launch_dir / "datasets.json"
    datasets_file.write_text(json.dumps(datasets, indent=2))

    script = build_sbatch_script(
        args=args,
        model_dir=model_dir,
        panddas_dir=panddas_dir,
        datasets_file=datasets_file,
        launch_dir=launch_dir,
        n_datasets=len(datasets),
        pandda_args=pandda_args,
        pythonpath=pythonpath,
    )
    script_file = launch_dir / "pandda2_array.sh"
    script_file.write_text(script)
    script_file.chmod(0o755)

    print(f"Launch directory: {launch_dir}")
    print(f"Dataset list:     {datasets_file}")
    print(f"sbatch script:    {script_file}")
    print(f"PanDDA2 output:   {panddas_dir}")

    if args.dry_run:
        print("\n--- sbatch script (dry run, not submitted) ---")
        print(script)
        return 0

    result = subprocess.run(
        ["sbatch", str(script_file)],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        sys.stderr.write(result.stdout)
        sys.stderr.write(result.stderr)
        sys.exit(f"sbatch failed with exit code {result.returncode}")
    print(result.stdout.strip())
    return 0


def run() -> None:
    sys.exit(main(sys.argv[1:]))


if __name__ == "__main__":
    run()
