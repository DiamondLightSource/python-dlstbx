#!/bin/bash
#SBATCH --job-name=XChemCollate
#SBATCH --partition=cs04r
#SBATCH --cpus-per-task=4
#SBATCH --mem-per-cpu=12288
#SBATCH --time=1:00:00
#SBATCH --output=xchem_collate_%j.out
#
# Run the XChem collate pipeline for a single labxchem visit (not an array job):
# PanDDA2 postrun -> model selection + soakDB update -> Pipedream collate ->
# XChemAlign collate -> Fragalysis upload.
#
# Usage (submit to the cluster, or run inline on the current node):
#   sbatch run_xchem_collate.sh <processing_dir> [--no-pipedream] [--overwrite]
#   bash   run_xchem_collate.sh <processing_dir> [--no-pipedream] [--overwrite]
#
# <processing_dir> is the labxchem <visit>/processing directory, e.g.
#   /dls/labxchem/data/lb42888/lb42888-1/processing
#
# Override the dlstbx checkout used inside the job with DLSTBX_SRC=/path/to/src.

set -euo pipefail
umask 002   # keep new files group-writable for the proposal group

PROCESSING_DIR="${1:?Usage: [sbatch] run_xchem_collate.sh <processing_directory> [--no-pipedream] [--overwrite]}"
shift

# This standalone code lives in the dev checkout, not in the dials module, so
# make it importable inside the job.
DLSTBX_SRC="${DLSTBX_SRC:-/dls/science/users/qvu59474/softwaresrc/dials_dev/modules/python-dlstbx/src}"

module load dials/latest
module load buster/20260424

export PYTHONPATH="${DLSTBX_SRC}:${PYTHONPATH:-}"

python -m dlstbx.cli.xchem_collate.run \
    --processing-directory "${PROCESSING_DIR}" "$@"
