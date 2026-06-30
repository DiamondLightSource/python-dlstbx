"""Standalone suite to run the XChem PanDDA2 hit-identification pipeline as a
Slurm array job over a model_building directory, outside of the zocalo /
ispyb trigger machinery.

- ``submit``  -- enumerate datasets in a model_building dir and ``sbatch`` an
  array job.
- ``task``    -- per-array-task entry point; runs one dtag.
- ``core``    -- ``process_pandda_dataset``, the framework-free per-dtag
  processing shared by the array tasks.
"""

from __future__ import annotations

# PanDDA2 software install root. Defined here (rather than in ``core``) so the
# lightweight ``submit`` launcher can import it without pulling in gemmi/MVS.
PANDDA_2_DIR = "/dls_sw/i04-1/software/PanDDA2"
