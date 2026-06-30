# Standalone PanDDA2 array suite

Run the XChem PanDDA2 hit-identification pipeline (the same processing as the
`pandda_xchem` zocalo wrapper) as a Slurm array job over a `model_building`
directory, **outside** the zocalo / ispyb trigger machinery. No ispyb writes,
no recipes — just a terminal command that `sbatch`es an array job.

## What it does

For every dataset (`<dtag>`) under the given `model_building` directory it runs,
one Slurm array task per dataset:

1. **PanDDA2** `process_dataset.py`
2. **Rhofit** ligand fitting into the best-scoring event
3. **Ligand scoring** of each build
4. **Merge** of the protein model with the best-fitted ligand →
   `<dtag>-pandda-model.pdb`
5. **Output**: `best_score.txt`, `pandda2_results.json`, MVS html.

This is the standalone equivalent of `dlstbx.wrapper.pandda_xchem`; it reuses the
same low-level helpers in `dlstbx.util.pandda` and the MVS output helpers, so the
results match the production pipeline.

## Prerequisites (per dataset)

Each `<model_building>/<dtag>/compound/` must already contain:

- exactly one `<code>.smiles` (datasets without one are skipped), and
- `<code>.cif` restraints (needed by Rhofit).

i.e. the upstream ligand-restraints (grade2) step must already have run. This
suite does **not** generate restraints.

## Usage

```bash
# Preview: enumerate datasets and print the sbatch script without submitting
python -m dlstbx.cli.pandda_array.submit \
    --model-building-dir /dls/labxchem/data/<prop>/<visit>/processing/auto/analysis/model_building \
    --dry-run

# Submit the array job
python -m dlstbx.cli.pandda_array.submit \
    --model-building-dir /dls/labxchem/data/<prop>/<visit>/processing/auto/analysis/model_building
```

Output (the `panddas` directory) defaults to
`<model_building>/../pandda2/panddas`; override with `--out-dir`.

PanDDA2 settings default to `autoprocessing.pandda` from `<visit>/.user.yaml`
(derived from the model_building path); override with `--pandda-args` or point at
a different file with `--user-yaml`.

The submitter writes everything for a run into a timestamped launch directory
(`<out_dir>/.standalone_launch/<stamp>/`):

- `datasets.json` — the ordered dtag list (indexed by `SLURM_ARRAY_TASK_ID`)
- `pandda2_array.sh` — the generated sbatch script
- `slurm-<jobid>_<task>.out` — per-task Slurm logs

Per-dataset processing logs are written to `<out_dir>/standalone_logs/<dtag>.log`.

### Useful options

| Flag | Default | Meaning |
|------|---------|---------|
| `--out-dir` | `<model_building>/../pandda2/panddas` | PanDDA2 output dir |
| `--dtags x0001 x0002` | all valid | restrict to a subset |
| `--overwrite` | off | remove existing `processed_datasets/<dtag>` first |
| `--timeout-minutes` | 145 | PanDDA2 step timeout |
| `--partition` | `mx_low,cs04r` | Slurm partition |
| `--account` | (omitted) | Slurm account |
| `--max-parallel` | 350 | max concurrent array tasks |
| `--time-limit` | `2:30:00` | Slurm wall-clock per task |
| `--dry-run` | off | print the script, do not submit |

## Running a single dataset by hand

The array task entry point can be run directly (e.g. to debug one dtag):

```bash
python -m dlstbx.cli.pandda_array.task \
    --model-building-dir .../model_building \
    --datasets-file .../datasets.json \
    --task-id 1
```

## Files

- `submit.py` — enumerate datasets, write the launch files, `sbatch` the array.
- `task.py` — per-array-task runner; picks its dtag via `SLURM_ARRAY_TASK_ID`.
- `core.py` — `process_pandda_dataset(...)`, the framework-free per-dtag work.
