from __future__ import annotations

import itertools
import json
import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import Any, Optional

import iotbx.mtz
import iotbx.pdb
import pydantic
from cctbx import crystal, uctbx

import dlstbx.util
import dlstbx.util.symlink
from dlstbx.util import ChainMapWithReplacement
from dlstbx.util.pdb import PDBFileOrCode
from dlstbx.wrapper import Wrapper


class Xia2SsxParams(pydantic.BaseModel):
    template: Optional[Path]
    image: Optional[Path]
    unit_cell: Optional[
        tuple[
            pydantic.NonNegativeFloat,
            pydantic.NonNegativeFloat,
            pydantic.NonNegativeFloat,
            pydantic.NonNegativeFloat,
            pydantic.NonNegativeFloat,
            pydantic.NonNegativeFloat,
        ]
    ] = None
    spacegroup: Optional[str] = None
    reference_pdb: list[PDBFileOrCode] = []
    reference_geometry: Optional[Path] = None

    @pydantic.validator("unit_cell", pre=True)
    def check_unit_cell(cls, v):
        if isinstance(v, str):
            v = v.replace(",", " ").split()
        v = tuple(float(v) for v in v)
        return v

    @pydantic.root_validator
    def check_template_or_image(cls, values):
        if values.get("template") is None and values.get("image") is None:
            raise ValueError("Either template or image must be defined")
        return values


class Payload(pydantic.BaseModel):
    working_directory: Path
    results_directory: Path
    create_symlink: Optional[Path] = None
    timeout: Optional[pydantic.PositiveFloat] = None


class Xia2SsxWrapper(Wrapper):
    _logger_name = "dlstbx.wrap.xia2.ssx"
    name = "xia2.ssx"
    params_t = Xia2SsxParams

    def construct_commandline(self, params: Xia2SsxParams):
        command = [
            "xia2.ssx",
            f"template={params.template}"
            if params.template
            else f"image={params.image}",
        ]
        if params.unit_cell:
            command.append("unit_cell=%s,%s,%s,%s,%s,%s" % params.unit_cell)
        if params.spacegroup:
            command.append(f"space_group={params.spacegroup}")
        reference_pdb = self.find_matching_reference_pdb(params)
        if reference_pdb:
            command.append(f"reference={reference_pdb}")
        if params.reference_geometry:
            command.append(f"reference_geometry={params.reference_geometry}")
        return command

    def find_matching_reference_pdb(self, params: Xia2SsxParams) -> str | None:
        if not params.unit_cell and not params.spacegroup:
            return None
        input_symmetry = crystal.symmetry(
            unit_cell=params.unit_cell,
            space_group=params.spacegroup,
        )
        for pdb in params.reference_pdb:
            if not pdb.filepath or pdb.source == "AlphaFold":
                continue
            pdb_inp = iotbx.pdb.input(pdb.filepath)
            crystal_symmetry = pdb_inp.crystal_symmetry()
            if crystal_symmetry is None:
                continue
            if not crystal_symmetry.is_similar_symmetry(
                input_symmetry, relative_length_tolerance=0.05
            ):
                continue
            # Just use the first pdb that matches - we should probably be
            # more clever and choose the closest match
            return pdb.filepath
        return None

    def send_results_to_ispyb(self, z: dict, xtriage_results: dict):
        ispyb_command_list = results_to_ispyb_command_list(
            z, xtriage_results=xtriage_results
        )
        self.log.debug("Sending %s", ispyb_command_list)
        self.recwrap.send_to("ispyb", {"ispyb_command_list": ispyb_command_list})

    def run(self):
        job_parameters = self.recwrap.recipe_step["job_parameters"]
        params_d = ChainMapWithReplacement(
            job_parameters.get("xia2.ssx") or {},
            job_parameters.get("ispyb_parameters") or {},
        )

        try:
            xia2_ssx_params = self.params_t(**params_d)
            params = Payload(**job_parameters)
        except (Exception, pydantic.ValidationError) as e:
            self.log.error(e, exc_info=True)
            raise

        working_directory = params.working_directory
        results_directory = params.results_directory

        # Create working directory with symbolic link
        working_directory.mkdir(parents=True, exist_ok=True)
        if params.create_symlink:
            dlstbx.util.symlink.create_parent_symlink(
                os.fspath(working_directory), params.create_symlink
            )

        command = self.construct_commandline(xia2_ssx_params)
        print(" ".join(command))
        self.log.info(" ".join(command))
        try:
            start_time = time.perf_counter()
            result = subprocess.run(
                command,
                timeout=params.timeout,
                cwd=working_directory,
            )
            runtime = time.perf_counter() - start_time
            self.log.info(f"xia2.ssx took {runtime} seconds")
            self._runtime_hist.observe(runtime)
        except subprocess.TimeoutExpired as te:
            success = False
            self.log.warning(f"xia2.ssx timed out: {te.timeout}\n  {te.cmd}")
            self.log.debug(te.stdout)
            self.log.debug(te.stderr)
            self._timeout_counter.inc()
        else:
            success = not result.returncode
            if success:
                self.log.info("xia2.ssx successful")
            else:
                self.log.info(f"xia2.ssx failed with exitcode {result.returncode}")
                self.log.debug(result.stdout)
                self.log.debug(result.stderr)

        if success:
            # copy output files to result directory
            results_directory.mkdir(parents=True, exist_ok=True)
            if params.create_symlink:
                dlstbx.util.symlink.create_parent_symlink(
                    str(results_directory), params.create_symlink
                )

            # Probably need to be a little more selective here...
            for f in working_directory.iterdir():
                if f.is_file():
                    shutil.copy(f, results_directory)

        for subdir in ("DataFiles", "LogFiles"):
            src = working_directory / subdir
            dst = results_directory / subdir
            if src.exists():
                self.log.debug(f"Recursively copying {src} to {dst}")
                shutil.copytree(src, dst)
            elif not success:
                self.log.info(
                    f"Expected output directory does not exist (non-zero exitcode): {src}"
                )
            else:
                self.log.warning(f"Expected output directory does not exist: {src}")

        allfiles = []
        for f in working_directory.glob("*.*"):
            if f.is_file() and not f.name.startswith("."):
                self.log.debug(f"Copying {f} to results directory")
                shutil.copy(f, results_directory)
                allfiles.append(os.fspath(results_directory / f.name))

        # Send results to various listeners
        logfiles = {"xia2.ssx.log", "xia2.ssx_reduce.log"}
        for result_file in map(results_directory.joinpath, logfiles):
            if result_file.is_file():
                self.record_result_individual_file(
                    {
                        "file_path": os.fspath(result_file.parent),
                        "file_name": result_file.name,
                        "file_type": "log",
                        "importance_rank": 1
                        if result_file.name == "xia2.ssx.html"
                        else 2,
                    }
                )

        datafiles_path = results_directory / "DataFiles"
        if datafiles_path.exists():
            for result_file in datafiles_path.iterdir():
                if not result_file.is_file():
                    continue
                file_type = "result"
                if result_file.suffix in {".log", ".txt"}:
                    file_type = "log"
                self.record_result_individual_file(
                    {
                        "file_path": os.fspath(result_file.parent),
                        "file_name": result_file.name,
                        "file_type": file_type,
                        "importance_rank": 1
                        if result_file.name.endswith("_free.mtz")
                        else 2,
                    }
                )
                allfiles.append(os.fspath(result_file))

        logfiles_path = results_directory / "LogFiles"
        if logfiles_path.exists():
            for result_file in logfiles_path.iterdir():
                if not result_file.is_file():
                    continue
                file_type = "log"
                if result_file.suffix == ".json":
                    file_type = "graph"
                elif result_file.suffix == ".png":
                    file_type = "log"
                self.record_result_individual_file(
                    {
                        "file_path": os.fspath(result_file.parent),
                        "file_name": result_file.name,
                        "file_type": file_type,
                        "importance_rank": 2,
                    }
                )
                allfiles.append(os.fspath(result_file))

        merged_mtz = working_directory / "DataFiles" / "merged.mtz"
        mtz = iotbx.mtz.object(os.fspath(merged_mtz))
        space_group = mtz.space_group().type().lookup_symbol()
        unit_cell = mtz.crystals()[0].unit_cell_parameters()

        dials_merge_json = working_directory / "LogFiles" / "dials.merge.json"
        self.log.info(f"{dials_merge_json=}")
        self.log.info(f"{dials_merge_json.is_file()=}")
        if dials_merge_json.is_file():
            with dials_merge_json.open() as fh:
                d = json.load(fh)
            wl = list(d.keys())[0]

            merging_stats = d[wl]["merging_stats"]
            merging_stats_anom = d[wl]["merging_stats_anom"]

            ispyb_d = {
                "commandline": " ".join(command),
                "spacegroup": space_group,
                "unit_cell": unit_cell,
                "scaling_statistics": ispyb_scaling_statistics_from_merging_stats_d(
                    merging_stats, merging_stats_anom
                ),
            }

            xtriage_results = d[wl]["xtriage_output"]

            self.send_results_to_ispyb(ispyb_d, xtriage_results=xtriage_results)

        if success:
            self._success_counter.inc()
        else:
            self._failure_counter.inc()

        return success


def ispyb_scaling_statistics_from_merging_stats_d(
    merging_stats: dict, merging_stats_anom: dict
):
    def lookup(merging_stats, item, shell):
        i_bin = {"innerShell": 0, "outerShell": -1}.get(shell)
        if i_bin is not None:
            return merging_stats[item][i_bin]
        return merging_stats["overall"][item]

    scaling_statistics = {}

    for shell in {"overall", "innerShell", "outerShell"}:
        scaling_statistics[shell] = {
            "cc_half": lookup(merging_stats, "cc_one_half", shell),
            "completeness": lookup(merging_stats, "completeness", shell),
            "mean_i_sig_i": lookup(merging_stats, "i_over_sigma_mean", shell),
            "multiplicity": lookup(merging_stats, "multiplicity", shell),
            "n_tot_obs": lookup(merging_stats, "n_obs", shell),
            "n_tot_unique_obs": lookup(merging_stats, "n_uniq", shell),
            "r_merge": lookup(merging_stats, "r_merge", shell),
            "res_lim_high": uctbx.d_star_sq_as_d(
                lookup(merging_stats, "d_star_sq_min", shell)
            ),
            "res_lim_low": uctbx.d_star_sq_as_d(
                lookup(merging_stats, "d_star_sq_max", shell)
            ),
            "anom_completeness": lookup(merging_stats_anom, "anom_completeness", shell),
            "anom_multiplicity": lookup(merging_stats_anom, "multiplicity", shell),
            "cc_anom": lookup(merging_stats_anom, "cc_anom", shell),
            "r_meas_all_iplusi_minus": lookup(merging_stats_anom, "r_meas", shell),
        }

    return scaling_statistics


def results_to_ispyb_command_list(
    z: dict, xtriage_results: dict | None = None
) -> list[dict[str, Any]]:
    ispyb_command_list = []

    # Step 1: Add new record to AutoProc, keep the AutoProcID
    register_autoproc = {
        "ispyb_command": "write_autoproc",
        "autoproc_id": None,
        "store_result": "ispyb_autoproc_id",
        "spacegroup": z["spacegroup"],
        "refinedcell_a": z["unit_cell"][0],
        "refinedcell_b": z["unit_cell"][1],
        "refinedcell_c": z["unit_cell"][2],
        "refinedcell_alpha": z["unit_cell"][3],
        "refinedcell_beta": z["unit_cell"][4],
        "refinedcell_gamma": z["unit_cell"][5],
    }
    ispyb_command_list.append(register_autoproc)

    # Step 2: Store scaling results, linked to the AutoProcID
    #         Keep the AutoProcScalingID
    insert_scaling = z["scaling_statistics"]
    insert_scaling.update(
        {
            "ispyb_command": "insert_scaling",
            "autoproc_id": "$ispyb_autoproc_id",
            "store_result": "ispyb_autoprocscaling_id",
        }
    )
    ispyb_command_list.append(insert_scaling)

    # Step 3: Store integration result, linked to the ScalingID
    integration = {
        "ispyb_command": "upsert_integration",
        "scaling_id": "$ispyb_autoprocscaling_id",
        "cell_a": z["unit_cell"][0],
        "cell_b": z["unit_cell"][1],
        "cell_c": z["unit_cell"][2],
        "cell_alpha": z["unit_cell"][3],
        "cell_beta": z["unit_cell"][4],
        "cell_gamma": z["unit_cell"][5],
        #'refined_xbeam': z['refined_beam'][0],
        #'refined_ybeam': z['refined_beam'][1],
    }
    ispyb_command_list.append(integration)

    if xtriage_results is not None:
        for level, messages in xtriage_results.items():
            for message in messages:
                if (
                    message["text"]
                    == "The merging statistics indicate that the data may be assigned to the wrong space group."
                ):
                    # this is not a useful warning
                    continue
                ispyb_command_list.append(
                    {
                        "ispyb_command": "add_program_message",
                        "program_id": "$ispyb_autoprocprogram_id",
                        "message": message["text"],
                        "description": message["summary"],
                        "severity": {0: "INFO", 1: "WARNING", 2: "ERROR"}.get(
                            message["level"]
                        ),
                    }
                )
    return ispyb_command_list


class Xia2SsxReduceParams(pydantic.BaseModel):
    data: list[str]
    unit_cell: Optional[
        tuple[
            pydantic.NonNegativeFloat,
            pydantic.NonNegativeFloat,
            pydantic.NonNegativeFloat,
            pydantic.NonNegativeFloat,
            pydantic.NonNegativeFloat,
            pydantic.NonNegativeFloat,
        ]
    ] = None
    spacegroup: Optional[str] = None
    reference_pdb: list[PDBFileOrCode] = []

    @pydantic.validator("unit_cell", pre=True)
    def check_unit_cell(cls, v):
        if isinstance(v, str):
            v = v.replace(",", " ").split()
        v = tuple(float(v) for v in v)
        return v

    @pydantic.validator("spacegroup", pre=True)
    def check_spacegroup(cls, v):
        if isinstance(v, list):
            v = v[0]
        return v


class Xia2SsxReduceWrapper(Xia2SsxWrapper):
    _logger_name = "dlstbx.wrap.xia2.ssx_reduce"
    name = "xia2.ssx_reduce"
    params_t = Xia2SsxReduceParams

    def construct_commandline(self, params: Xia2SsxParams):
        command = [
            "xia2.ssx_reduce",
        ]
        data_files = itertools.chain.from_iterable(
            files.split(";") for files in params.data
        )
        for f in data_files:
            command.append(f)
        if params.unit_cell:
            command.append("unit_cell=%s,%s,%s,%s,%s,%s" % params.unit_cell)
        if params.spacegroup:
            command.append(f"space_group={params.spacegroup}")
        reference_pdb = self.find_matching_reference_pdb(params)
        if reference_pdb:
            command.append(f"reference={reference_pdb}")
        return command
