from __future__ import annotations

import json
import os
from itertools import tee
from pathlib import Path

import libtbx.load_env
from cctbx.eltbx import henke, sasaki
from cctbx.sgtbx import space_group, space_group_symbols
from cctbx.uctbx import unit_cell
from iotbx import mtz
from iotbx.bioinformatics import fasta_sequence

from dlstbx.util.processing_stats import get_model_data


def get_tabulated_fp_fpp(atom, name, wavelength):
    """Select reference f' and f" values for a given heavy atom."""

    fp_fpp_table = {
        "Se": {"peak": 12665, "infl": 12657, "hrem": 12860, "lrem": 12460},
        "Zn": {"peak": 9671, "infl": 9659, "hrem": 9824, "lrem": 9524},
        "Pt": {"peak": 11572, "infl": 11564, "hrem": 11867, "lrem": 11372},
        "Hg": {"peak": 12320, "infl": 12283, "hrem": 12540, "lrem": 12066},
        "Br": {"peak": 13479, "infl": 13473, "hrem": 13529, "lrem": 13423},
        "Mn": {"peak": 6547, "infl": 6539, "hrem": 6690, "lrem": 6390},
        "Fe": {"peak": 7117, "infl": 7112, "hrem": 7212, "lrem": 7017},
    }
    try:
        tbl = sasaki.table(atom)
        fp_fdp = tbl.at_ev(fp_fpp_table[atom][name])
        res = (fp_fdp.fp(), fp_fdp.fdp())
    except Exception:
        if wavelength > 2.8:
            tbl = henke.table(atom)
        else:
            tbl = sasaki.table(atom)
        fp_fdp = tbl.at_angstrom(wavelength)
        res = (fp_fdp.fp(), fp_fdp.fdp())

    return res


def spacegroup_short(spacegroup_name, logger):
    """Code from cctbx tst_sgtbx.py test to look up short space group symbols."""

    symbols_cpp = libtbx.env.under_dist("cctbx", "sgtbx/symbols.cpp")
    if not os.path.isfile(symbols_cpp):
        logger.warning(
            "Cannot generate short space group symbol. %s file not available.",
            symbols_cpp,
        )
        return spacegroup_name
    else:
        table_name = "vol_a_short_mono_hm_dict"
        with open(symbols_cpp) as f:
            for line in f:
                if line.find(table_name) > 0:
                    break
            else:
                logger.warning(f"Cannot find {table_name} table in {symbols_cpp} file.")
                return spacegroup_name
            symbol_pairs = []
            for line in f:
                if line.find("{ 0, 0 },") > 0:
                    break
                for c in '{},"':
                    line = line.replace(c, "")
                short_sg, full_sg = line.split()
                full_sg = space_group_symbols(full_sg, "A").hermann_mauguin()
                symbol_pairs.append((full_sg, short_sg))
            else:
                logger.warning(
                    f"Error reading {table_name} table in {symbols_cpp} file."
                )
                return spacegroup_name
            short_symbol_dict = dict(symbol_pairs)
            spacegroup_name_full = space_group_symbols(
                spacegroup_name, "A"
            ).hermann_mauguin()
            if spacegroup_name_full in short_symbol_dict:
                return short_symbol_dict[spacegroup_name_full]
            else:
                return spacegroup_name


def get_heavy_atom_job(msg):
    """Try to guess heavy atom if it is missing from the input"""

    mtz_obj = mtz.object(msg.hklin)
    sg = mtz_obj.space_group()
    for crystal in mtz_obj.crystals():
        if crystal.name() == "HKL_base":
            continue
        uc = crystal.unit_cell()
        for dataset in crystal.datasets():
            msg.wavelength = dataset.wavelength()
            break
        break
    n_ops = len(sg.all_ops())
    v_asu = uc.volume() / n_ops
    mw = v_asu / 2.7
    try:
        msg.nres = len(msg.sequence)
    except TypeError:
        msg.nres = int(mw / 110)
        msg.sequence = "A" * msg.nres
        msg.compound = "Polyala"


def number_sites_estimate(cell, pointgroup):
    """Guess # heavy atoms likely to be in here (as a floating point number)
    based on Matthews coefficient, average proportion of methionine in
    protein sequences and typical mass of an amino acid."""

    sg = space_group(space_group_symbols(pointgroup).hall())
    uc = unit_cell(cell)

    n_ops = len(sg.all_ops())

    v_asu = uc.volume() / n_ops

    return max(1, int(round(0.023 * v_asu / (2.7 * 128))))


def read_data(msg):
    """Read and scale input data"""

    def __read_mtz_object(obj, anomalous=True):

        for ma in obj.as_miller_arrays():
            if anomalous and not ma.anomalous_flag():
                continue
            if str(ma.observation_type()) != "xray.intensity":
                continue
            return ma

    mtz_obj = mtz.object(msg.hklin)

    mtz_data = __read_mtz_object(mtz_obj)
    if not mtz_data:
        raise ValueError("No anomalous intensity data found in %s" % msg.hklin)

    msg.nrefl = mtz_obj.n_reflections()
    msg.pointgroup = mtz_data.space_group().type().number()
    msg.unit_cell = mtz_data.unit_cell().parameters()
    try:
        assert msg.nsites > 0
    except (AttributeError, AssertionError):
        msg.nsites = number_sites_estimate(msg.unit_cell, msg.pointgroup)


def read_mtz_datasets(msg, logger):
    """Assign dataset types based on wavelength values and list of input parameters."""

    datasets = []
    obj = mtz.object(msg.hklin)

    wv_list = []
    for (crst_id, crystal) in enumerate(obj.crystals()):
        if crystal.name() == "HKL_base":
            continue
        wv_list.extend(
            [
                (crst_id, ds_idx, ds.wavelength())
                for (ds_idx, ds) in enumerate(crystal.datasets())
            ]
        )

    wv_list.sort(key=lambda ds: ds[2], reverse=True)
    name_list = [
        name
        for name in ["lrem", "infl", "peak", "hrem"]
        if name in [ds["name"] for ds in msg.datasets]
    ]
    for (total_idx, ((crst_idx, ds_idx, wv), name)) in enumerate(
        zip(wv_list, name_list)
    ):
        dataset = obj.crystals()[crst_idx].datasets()[ds_idx]
        columns = [
            (col.label(), col.type())
            for col in dataset.columns()
            if col.type() in ["K", "M"]
        ]
        if not columns:
            columns = [
                (col.label(), col.type())
                for col in dataset.columns()
                if col.type() in ["G", "L"]
            ]

        tmp_dataset = {
            "mtz": os.path.basename(msg.hklin),
            "index": total_idx + 1,
            "name": name,
            "wavelength": wv,
            "column_list": columns,
            "columns": " ".join([label for (label, _) in columns]),
        }

        try:
            fp, fpp = next(
                ((ds["fp"], ds["fpp"]) for ds in msg.datasets if ds["name"] == name)
            )
        except Exception:
            fp, fpp = get_tabulated_fp_fpp(msg.atom, name, wv)

        tmp_dataset.update({"fp": fp, "fpp": fpp})

        itr_f, itr_d = tee(zip(dataset.columns()[:-1], dataset.columns()[1:]))
        try:
            fcol, sigfcol = next(
                (
                    (f.label(), sigf.label())
                    for f, sigf in itr_f
                    if (f.type() == "F" and sigf.type() == "Q")
                )
            )
            tmp_dataset.update({"F": fcol, "SIGF": sigfcol})
        except Exception:
            logger.warning("Warning: F/SIGF columns are missing")
        try:
            dcol, sigdcol = next(
                (
                    (d.label(), sigd.label())
                    for d, sigd in itr_d
                    if (d.type() == "D" and sigd.type() == "Q")
                )
            )
            tmp_dataset.update({"DANO": dcol, "SIGDANO": sigdcol})
        except Exception:
            logger.warning("Warning: DANO/SIGDANO columns are missing")

        try:
            isym = next((i.label() for i in dataset.columns() if i.type() == "Y"))
            tmp_dataset.update({"ISYM": isym})
        except Exception:
            logger.info("Warning: ISYM column is missing")

        datasets.append(tmp_dataset)

    msg.datasets = datasets


def write_settings_file(working_directory, msg):

    json_data = json.dumps(
        {
            "atom": msg.atom,
            "dataset": msg.dataset_names,
            "spacegroup": msg.spacegroup,
            "nsites": msg.nsites,
            "compound": msg.compound,
            "sequence": msg.sequence,
        },
        indent=4,
        separators=(",", ":"),
    )
    settings_path = working_directory / "big_ep_settings.json"
    with open(settings_path, "w") as json_file:
        json_file.write(json_data)

    return msg


def write_sequence_file(working_directory, msg):
    msg.seqin_filename = "sequence.fasta"
    seqin = working_directory / msg.seqin_filename

    with open(seqin, "w") as fp:
        # Workaround to get Crank2 to accept dummy poly-alanine sequence
        seq_line_len = 3 if msg.compound == "Polyala" else 80
        fp.write(
            fasta_sequence(sequence=msg.sequence, name=msg.compound).format(
                seq_line_len
            )
        )


def get_autosharp_model_files(working_directory, logger):

    parse_path = lambda v: str(Path(v.split("=")[1][1:-2]).resolve())

    try:
        with open(str(working_directory / ".autoSHARP"), "r") as f:
            lines = f.readlines()
            for mtz_line, pdb_line in zip(lines[:0:-1], lines[-2::-1]):
                if "autoSHARP_modelmtz=" in mtz_line and "autoSHARP_model=" in pdb_line:
                    # pdb_filename = parse_value(pdb_line).replace(
                    #    str(working_directory, "autoSHARP"), self.msg._wd
                    # )
                    # mtz_filename = parse_value(mtz_line).replace(
                    #    str(working_directory  "autoSHARP"), self.msg._wd
                    # )
                    mdl_dict = {
                        "pdb": parse_path(pdb_line),
                        "mtz": parse_path(mtz_line),
                        "pipeline": "autoSHARP",
                        "map": "",
                        "mapcc": 0.0,
                        "mapcc_dmin": 0.0,
                    }
                    if "parrot" in mdl_dict["mtz"]:
                        mdl_dict.update(
                            {
                                "fwt": "parrot.F_phi.F",
                                "phwt": "parrot.F_phi.phi",
                                "fom": None,
                            }
                        )
                    elif "wARP" in mdl_dict["mtz"]:
                        mdl_dict.update({"fwt": "FWT", "phwt": "PHWT", "fom": None})
                    else:
                        logger.info(
                            f"Skipping model generation. Unsupported mtz file type {mdl_dict['mtz']}"
                        )
                        return None
                    model_data = get_model_data(
                        str(working_directory), mdl_dict, logger
                    )
                    if model_data is None:
                        return

                    mdl_dict.update(model_data)
                    return mdl_dict
            logger.info("Cannot find record with autoSHARP output files")
            return None
    except OSError:
        logger.info("Cannot find .autoSHARP results file")
        return None


def get_autobuild_model_files(working_directory, logger):

    mdl_dict = {
        "pdb": str(working_directory / "AutoBuild_run_1_" / "overall_best.pdb"),
        "mtz": str(
            working_directory
            / "AutoBuild_run_1_"
            / "overall_best_denmod_map_coeffs.mtz"
        ),
        "pipeline": "AutoBuild",
        "fwt": "FWT",
        "phwt": "PHWT",
        "fom": None,
    }
    model_data = get_model_data(str(working_directory), mdl_dict, logger)
    if model_data is None:
        return

    mdl_dict.update(model_data)
    return mdl_dict


def get_crank2_model_files(working_directory, logger):

    ref_pth = working_directory / "crank2" / "5-comb_phdmmb" / "ref"
    dmfull_pth = working_directory / "crank2" / "5-comb_phdmmb" / "dmfull" / "ref"

    if os.path.isdir(ref_pth):
        mdl_dict = {
            "pdb": str(ref_pth / "sepsubstrprot" / "part.pdb"),
            "mtz": str(ref_pth / "refmac" / "REFMAC5.mtz"),
            "pipeline": "Crank2",
        }
    elif os.path.isdir(dmfull_pth):
        mdl_dict = {
            "pdb": str(dmfull_pth / "sepsubstrprot" / "part.pdb"),
            "mtz": str(dmfull_pth / "REFMAC5.mtz"),
            "pipeline": "Crank2",
        }
    else:
        return

    mdl_dict.update({"fwt": "REFM_FWT", "phwt": "REFM_PHWT", "fom": None})
    model_data = get_model_data(str(working_directory), mdl_dict, logger)
    if model_data is None:
        return

    mdl_dict.update(model_data)
    return mdl_dict


def get_map_model_from_json(json_path):
    abs_json_path = os.path.join(json_path, "big_ep_model_ispyb.json")
    with open(abs_json_path) as json_file:
        msg_json = json.load(json_file)
    return {
        "mtz": msg_json["mtz"],
        "pdb": msg_json["pdb"],
        "map": msg_json["map"],
        "data": {
            "residues": "{}".format(msg_json["total"]),
            "max_frag": "{}".format(msg_json["max"]),
            "frag": "{}".format(msg_json["fragments"]),
            "mapcc": "{:.2f} ({:.2f})".format(
                msg_json["mapcc"], msg_json["mapcc_dmin"]
            ),
        },
    }


def write_coot_script(working_directory, mdl_dict):
    coot_script = [
        "set_map_radius(20.0)",
        "set_dynamic_map_sampling_on()",
        "set_dynamic_map_size_display_on()",
    ]

    if os.path.isfile(mdl_dict["pdb"]):
        coot_script.append(
            'read_pdb("{}")'.format(os.path.relpath(mdl_dict["pdb"], working_directory))
        )
    if os.path.isfile(mdl_dict["map"]):
        coot_script.append(
            'handle_read_ccp4_map("{}", 0)'.format(
                os.path.relpath(mdl_dict["map"], working_directory)
            )
        )
    with open(os.path.join(working_directory, "models.py"), "w") as fp:
        fp.write(os.linesep.join(coot_script))
    with open(os.path.join(working_directory, "big_ep_coot.sh"), "w") as fp:
        fp.write(
            os.linesep.join(
                [
                    "#!/bin/sh",
                    "module purge",
                    "module load ccp4",
                    "coot --script models.py --no-guano",
                ]
            )
        )


def ispyb_write_model_json(working_directory, mdl_dict, logger):

    json_data = json.dumps(mdl_dict, indent=4, separators=(",", ":"))
    with open(
        os.path.join(working_directory, "big_ep_model_ispyb.json"), "w"
    ) as json_file:
        json_file.write(json_data)


def send_results_to_ispyb(results_directory, log_files, record_result, logger):
    try:
        mdl_dict = get_map_model_from_json(results_directory)
        for key in ["pdb", "map", "mtz"]:
            fp = mdl_dict[key]
            if os.path.isfile(fp):
                record_result(
                    {
                        "file_path": os.path.dirname(fp),
                        "file_name": os.path.basename(fp),
                        "file_type": "Result",
                        "importance_rank": 1,
                    }
                )
        record_result(
            {
                "file_path": str(results_directory),
                "file_name": "big_ep_model_ispyb.json",
                "file_type": "Result",
                "importance_rank": 2,
            }
        )
    except Exception:
        logger.warning(
            f"Cannot get model data file in {results_directory}", exc_info=True
        )

    for pipeline_logfile in log_files:
        if os.path.isfile(pipeline_logfile):
            try:
                record_result(
                    {
                        "file_path": os.path.dirname(pipeline_logfile),
                        "file_name": os.path.basename(pipeline_logfile),
                        "file_type": "log",
                        "importance_rank": 1,
                    }
                )
            except Exception:
                logger.warning(
                    f"Cannot write pipeline log file {pipeline_logfile}", exc_info=True
                )
