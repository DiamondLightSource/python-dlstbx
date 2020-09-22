import os
import shutil

from cctbx.eltbx import sasaki, henke
from iotbx import mtz
from iotbx.bioinformatics import fasta_sequence
import libtbx.load_env
from cctbx.sgtbx import space_group_symbols

import json
from itertools import tee
import py
import subprocess
from dlstbx.util.processing_stats import get_pdb_chain_stats, write_ispyb_maps


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
            "Cannot generate short space group symbol. %s file not available."
            % symbols_cpp
        )
        return spacegroup_name
    else:
        table_name = "vol_a_short_mono_hm_dict"
        with open(symbols_cpp, "r") as f:
            for line in f:
                if line.find(table_name) > 0:
                    break
            else:
                logger.warning(
                    "Cannot find %s table in %s file." % (table_name, symbols_cpp)
                )
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
                    "Error reading %s table in %s file." % (table_name, symbols_cpp)
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

    msg._wd = os.path.join(
        msg._wd, "_".join([msg.atom, str(msg.nsites), msg.spacegroup])
    )
    msg._results_wd = os.path.join(
        msg._results_wd, "_".join([msg.atom, str(msg.nsites), msg.spacegroup])
    )
    msg._root_wd = msg._wd
    if not os.path.exists(msg._wd):
        os.makedirs(msg._wd)

    return msg


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
        raise ValueError("No anomalous intensity data found in %s" % msg._hklin)

    msg.nrefl = mtz_obj.n_reflections()
    msg.pointgroup = mtz_data.space_group().type().number()
    msg.unit_cell = mtz_data.unit_cell().parameters()

    return msg


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
            logger.warning("Warning: ISYM column is missing")

        datasets.append(tmp_dataset)

    msg.datasets = datasets
    return msg


def write_settings_file(msg):

    json_data = json.dumps(
        {
            "atom": msg.atom,
            "dataset": "|".join([ds["name"] for ds in msg.datasets]),
            "spacegroup": msg.spacegroup,
            "nsites": msg.nsites,
            "compound": "Protein",
            "sequence": msg.sequence,
        },
        indent=4,
        separators=(",", ":"),
    )
    with open(os.path.join(msg._wd, "big_ep_settings.json"), "w") as json_file:
        json_file.write(json_data)

    return msg


def setup_autosharp_jobs(msg, logger):
    """Setup input directory to run autoSHARP."""

    msg._wd = os.path.join(msg._wd, "autoSHARP")
    msg._results_wd = os.path.join(msg._results_wd, "autoSHARP")
    if not os.path.exists(msg._wd):
        os.makedirs(msg._wd)

    write_sequence_file(msg)

    if hasattr(msg, "spacegroup"):
        msg.spacegroup = spacegroup_short(msg.spacegroup, logger)

    shutil.copyfile(msg.hklin, os.path.join(msg._wd, os.path.basename(msg.hklin)))

    return msg


def get_autosharp_model_files(msg, logger):

    parse_value = lambda v: v.split("=")[1][1:-2]

    try:
        with open(os.path.join(msg._wd, ".autoSHARP"), "r") as f:
            lines = f.readlines()
            for mtz_line, pdb_line in zip(lines[:0:-1], lines[-2::-1]):
                if "autoSHARP_modelmtz=" in mtz_line and "autoSHARP_model=" in pdb_line:
                    try:
                        mdl_dict = {
                            "pdb": parse_value(pdb_line),
                            "mtz": parse_value(mtz_line),
                            "pipeline": "autoSHARP",
                        }
                        if "LJS" in os.path.basename(mdl_dict["mtz"]):
                            mdl_dict.update(
                                {
                                    "fwt": "parrot.F_phi.F",
                                    "phwt": "parrot.F_phi.phi",
                                    "fom": None,
                                }
                            )
                        else:
                            mdl_dict.update({"fwt": "FWT", "phwt": "PHWT", "fom": None})

                        mdl_dict.update(get_pdb_chain_stats(mdl_dict["pdb"], logger))

                        (map_filename, mapcc, mapcc_dmin) = write_ispyb_maps(
                            msg._wd, mdl_dict, logger
                        )
                        if map_filename:
                            mdl_dict["map"] = map_filename
                            mdl_dict["mapcc"] = mapcc
                            mdl_dict["mapcc_dmin"] = mapcc_dmin

                        msg.model = mdl_dict
                        ispyb_write_model_json(msg, logger)
                        return msg
                    except Exception:
                        logger.exception("autoSHARP results parsing error")
                        continue
            return None
    except IOError:
        return None


def write_sequence_file(msg):
    msg.seqin_filename = "sequence.fasta"
    msg.seqin = os.path.join(msg._wd, msg.seqin_filename)

    with open(msg.seqin, "w") as fp:
        fp.write(fasta_sequence(msg.sequence).format(80))


def setup_autosol_jobs(msg):
    """Setup working directory for running Phenix AutoSol pipeline"""

    msg._wd = os.path.join(msg._wd, "AutoSol")
    msg._results_wd = os.path.join(msg._results_wd, "AutoSol")
    if not os.path.exists(msg._wd):
        os.makedirs(msg._wd)

    write_sequence_file(msg)

    msg.autosol_hklin = os.path.join(msg._wd, os.path.basename(msg.hklin))
    shutil.copyfile(msg.hklin, msg.autosol_hklin)
    return msg


def get_autobuild_model_files(msg, logger):

    mdl_dict = {
        "pdb": os.path.join(msg._wd, "AutoBuild_run_1_", "overall_best.pdb"),
        "mtz": os.path.join(
            msg._wd, "AutoBuild_run_1_", "overall_best_denmod_map_coeffs.mtz"
        ),
        "pipeline": "AutoBuild",
        "fwt": "FWT",
        "phwt": "PHWT",
        "fom": None,
    }
    try:
        mdl_dict.update(get_pdb_chain_stats(mdl_dict["pdb"], logger))

        (map_filename, mapcc, mapcc_dmin) = write_ispyb_maps(msg._wd, mdl_dict, logger)
        if map_filename:
            mdl_dict["map"] = map_filename
            mdl_dict["mapcc"] = mapcc
            mdl_dict["mapcc_dmin"] = mapcc_dmin

        msg.model = mdl_dict
        ispyb_write_model_json(msg, logger)
    except Exception:
        logger.info("Cannot process AutoBuild results files")
    return msg


def setup_pointless_jobs(msg):
    """Update spacegroup in the input mtz file"""

    msg._wd = os.path.join(msg._root_wd, "pointless")
    if not os.path.exists(msg._wd):
        os.makedirs(msg._wd)

    msg.input_hkl = msg.hklin
    (_, filext) = os.path.split(msg.hklin)
    (filename, ext) = os.path.splitext(filext)
    msg.hklin = os.path.join(msg._wd, "".join([filename, msg.spacegroup, ext]))

    return msg


def setup_crank2_jobs(msg):
    """Setup directory to run Crank2 pipeline"""

    msg._wd = os.path.join(msg._root_wd, "crank2")
    msg._results_wd = os.path.join(msg._results_wd, "crank2")
    if not os.path.exists(msg._wd):
        os.makedirs(msg._wd)

    msg.enableArpWarp = False  # (msg.data.d_min() < 2.5)

    write_sequence_file(msg)

    return msg


def get_crank2_model_files(msg, logger):

    ref_pth = os.path.join(msg._wd, "crank2", "5-comb_phdmmb", "ref")
    dmfull_pth = os.path.join(msg._wd, "crank2", "5-comb_phdmmb", "dmfull", "ref")
    if os.path.isdir(ref_pth):
        mdl_dict = {
            "pdb": os.path.join(ref_pth, "sepsubstrprot", "part.pdb"),
            "mtz": os.path.join(ref_pth, "refmac", "REFMAC5.mtz"),
            "pipeline": "Crank2",
        }
    elif os.path.isdir(dmfull_pth):
        mdl_dict = {
            "pdb": os.path.join(dmfull_pth, "sepsubstrprot", "part.pdb"),
            "mtz": os.path.join(dmfull_pth, "REFMAC5.mtz"),
            "pipeline": "Crank2",
        }
    else:
        return

    mdl_dict.update({"fwt": "REFM_FWT", "phwt": "REFM_PHWT", "fom": None})
    try:
        mdl_dict.update(get_pdb_chain_stats(mdl_dict["pdb"], logger))

        (map_filename, mapcc, mapcc_dmin) = write_ispyb_maps(msg._wd, mdl_dict, logger)
        if map_filename:
            mdl_dict["map"] = map_filename
            mdl_dict["mapcc"] = mapcc
            mdl_dict["mapcc_dmin"] = mapcc_dmin

        msg.model = mdl_dict
        ispyb_write_model_json(msg, logger)

    except Exception:
        logger.info("Cannot process crank2 results files")

    return msg


def get_map_model_from_json(json_path, logger):
    try:
        abs_json_path = os.path.join(json_path, "big_ep_model_ispyb.json")
        result = {"json": abs_json_path}
        with open(abs_json_path, "r") as json_file:
            msg_json = json.load(json_file)
        result.update({k: msg_json[k] for k in ["pdb", "map", "mtz"]})
        return result
    except Exception:
        logger.debug(
            "Couldn't read map/model data from %s", abs_json_path, exc_info=True
        )


def write_coot_script(working_directory, logger):
    coot_script = [
        "set_map_radius(20.0)",
        "set_dynamic_map_sampling_on()",
        "set_dynamic_map_size_display_on()",
    ]

    try:
        fp = get_map_model_from_json(working_directory)
    except Exception:
        logger.warning("Cannot read big_ep summary json file")
    if os.path.isfile(fp["pdb"]):
        coot_script.append(
            'read_pdb("{}")'.format(os.path.relpath(fp["pdb"], working_directory))
        )
    if os.path.isfile(fp["map"]):
        coot_script.append(
            'handle_read_ccp4_map("{}", 0)'.format(
                os.path.relpath(fp["map"], working_directory)
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
                    "coot --python models.py --no-guano",
                ]
            )
        )


def ispyb_write_model_json(msg, logger):

    json_data = json.dumps(msg.model, indent=4, separators=(",", ":"))
    with open(os.path.join(msg._wd, "big_ep_model_ispyb.json"), "w") as json_file:
        json_file.write(json_data)
    try:
        if os.path.isfile(msg.synchweb_ticks):
            fp = open(msg.synchweb_ticks, "a")
        else:
            fp = open(msg.synchweb_ticks, "w")
            fp.write("Legacy log file to update ap_status in SynchWeb\n")
        fp.write("Results for Residues")
        fp.write(json_data)
        fp.close()
    except IOError:
        logger.exception("Error creating legacy log file for SynchWeb")
    write_coot_script(msg._wd)


def copy_results(working_directory, results_directory, logger):
    def ignore_func(directory, files):
        ignore_list = [".launch", ".recipewrap"]
        pth = py.path.local(directory)
        for f in files:
            fp = pth.join(f)
            if not fp.check():
                ignore_list.append(f)
        return ignore_list

    shutil.copytree(working_directory, results_directory, ignore=ignore_func)
    src_pth_esc = r"\/".join(working_directory.split(os.sep))
    dest_pth_esc = r"\/".join(results_directory.split(os.sep))
    sed_command = (
        r"find %s -type f -exec grep -Iq . {} \; -and -exec sed -i 's/%s/%s/g' {} +"
        % (results_directory, src_pth_esc, dest_pth_esc)
    )
    try:
        subprocess.call([sed_command], shell=True)
    except Exception:
        logger.debug("Failed to run sed command to update paths", exc_info=True)


def send_results_to_ispyb(results_directory, record_result, logger):
    result = False
    try:
        f = get_map_model_from_json(results_directory, logger)
        for fp in f.values():
            if os.path.isfile(fp):
                record_result(
                    {
                        "file_path": os.path.dirname(fp),
                        "file_name": os.path.basename(fp),
                        "file_type": "Result",
                    }
                )
                result = True
    except Exception:
        pass

    log_files = [
        "LISTautoSHARP.html",
        "phenix_autosol.log",
        "phenix_autobuild.log",
        "crank2.log",
    ]
    for fp in log_files:
        pipeline_logfile = os.path.join(results_directory, fp)
        if os.path.isfile(pipeline_logfile):
            record_result(
                {"file_path": results_directory, "file_name": fp, "file_type": "log"}
            )
    return result
