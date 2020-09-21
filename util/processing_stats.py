import itertools
import operator
import iotbx.pdb
from iotbx.gui_tools.reflections import map_coeffs_from_mtz_file

from mmtbx import monomer_library

import os
from os.path import basename, splitext
from cctbx import maptbx
import mmtbx.utils
import mmtbx.maps.correlation


def get_pdb_chain_stats(pdb_file, logger):
    """Return number of fragments, residues and the longest fragment length
    in the input pdb file."""

    with open(pdb_file) as pdb_f:
        pdb_lines = "".join(
            [
                l
                for l in pdb_f.readlines()
                if not ("DUM" in l or "HET" in l or "WAT" in l)
            ]
        )

    pdb_obj = iotbx.pdb.hierarchy.input(pdb_string=pdb_lines)

    all_chains = []
    aa_keys = iotbx.pdb.amino_acid_codes.one_letter_given_three_letter.keys()
    logger.info(f"aa_keys: {aa_keys}")
    for model in pdb_obj.hierarchy.models():
        for chain in model.chains():
            resids = list(
                itertools.chain.from_iterable(
                    [
                        [
                            int("".join(([a for a in rg.resid() if a.isdigit()])))
                            for ag in rg.atom_groups()
                            if (ag.resname in aa_keys)
                        ]
                        for rg in chain.residue_groups()
                    ]
                )
            )
            if resids:
                try:
                    chain_breaks = (
                        [
                            (0, 0),
                        ]
                        + [
                            (i, l)
                            for (i, l) in enumerate(
                                map(
                                    lambda x: operator.sub(*x),
                                    zip(resids[1:], resids[:-1]),
                                ),
                                1,
                            )
                            if l > 1
                        ]
                        + [
                            (len(resids), 0),
                        ]
                    )
                    chain_lens = [
                        operator.sub(x[0], y[0])
                        for (x, y) in zip(chain_breaks[1:], chain_breaks[:-1])
                    ]
                    all_chains.extend(chain_lens)
                except Exception:
                    logger.exception(f"Error in processing content of {pdb_file} file")
                    return
    return {
        "fragments": len(all_chains),
        "total": sum(all_chains),
        "max": max(all_chains) if all_chains else 0,
    }


def write_ispyb_maps(_wd, mdl_dict, logger):
    logger.info(f"mdl_dict {mdl_dict}")
    map_filename = "".join(
        [
            splitext(basename(mdl_dict["mtz"]))[0],
            mdl_dict["fwt"],
            mdl_dict["phwt"],
            mdl_dict["fom"] if mdl_dict["fom"] else "",
            ".map",
        ]
    )
    map_filepath = os.path.join(_wd, map_filename)

    try:
        map_coeffs = map_coeffs_from_mtz_file(
            mdl_dict["mtz"],
            f_label=",".join([mdl_dict["fwt"], mdl_dict["phwt"]]),
            phi_label=None,
            fom_label=None,
        )

        map_coeffs = map_coeffs.map_to_asu().average_bijvoet_mates()
        map_data = map_coeffs.fft_map(resolution_factor=1 / 3.0)
        map_data.apply_sigma_scaling()
        map_data.as_ccp4_map(file_name=map_filepath)

        mapcc, mapcc_dmin = calculate_mapcc(mdl_dict["pdb"], map_filepath, logger)

        return (map_filepath, mapcc, mapcc_dmin)
    except Exception:
        logger.info(f"Cannot generate {map_filepath} map file")
        return None


def calculate_mapcc(pdb_filepath, map_filepath, logger):
    try:
        mon_lib_srv = monomer_library.server.server()
        ener_lib = monomer_library.server.ener_lib()
        params = mmtbx.utils.process_command_line_args(
            args=(pdb_filepath, map_filepath), suppress_symmetry_related_errors=True
        )
        with open(pdb_filepath) as pdb_f:
            pdb_lines = "".join(
                [
                    l
                    for l in pdb_f.readlines()
                    if not ("DUM" in l or "HET" in l or "WAT" in l)
                ]
            )

        processed_pdb_file = monomer_library.pdb_interpretation.process(
            mon_lib_srv=mon_lib_srv,
            ener_lib=ener_lib,
            file_name=None,
            raw_records=pdb_lines,
        )
        xrs = processed_pdb_file.xray_structure()
        # xrs.scattering_type_registry(table = params.scattering_table)
        map_data = params.ccp4_map.data.as_double()

        mapcc_dmin = maptbx.resolution_from_map_and_model(
            map_data=map_data, xray_structure=xrs
        )
        corr = mmtbx.maps.correlation.from_map_and_xray_structure_or_fmodel(
            xray_structure=xrs, map_data=map_data, d_min=mapcc_dmin
        )
        mapcc = corr.cc()
        return (mapcc, mapcc_dmin)
    except Exception:
        logger.info(
            f"Cannot generate mapcc value for {map_filepath} and {pdb_filepath} files"
        )
        return (0.0, 0.0)
