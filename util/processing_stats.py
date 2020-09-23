import os
from os.path import basename, splitext
import itertools
import operator

import iotbx.ccp4_map
import iotbx.pdb
from iotbx.gui_tools.reflections import map_coeffs_from_mtz_file
import cctbx.maptbx.resolution_from_map_and_model
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
    map_filename = "".join(
        [
            splitext(basename(mdl_dict["mtz"]))[0],
            mdl_dict["fwt"],
            mdl_dict["phwt"],
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
        logger.warning(f"Cannot generate {map_filepath} map file")
        return None


def calculate_mapcc(pdb_filepath, map_filepath, logger):
    try:
        with open(pdb_filepath) as pdb_f:
            pdb_lines = "".join(
                [
                    l
                    for l in pdb_f.readlines()
                    if not ("DUM" in l or "HET" in l or "WAT" in l)
                ]
            )

        m = iotbx.ccp4_map.map_reader(file_name=map_filepath)
        map_data = m.data.as_double()

        pdb_inp = iotbx.pdb.input(source_info=None, lines=pdb_lines)
        cs = pdb_inp.crystal_symmetry()
        ph = pdb_inp.construct_hierarchy()
        xrs = ph.extract_xray_structure(crystal_symmetry=cs)
        mapcc_dmin = cctbx.maptbx.resolution_from_map_and_model.run(
            map_data=map_data, xray_structure=xrs
        )
        corr = mmtbx.maps.correlation.from_map_and_xray_structure_or_fmodel(
            xray_structure=xrs, map_data=map_data, d_min=mapcc_dmin.d_min
        )
        mapcc = corr.cc()
        return (mapcc, mapcc_dmin.d_min)
    except Exception:
        logger.info(
            f"Cannot generate mapcc value for {map_filepath} and {pdb_filepath} files"
        )
        return (0.0, 0.0)
