from __future__ import absolute_import, division, print_function

import logging
import json
import os
import py
import shutil
import sys

import procrunner

logger = logging.getLogger("dlstbx.align_crystal")


def _run_command(args):
    logger.info("command: %s", " ".join(args))
    result = procrunner.run_process(args, print_stdout=False, print_stderr=False)

    if result["exitcode"]:
        logger.info("exitcode: %s", result["exitcode"])
        logger.info(result["stdout"])
        logger.info(result["stderr"])
    else:
        logger.debug("time_start: %s", result["time_start"])
        logger.debug("time_end: %s", result["time_end"])
        logger.info("runtime: %s", result["runtime"])
    return result


def align_crystal(image_files, nproc=None):
    def _import(image_files):
        """Run dials.import"""
        datablock_json = "datablock.json"
        args = [
            "dials.import",
            "allow_multiple_sweeps=True",
            "output.datablock=%s" % datablock_json,
        ] + image_files
        result = _run_command(args)
        return datablock_json

    def _find_spots(datablock_json):
        """Run dials.find_spots"""
        strong_pickle = "strong.pickle"
        args = [
            "dials.find_spots",
            datablock_json,
            "output.reflections=%s" % strong_pickle,
        ]
        if nproc is not None:
            args.append("nproc=%s" % nproc)
        result = _run_command(args)
        return strong_pickle

    def _index(datablock_json, strong_pickle):
        """Run dials.index"""
        # could iteratively try different indexing options until one succeeds
        experiments_json = "indexed_experiments.json"
        indexed_pickle = "indexed.pickle"
        args = [
            "dials.index",
            datablock_json,
            strong_pickle,
            "output.experiments=%s" % experiments_json,
            "output.reflections=%s" % indexed_pickle,
        ]
        result = _run_command(args)
        success = not result["exitcode"] and not result["stderr"]
        return success, experiments_json, indexed_pickle

    def _reindex(indexed_pickle, cb_op):
        """Reindex the indexed reflections using the given cb_op"""
        args = ["dials.reindex", indexed_pickle, 'change_of_basis_op="%s"' % cb_op]
        result = _run_command(args)
        return result

    def _lattice(experiments_json, indexed_pickle):
        """Run dials.refine_bravais_settings"""
        args = ["dials.refine_bravais_settings", experiments_json, indexed_pickle]
        result = _run_command(args)
        assert os.path.exists("bravais_summary.json")
        with open("bravais_summary.json", "rb") as f:
            d = json.load(f)
            solutions = dict((int(k), v) for k, v in d.items())
            for k in solutions.keys():
                solutions[k]["experiments_file"] = "bravais_setting_%d.json" % k
            soln = solutions[max(solutions.keys())]
        experiments_json = "reindexed_experiments.json"
        shutil.copyfile(soln["experiments_file"], experiments_json)
        reflections_pickle = "reindexed_reflections.pickle"
        _reindex("indexed.pickle", soln["cb_op"])
        return experiments_json, reflections_pickle

    def _align_crystal(experiments_json):
        args = ["dials.align_crystal", experiments_json]
        result = _run_command(args)
        return result

    datablock = _import(image_files)
    reflections = _find_spots(datablock)
    success, experiments, reflections = _index(datablock, reflections)
    if not success:
        return False
    experiments, reflections = _lattice(experiments, reflections)
    _align_crystal(experiments)
    return True


def run(args):
    # setup logging
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    fh = logging.FileHandler("dlstbx.align_crystal.log")
    fh.setLevel(logging.INFO)
    logger.setLevel(logging.INFO)
    logger.addHandler(ch)
    logger.addHandler(fh)

    image_files = [f for f in args if os.path.isfile(f)]
    assert image_files
    if not align_crystal(image_files):
        sys.exit("crystal alignment failed")


if __name__ == "__main__":
    run(sys.argv[1:])
