import argparse
import pathlib

from dlstbx.util.pdb import trim_pdb_bfactors


def run(args=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("pdb_in", type=pathlib.Path)
    parser.add_argument("pdb_out", type=pathlib.Path)
    parser.add_argument("--atom_selection", type=str, default=None)
    parser.add_argument("--set_b_iso", type=float, default=None)
    args = parser.parse_args(args=args)
    trim_pdb_bfactors(
        str(args.pdb_in),
        str(args.pdb_out),
        atom_selection=args.atom_selection,
        set_b_iso=args.set_b_iso,
    )
