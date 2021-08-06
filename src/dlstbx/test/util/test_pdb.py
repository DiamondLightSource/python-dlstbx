from dlstbx.util.pdb import trim_pdb_bfactors


def test_trim_pdb_bfactors(tmp_path):
    pdb_in = tmp_path / "untrimmed.pdb"
    pdb_in.write_text(
        """\
ATOM      1  N   GLY A   1      17.285  -9.517 -12.914  1.00 22.52           N
ATOM      2  CA  GLY A   1      17.293  -9.183 -11.470  1.00 22.67           C
ATOM      3  C   GLY A   1      15.896  -9.378 -10.959  1.00 21.47           C
ATOM      4  O   GLY A   1      15.071  -9.879 -11.685  1.00 22.16           O
ATOM      5  N   ILE A   2      15.667  -9.033  -9.708  1.00 18.87           N
ATOM      6  CA  ILE A   2      14.331  -9.182  -9.114  1.00 17.77           C
ATOM      7  C   ILE A   2      13.203  -8.549  -9.976  1.00 18.19           C
ATOM      8  O   ILE A   2      12.162  -9.108 -10.053  1.00 17.34           O
ATOM      9  CB  ILE A   2      14.280  -8.630  -7.717  1.00 16.98           C
ATOM     10  CG1 ILE A   2      12.974  -9.086  -7.038  1.00 16.87           C
ATOM     11  CG2 ILE A   2      14.372  -7.128  -7.698  1.00 19.23           C
ATOM     12  CD1 ILE A   2      12.900  -8.587  -5.573  1.00 15.97           C
ATOM     13  N   VAL A   3      13.459  -7.408 -10.611  1.00 20.09           N
ATOM     14  CA  VAL A   3      12.403  -6.702 -11.325  1.00 19.49           C
ATOM     15  C   VAL A   3      12.066  -7.484 -12.556  1.00 19.91           C
ATOM     16  O   VAL A   3      10.902  -7.586 -12.959  1.00 20.54           O
ATOM     17  CB  VAL A   3      12.818  -5.187 -11.656  1.00 19.91           C
ATOM     18  CG1 VAL A   3      11.767  -4.535 -12.498  1.00 19.20           C
ATOM     19  CG2 VAL A   3      13.079  -4.440 -10.349  1.00 20.09           C
"""
    )
    pdb_out = tmp_path / "trimmed.pdb"
    trim_pdb_bfactors(
        str(pdb_in), str(pdb_out), atom_selection="bfactor < 20", set_b_iso=20
    )
    assert (
        pdb_out.read_text()
        == """\
ATOM      5  N   ILE A   2      15.667  -9.033  -9.708  1.00 20.00           N
ATOM      6  CA  ILE A   2      14.331  -9.182  -9.114  1.00 20.00           C
ATOM      7  C   ILE A   2      13.203  -8.549  -9.976  1.00 20.00           C
ATOM      8  O   ILE A   2      12.162  -9.108 -10.053  1.00 20.00           O
ATOM      9  CB  ILE A   2      14.280  -8.630  -7.717  1.00 20.00           C
ATOM     10  CG1 ILE A   2      12.974  -9.086  -7.038  1.00 20.00           C
ATOM     11  CG2 ILE A   2      14.372  -7.128  -7.698  1.00 20.00           C
ATOM     12  CD1 ILE A   2      12.900  -8.587  -5.573  1.00 20.00           C
ATOM     14  CA  VAL A   3      12.403  -6.702 -11.325  1.00 20.00           C
ATOM     15  C   VAL A   3      12.066  -7.484 -12.556  1.00 20.00           C
ATOM     17  CB  VAL A   3      12.818  -5.187 -11.656  1.00 20.00           C
ATOM     18  CG1 VAL A   3      11.767  -4.535 -12.498  1.00 20.00           C
TER
"""
    )
