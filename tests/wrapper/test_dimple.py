from __future__ import annotations

import gemmi
import pytest
from dlstbx.wrapper.dimple import (
    get_blobs_from_anode_log,
    get_blobs_from_find_blobs_log,
)


def test_get_blobs_from_anode_log(tmp_path):
    cell = gemmi.UnitCell(67.89, 67.89, 102.08, 90, 90, 90)
    anode_log = tmp_path / "anode.log"
    anode_log.write_text(
        """
 Strongest unique anomalous peaks

          X        Y        Z   Height(sig)  SOF     Nearest atom

 S1    0.23127  0.42256  0.32278   13.50    1.000    0.030  SG_B:CYS19
 S2    0.25704  0.51956  0.41747   10.99    1.000    0.126  SG_A:CYS6
 S3    0.27768  0.49993  0.41883   10.89    1.000    0.064  SG_A:CYS11
 S4    0.21474  0.40945  0.33748   10.46    1.000    0.091  SG_A:CYS20
 S5    0.33665  0.60248  0.38886    8.07    1.000    0.042  SG_B:CYS7
 S6    0.30964  0.61579  0.38795    6.90    1.000    0.315  SG_A:CYS7
 S7    0.25794  0.25795  0.25793    4.33    0.333    3.612  OE1_B:GLU21
 S8    0.05565  0.52248  0.37525    4.30    1.000    5.564  O_A:HOH2013
 S9    0.31861  0.40817  0.27590    4.20    1.000    1.865  CD2_B:TYR16
"""
    )
    blobs = get_blobs_from_anode_log(anode_log, cell)
    assert len(blobs) == 9
    assert blobs[0].dict() == {
        "filepath": None,
        "xyz": pytest.approx((15.7009203, 28.6875984, 32.9493824)),
        "height": 13.5,
        "occupancy": 1.0,
        "nearest_atom": {
            "name": "SG",
            "chain_id": "B",
            "res_seq": 19,
            "res_name": "CYS",
        },
        "nearest_atom_distance": 0.03,
        "map_type": "anomalous",
        "view1": None,
        "view2": None,
        "view3": None,
    }
    assert blobs[8].dict() == {
        "filepath": None,
        "xyz": pytest.approx((21.6304329, 27.7106613, 28.163872)),
        "height": 4.2,
        "occupancy": 1.0,
        "nearest_atom": {
            "name": "CD2",
            "chain_id": "B",
            "res_seq": 16,
            "res_name": "TYR",
        },
        "nearest_atom_distance": 1.865,
        "map_type": "anomalous",
        "view1": None,
        "view2": None,
        "view3": None,
    }


def test_get_blobs_from_find_blobs_log(tmp_path):
    find_blobs_log = tmp_path / "find-blobs.log"
    find_blobs_log.write_text(
        """
Searching for clusters in density map, using grid: Nuvw = ( 180, 180,  80)
Density std.dev: 0.498, cut-off: 0.399 e/A^3 (0.8 sigma)
Protein mass center: xyz = (   -0.5181,     20.49,     18.82)
4 clusters (with given criteria) found
#0    144 grid points, score 104.9     (   3.62,  22.63,  24.25)
#1    147 grid points, score 87.42     (  -9.39,  36.17,  11.30)
#2    132 grid points, score 79.52     (  -2.99,  29.73,   4.09)
#3    126 grid points, score 75.82     (   6.06,  27.58,   6.56)
"""
    )
    blobs = get_blobs_from_find_blobs_log(find_blobs_log)
    assert len(blobs) == 4
    assert blobs[0].dict() == {
        "filepath": None,
        "xyz": (3.62, 22.63, 24.25),
        "height": 104.9,
        "map_type": "difference",
        "occupancy": None,
        "nearest_atom": None,
        "nearest_atom_distance": None,
        "view1": None,
        "view2": None,
        "view3": None,
    }
