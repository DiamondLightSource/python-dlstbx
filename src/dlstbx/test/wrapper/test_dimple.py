import dataclasses

from dlstbx.wrapper.dimple import get_blobs_from_anode_log


def test_get_blobs_from_anode_log(tmp_path):
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
    blobs = get_blobs_from_anode_log(anode_log)
    assert len(blobs) == 9
    assert dataclasses.asdict(blobs[0]) == {
        "xyz": (0.23127, 0.42256, 0.32278),
        "height": 13.5,
        "occupancy": 1.0,
        "nearest_atom": {
            "name": "SG",
            "chain_id": "B",
            "res_seq": "19",
            "res_name": "CYS",
        },
        "nearest_atom_distance": 0.03,
        "map_type": "anomalous",
    }
    assert dataclasses.asdict(blobs[8]) == {
        "xyz": (0.31861, 0.40817, 0.2759),
        "height": 4.2,
        "occupancy": 1.0,
        "nearest_atom": {
            "name": "CD2",
            "chain_id": "B",
            "res_seq": "16",
            "res_name": "TYR",
        },
        "nearest_atom_distance": 1.865,
        "map_type": "anomalous",
    }
