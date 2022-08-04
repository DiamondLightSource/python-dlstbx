from __future__ import annotations

import datetime

from dlstbx import crud, schemas


def test_get_data_collection(db_session):
    dcid = 993677
    dc = crud.get_data_collection(dcid, db_session)
    assert dc.dataCollectionId == dcid


def test_get_gridinfo_for_dcid(db_session):
    dcid = 6017405
    gi = crud.get_gridinfo_for_dcid(dcid, db_session)
    assert gi.dataCollectionId == dcid


def test_get_blsession_for_dcid(db_session):
    dcid = 993677
    bs = crud.get_blsession_for_dcid(dcid, db_session)
    assert bs.beamLineName == "i03"


def test_get_detector(db_session):
    det = crud.get_detector(4, db_session)
    assert det.detectorModel == "Excalibur"


def test_get_blsample(db_session):
    sample = crud.get_blsample(398827, db_session)
    assert sample.name == "XPDF-2"


def test_get_dcids_for_sample_id(db_session):
    assert crud.get_dcids_for_sample_id(398827, db_session) == [
        6017412,
        6017419,
        6017420,
        6017421,
        6017422,
        6017423,
        6017424,
        6017425,
        6017432,
        6017439,
        6017446,
        6017453,
        6017460,
        6017467,
        6017474,
        6017481,
        6017488,
        6017495,
        6017502,
        6017509,
        6017516,
        6017523,
        6017530,
        6017537,
        6017544,
        6017551,
        6017558,
        6017565,
        6017572,
        6017579,
        6017586,
    ]


def test_get_dcids_for_same_directory(db_session):
    assert crud.get_dcids_for_same_directory(1052494, db_session) == [1052503]


def test_get_dcids_for_data_collection_group(db_session):
    assert crud.get_dcids_for_data_collection_group(5441022, db_session) == [6017688]


def test_get_crystal_for_dcid(db_session):
    crystal = crud.get_crystal_for_dcid(6017586, db_session)
    assert crystal.spaceGroup == "P12121"


def test_get_protein_for_dcid(db_session):
    protein = crud.get_protein_for_dcid(6017405, db_session)
    assert protein.name == "therm"


def test_get_priority_processing_for_sample_id(db_session):
    assert crud.get_priority_processing_for_sample_id(398827, db_session) is None


def test_insert_dimple_results(db_session):
    mxmrrun = schemas.MXMRRun(
        auto_proc_scaling_id=596133,
        rwork_start=0.1847,
        rwork_end=0.1804,
        rfree_start=0.1858,
        rfree_end=0.1923,
    )

    blobs = [
        schemas.Blob(
            xyz=(0.1, 0.2, 0.3),
            height=10,
            map_type="difference",
        ),
        schemas.Blob(
            xyz=(0.1, 0.2, 0.3),
            height=10,
            map_type="anomalous",
            occupancy=1,
            nearest_atom=schemas.Atom(
                name="O",
                res_name="foo",
                res_seq=42,
                chain_id="A",
            ),
            nearest_atom_distance=1.5,
            filepath="/path/to/results",
            view1="anom-blob1.png",
            view2="anom-blob2.png",
            view3="anom-blob3.png",
        ),
    ]
    auto_proc_program = schemas.AutoProcProgram(
        command_line="dimple  /path/to/free.mtz /path/to/pdb2bn3.ent.gz /path/to/dimple --anode -fpng",
        programs="dimple",
        status=1,
        message="Blob scores: 93",
        start_time=datetime.datetime(2021, 12, 17, 9, 34, 48),
        end_time=datetime.datetime(2021, 12, 17, 9, 36, 26),
    )
    attachments = [
        schemas.Attachment(
            file_type="result",
            file_path="/path/to/results",
            file_name="final.mtz",
            timestamp=datetime.datetime(2021, 12, 17, 9, 36, 26),
            importance_rank=None,
        ),
        schemas.Attachment(
            file_type="log",
            file_path="/path/to/results",
            file_name="dimple.log",
            timestamp=datetime.datetime(2021, 12, 17, 9, 36, 26),
            importance_rank=None,
        ),
    ]

    db_mxmrrun = crud.insert_dimple_result(
        mxmrrun=mxmrrun,
        auto_proc_program=auto_proc_program,
        blobs=blobs,
        attachments=attachments,
        session=db_session,
    )
    assert db_mxmrrun.mxMRRunId
