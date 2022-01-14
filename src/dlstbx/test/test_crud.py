import datetime

from dlstbx import crud, schemas


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
