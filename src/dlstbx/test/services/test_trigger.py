import datetime
from unittest import mock

import pytest
import workflows.transport.common_transport
from ispyb.sqlalchemy import (
    AutoProcIntegration,
    AutoProcProgram,
    AutoProcProgramAttachment,
    DataCollection,
    DataCollectionGroup,
    ProcessingJob,
    ProcessingJobParameter,
    Protein,
)
from workflows.recipe.wrapper import RecipeWrapper

from dlstbx.services.trigger import DLSTrigger


@pytest.fixture
def insert_multiplex_input(db_session):
    dcs = []
    for i in range(3):
        dcg = DataCollectionGroup(sessionId=55167)
        dc = DataCollection(
            DataCollectionGroup=dcg,
            wavelength=1.03936,
            startImageNumber=1,
            numberOfImages=25,
        )
        dcs.append(dc)
        db_session.add_all([dcg, dc])
        for sg in (None, "P422"):
            pj = ProcessingJob(
                DataCollection=dc,
                automatic=True,
            )
            pjps = []
            if sg:
                pjps = [
                    ProcessingJobParameter(
                        ProcessingJob=pj,
                        parameterKey="spacegroup",
                        parameterValue=sg,
                    )
                ]
            app = AutoProcProgram(
                ProcessingJob=pj,
                processingStatus=1,
                processingStartTime=datetime.datetime.now(),
                processingPrograms="xia2 dials",
            )
            api = AutoProcIntegration(DataCollection=dc, AutoProcProgram=app)
            db_session.add_all([api, app, pj] + pjps)
            for ext in ("expt", "refl"):
                db_session.add(
                    AutoProcProgramAttachment(
                        AutoProcProgram=app,
                        filePath=f"/path/to/xia2-dials-{i}{('-' + sg) if sg else ''}",
                        fileName=f"integrated.{ext}",
                    )
                )
    db_session.commit()
    return [dc.dataCollectionId for dc in dcs]


@pytest.mark.parametrize("spacegroup", [None, "P422"])
def test_multiplex(
    insert_multiplex_input,
    db_session_factory,
    testconfig,
    testdb,
    mocker,
    monkeypatch,
    spacegroup,
):
    monkeypatch.setenv("ISPYB_CREDENTIALS", testconfig)
    dcids = insert_multiplex_input
    message = {
        "recipe": {
            "1": {
                "parameters": {
                    "target": "multiplex",
                    "dcid": dcids[-1],
                    "wavelength": "1.03936",
                    "comment": "xia2.multiplex triggered by automatic xia2-dials",
                    "automatic": True,
                    "ispyb_parameters": {"spacegroup": spacegroup}
                    if spacegroup
                    else {},
                    "related_dcids": [
                        {
                            "dcids": dcids[:-1],
                            "sample_group_id": 123,
                            "name": "sample_group_123",
                        },
                        {
                            "dcids": dcids[1:-1],
                            "sample_id": 234,
                            "name": "sample_234",
                        },
                    ],
                    "backoff-delay": 8,
                    "backoff-max-try": 10,
                    "backoff-multiplier": 2,
                },
            },
        },
        "recipe-pointer": 1,
    }
    trigger = DLSTrigger()
    trigger._ispyb_sessionmaker = db_session_factory
    t = mock.create_autospec(workflows.transport.common_transport.CommonTransport)
    rw = RecipeWrapper(message=message, transport=t)
    trigger.ispyb = testdb
    send = mocker.spy(rw, "send")
    trigger.trigger(rw, {"some": "header"}, message)
    send.assert_called_once_with(
        {"result": [mocker.ANY, mocker.ANY]}, transaction=mocker.ANY
    )
    kall = send.mock_calls[0]
    name, args, kwargs = kall
    pjid = args[0]["result"][0]
    with db_session_factory() as db_session:
        pj = (
            db_session.query(ProcessingJob)
            .filter(ProcessingJob.processingJobId == pjid)
            .one()
        )
        assert pj.displayName == "xia2.multiplex"
        assert pj.recipe == "postprocessing-xia2-multiplex"
        assert pj.dataCollectionId == dcids[-1]
        assert pj.automatic
        params = {
            (pjp.parameterKey, pjp.parameterValue) for pjp in pj.ProcessingJobParameters
        }
        sg_extra = ("-" + spacegroup) if spacegroup else ""
        assert (
            params
            == {
                (
                    "data",
                    f"/path/to/xia2-dials-2{sg_extra}/integrated.expt;/path/to/xia2-dials-2{sg_extra}/integrated.refl",
                ),
                (
                    "data",
                    f"/path/to/xia2-dials-1{sg_extra}/integrated.expt;/path/to/xia2-dials-1{sg_extra}/integrated.refl",
                ),
                (
                    "data",
                    f"/path/to/xia2-dials-0{sg_extra}/integrated.expt;/path/to/xia2-dials-0{sg_extra}/integrated.refl",
                ),
                ("sample_group_id", "123"),
            }
            | ({("spacegroup", spacegroup)} if spacegroup else set())
        )


@pytest.fixture
def insert_dimple_input(db_session):
    dcg = DataCollectionGroup(sessionId=55167)
    dc = DataCollection(
        DataCollectionGroup=dcg,
        BLSAMPLEID=398827,
        startImageNumber=1,
        numberOfImages=180,
    )
    db_session.add_all([dc, dcg])
    db_session.commit()
    return dc.dataCollectionId


def test_dimple_trigger(
    insert_dimple_input,
    db_session_factory,
    testconfig,
    testdb,
    mocker,
    tmp_path,
    monkeypatch,
):
    monkeypatch.setenv("ISPYB_CREDENTIALS", testconfig)
    dcid = insert_dimple_input
    user_pdb_directory = tmp_path / "user_pdb"
    user_pdb_directory.mkdir()
    (user_pdb_directory / "test.pdb").touch()
    message = {
        "recipe": {
            "1": {
                "service": "DLS Trigger",
                "queue": "trigger",
                "parameters": {
                    "target": "dimple",
                    "dcid": dcid,
                    "comment": "DIMPLE triggered by automatic xia2-dials",
                    "automatic": True,
                    "scaling_id": 123456,
                    "user_pdb_directory": user_pdb_directory,
                    "mtz": "/path/to/xia2-dials/DataFiles/nt28218v3_xProtk11_free.mtz",
                    "pdb_tmpdir": tmp_path,
                },
            },
        },
        "recipe-pointer": 1,
    }
    trigger = DLSTrigger()
    trigger._ispyb_sessionmaker = db_session_factory
    t = mock.create_autospec(workflows.transport.common_transport.CommonTransport)
    rw = RecipeWrapper(message=message, transport=t)
    trigger.ispyb = testdb
    send = mocker.spy(rw, "send")
    trigger.trigger(rw, {"some": "header"}, message)
    send.assert_called_once_with({"result": mocker.ANY}, transaction=mocker.ANY)
    kall = send.mock_calls[0]
    name, args, kwargs = kall
    pjid = args[0]["result"]
    with db_session_factory() as db_session:
        pj = (
            db_session.query(ProcessingJob)
            .filter(ProcessingJob.processingJobId == pjid)
            .one()
        )
        assert pj.displayName == "DIMPLE"
        assert pj.recipe == "postprocessing-dimple"
        assert pj.dataCollectionId == dcid
        assert pj.automatic
        params = {
            (pjp.parameterKey, pjp.parameterValue) for pjp in pj.ProcessingJobParameters
        }
        assert params == {
            ("data", "/path/to/xia2-dials/DataFiles/nt28218v3_xProtk11_free.mtz"),
            ("scaling_id", "123456"),
            ("pdb", f"{tmp_path}/fe8c759005fb57ce14d3e66c07b21fec62252b4a/ceo2"),
            ("pdb", f"{user_pdb_directory}/test.pdb"),
        }


def test_ep_predict(db_session_factory, testconfig, testdb, mocker, monkeypatch):
    monkeypatch.setenv("ISPYB_CREDENTIALS", testconfig)
    dcid = 993677
    message = {
        "recipe": {
            "1": {
                "parameters": {
                    "target": "ep_predict",
                    "dcid": dcid,
                    "comment": "ep_predict triggered by automatic xia2-dials",
                    "automatic": True,
                    "diffraction_plan_info": {
                        "diffractionplanid": 2369980,
                        "anomalousScatterer": "S",
                    },
                    "program": "xia2 dials",
                    "program_id": 123456,
                    "data": "/path/to/xia2-dials/xia2.json",
                    "threshold": 0.8,
                }
            }
        },
        "recipe-pointer": 1,
    }
    trigger = DLSTrigger()
    trigger._ispyb_sessionmaker = db_session_factory
    t = mock.create_autospec(workflows.transport.common_transport.CommonTransport)
    rw = RecipeWrapper(message=message, transport=t)
    trigger.ispyb = testdb
    send = mocker.spy(rw, "send")
    trigger.trigger(rw, {"some": "header"}, message)
    send.assert_called_once_with({"result": mocker.ANY}, transaction=mocker.ANY)
    kall = send.mock_calls[0]
    name, args, kwargs = kall
    pjid = args[0]["result"]
    with db_session_factory() as db_session:
        pj = (
            db_session.query(ProcessingJob)
            .filter(ProcessingJob.processingJobId == pjid)
            .one()
        )
        assert pj.displayName == "ep_predict"
        assert pj.recipe == "postprocessing-ep-predict"
        assert pj.dataCollectionId == dcid
        assert pj.automatic
        params = {
            (pjp.parameterKey, pjp.parameterValue) for pjp in pj.ProcessingJobParameters
        }
        assert params == {
            ("threshold", "0.8"),
            ("data", "/path/to/xia2-dials/xia2.json"),
            ("program", "xia2 dials"),
            ("program_id", "123456"),
        }


def test_fast_ep(db_session_factory, testconfig, testdb, mocker, monkeypatch):
    monkeypatch.setenv("ISPYB_CREDENTIALS", testconfig)
    dcid = 993677
    message = {
        "recipe": {
            "1": {
                "service": "DLS Trigger",
                "queue": "trigger",
                "parameters": {
                    "target": "fast_ep",
                    "dcid": dcid,
                    "comment": "FastEP triggered by automatic FastDP",
                    "automatic": True,
                    "diffraction_plan_info": {
                        "diffractionplanid": 2021731,
                        "radiationsensitivity": 0.0,
                        "anomalousScatterer": "S",
                    },
                    "scaling_id": "123456",
                    "mtz": "/path/to/fast_dp/fast_dp.mtz",
                },
            }
        },
        "recipe-pointer": 1,
    }
    trigger = DLSTrigger()
    trigger._ispyb_sessionmaker = db_session_factory
    t = mock.create_autospec(workflows.transport.common_transport.CommonTransport)
    rw = RecipeWrapper(message=message, transport=t)
    trigger.ispyb = testdb
    send = mocker.spy(rw, "send")
    trigger.trigger(rw, {"some": "header"}, message)
    send.assert_called_once_with({"result": mocker.ANY}, transaction=mocker.ANY)
    kall = send.mock_calls[0]
    name, args, kwargs = kall
    pjid = args[0]["result"]
    with db_session_factory() as db_session:
        pj = (
            db_session.query(ProcessingJob)
            .filter(ProcessingJob.processingJobId == pjid)
            .one()
        )
        assert pj.displayName == "fast_ep"
        assert pj.recipe == "postprocessing-fast-ep"
        assert pj.dataCollectionId == dcid
        assert pj.automatic
        params = {
            (pjp.parameterKey, pjp.parameterValue) for pjp in pj.ProcessingJobParameters
        }
        assert params == {
            ("check_go_fast_ep", "1"),
            ("data", "/path/to/fast_dp/fast_dp.mtz"),
            ("scaling_id", "123456"),
        }


def test_big_ep(db_session_factory, testconfig, testdb, mocker, monkeypatch):
    monkeypatch.setenv("ISPYB_CREDENTIALS", testconfig)
    dcid = 1002287
    message = {
        "recipe": {
            "1": {
                "service": "DLS Trigger",
                "queue": "trigger",
                "parameters": {
                    "target": "big_ep",
                    "dcid": dcid,
                    "comment": "big_ep triggered by automatic xia2-dials",
                    "automatic": True,
                    "program_id": 56986673,
                    "diffraction_plan_info": {
                        "diffractionplanid": 2021731,
                        "radiationsensitivity": 0.0,
                        "anomalousScatterer": "S",
                    },
                    "xia2 dials": {
                        "data": "/path/to/xia2-dials/DataFiles/nt28218v3_xins24_free.mtz",
                        "scaled_unmerged_mtz": "/path/to/xia2-dials/DataFiles/nt28218v3_xins24_scaled_unmerged.mtz",
                        "path_ext": "xia2/dials-run",
                    },
                },
            },
        },
        "recipe-pointer": 1,
    }
    trigger = DLSTrigger()
    trigger._ispyb_sessionmaker = db_session_factory
    t = mock.create_autospec(workflows.transport.common_transport.CommonTransport)
    rw = RecipeWrapper(message=message, transport=t)
    trigger.ispyb = testdb
    send = mocker.spy(rw, "send")
    trigger.trigger(rw, {"some": "header"}, message)
    send.assert_called_once_with({"result": mocker.ANY}, transaction=mocker.ANY)
    t.send.assert_called_once_with(
        "processing_recipe",
        {
            "parameters": {
                "ispyb_process": mock.ANY,
                "program_id": 56986673,
                "data": "/path/to/xia2-dials/DataFiles/nt28218v3_xins24_free.mtz",
                "scaled_unmerged_mtz": "/path/to/xia2-dials/DataFiles/nt28218v3_xins24_scaled_unmerged.mtz",
                "path_ext": "xia2/dials-run",
                "force": False,
            },
            "recipes": [],
        },
    )
    pjid = t.send.call_args.args[1]["parameters"]["ispyb_process"]
    with db_session_factory() as db_session:
        pj = (
            db_session.query(ProcessingJob)
            .filter(ProcessingJob.processingJobId == pjid)
            .one()
        )
        assert pj.displayName == "big_ep"
        assert pj.recipe == "postprocessing-big-ep"
        assert pj.dataCollectionId == dcid
        assert pj.automatic
        params = {
            (pjp.parameterKey, pjp.parameterValue) for pjp in pj.ProcessingJobParameters
        }
        assert params == {
            (
                "scaled_unmerged_mtz",
                "/path/to/xia2-dials/DataFiles/nt28218v3_xins24_scaled_unmerged.mtz",
            ),
            ("data", "/path/to/xia2-dials/DataFiles/nt28218v3_xins24_free.mtz"),
            ("program_id", "56986673"),
        }


def test_mrbump(db_session_factory, testconfig, testdb, mocker, monkeypatch, tmp_path):
    monkeypatch.setenv("ISPYB_CREDENTIALS", testconfig)
    dcid = 1002287
    message = {
        "recipe": {
            "1": {
                "service": "DLS Trigger",
                "queue": "trigger",
                "parameters": {
                    "target": "mrbump",
                    "dcid": f"{dcid}",
                    "comment": "MrBUMP triggered by automatic xia2-3dii",
                    "automatic": True,
                    "user_pdb_directory": None,
                    "pdb_tmpdir": tmp_path,
                    "scaling_id": 123456,
                    "protein_info": {
                        "sequence": "ABCDEFG",
                    },
                    "hklin": "/path/to/xia2-3dii/DataFiles/foo_free.mtz",
                },
            },
        },
        "recipe-pointer": 1,
    }
    trigger = DLSTrigger()
    trigger._ispyb_sessionmaker = db_session_factory
    t = mock.create_autospec(workflows.transport.common_transport.CommonTransport)
    rw = RecipeWrapper(message=message, transport=t)
    trigger.ispyb = testdb
    send = mocker.spy(rw, "send")
    trigger.trigger(rw, {"some": "header"}, message)
    send.assert_called_once_with({"result": mocker.ANY}, transaction=mocker.ANY)
    t.send.assert_called_once_with(
        "processing_recipe",
        {
            "parameters": {
                "ispyb_process": mock.ANY,
            },
            "recipes": [],
        },
    )
    pjid = t.send.call_args.args[1]["parameters"]["ispyb_process"]
    with db_session_factory() as db_session:
        pj = (
            db_session.query(ProcessingJob)
            .filter(ProcessingJob.processingJobId == pjid)
            .one()
        )
        assert pj.displayName == "MrBUMP"
        assert pj.recipe == "postprocessing-mrbump"
        assert pj.dataCollectionId == dcid
        assert pj.automatic
        params = {
            (pjp.parameterKey, pjp.parameterValue) for pjp in pj.ProcessingJobParameters
        }
        assert params == {
            ("scaling_id", "123456"),
            ("hklin", "/path/to/xia2-3dii/DataFiles/foo_free.mtz"),
        }


def test_mrbump_with_model(
    db_session_factory, testconfig, testdb, mocker, monkeypatch, tmp_path
):
    monkeypatch.setenv("ISPYB_CREDENTIALS", testconfig)
    dcid = 1002287
    user_pdb_directory = tmp_path / "user_pdb"
    user_pdb_directory.mkdir()
    (user_pdb_directory / "test.pdb").touch()
    message = {
        "recipe": {
            "1": {
                "service": "DLS Trigger",
                "queue": "trigger",
                "parameters": {
                    "target": "mrbump",
                    "dcid": f"{dcid}",
                    "comment": "MrBUMP triggered by automatic xia2-3dii",
                    "automatic": True,
                    "scaling_id": 123456,
                    "user_pdb_directory": user_pdb_directory,
                    "pdb_tmpdir": tmp_path,
                    "protein_info": {
                        "sequence": "ABCDEFG",
                    },
                    "hklin": "/path/to/xia2-3dii/DataFiles/foo_free.mtz",
                },
            },
        },
        "recipe-pointer": 1,
    }
    trigger = DLSTrigger()
    trigger._ispyb_sessionmaker = db_session_factory
    t = mock.create_autospec(workflows.transport.common_transport.CommonTransport)
    rw = RecipeWrapper(message=message, transport=t)
    trigger.ispyb = testdb
    send = mocker.spy(rw, "send")
    trigger.trigger(rw, {"some": "header"}, message)
    send.assert_called_once_with({"result": mocker.ANY}, transaction=mocker.ANY)
    t.send.assert_has_calls(
        [
            mock.call(
                "processing_recipe",
                {
                    "parameters": {
                        "ispyb_process": mock.ANY,
                    },
                    "recipes": [],
                },
            ),
        ]
        * 2
    )
    all_params = []
    with db_session_factory() as db_session:
        for args, kwargs in t.send.call_args_list:
            pjid = args[1]["parameters"]["ispyb_process"]
            pj = (
                db_session.query(ProcessingJob)
                .filter(ProcessingJob.processingJobId == pjid)
                .one()
            )
            assert pj.displayName == "MrBUMP"
            assert pj.recipe == "postprocessing-mrbump"
            assert pj.dataCollectionId == dcid
            assert pj.automatic
            params = {
                (pjp.parameterKey, pjp.parameterValue)
                for pjp in pj.ProcessingJobParameters
            }
            all_params.append(params)
    assert sorted(all_params) == sorted(
        [
            {
                ("mdlunmod", "True"),
                ("dophmmer", "False"),
                ("hklin", "/path/to/xia2-3dii/DataFiles/foo_free.mtz"),
                ("scaling_id", "123456"),
                ("localfile", str(user_pdb_directory / "test.pdb")),
            },
            {
                ("hklin", "/path/to/xia2-3dii/DataFiles/foo_free.mtz"),
                ("scaling_id", "123456"),
            },
        ]
    )


@pytest.fixture
def insert_protein_with_sequence(db_session):
    protein = Protein(
        proposalId=141666,
        name="Test_Insulin",
        acronym="Test_Insulin",
        description="Insulin",
        sequence="GIVEQCCASVCSLYQLENYCNFVNQHLCGSHLVEALYLVCGERGFFYTPKA",
    )
    db_session.add(protein)
    db_session.commit()
    return protein.proteinId


def test_alphafold(
    insert_protein_with_sequence,
    db_session_factory,
    testconfig,
    testdb,
    mocker,
    monkeypatch,
    tmp_path,
):
    monkeypatch.setenv("ISPYB_CREDENTIALS", testconfig)
    protein_id = insert_protein_with_sequence

    message = {
        "recipe": {
            "1": {
                "service": "DLS Trigger",
                "queue": "trigger",
                "parameters": {
                    "target": "alphafold",
                    "protein_id": f"{protein_id}",
                },
            },
        },
        "recipe-pointer": 1,
    }
    trigger = DLSTrigger()
    trigger._ispyb_sessionmaker = db_session_factory
    t = mock.create_autospec(workflows.transport.common_transport.CommonTransport)
    rw = RecipeWrapper(message=message, transport=t)
    trigger.ispyb = testdb
    send = mocker.spy(rw, "send")
    trigger.trigger(rw, {"some": "header"}, message)
    send.assert_called_once_with({"result": mocker.ANY}, transaction=mocker.ANY)
    t.send.assert_called_once_with(
        "processing_recipe",
        {
            "recipes": ["alphafold"],
            "parameters": {
                "ispyb_protein_id": f"{protein_id}",
                "ispyb_protein_sequence": "GIVEQCCASVCSLYQLENYCNFVNQHLCGSHLVEALYLVCGERGFFYTPKA",
                "ispyb_protein_name": "Test_Insulin",
            },
        },
    )
