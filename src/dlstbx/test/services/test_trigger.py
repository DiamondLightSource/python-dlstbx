import datetime
import pytest
import workflows.transport.common_transport
from workflows.recipe.wrapper import RecipeWrapper
from unittest import mock

import ispyb.sqlalchemy
from ispyb.sqlalchemy import (
    AutoProcIntegration,
    AutoProcProgram,
    AutoProcProgramAttachment,
    DataCollection,
    DataCollectionGroup,
    ProcessingJob,
)

from dlstbx.services.trigger import DLSTrigger


@pytest.fixture
def insert_multiplex_input(alchemy):
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
        pj = ProcessingJob(
            DataCollection=dc,
            automatic=True,
        )
        app = AutoProcProgram(
            ProcessingJob=pj,
            processingStatus=1,
            processingStartTime=datetime.datetime.now(),
            processingPrograms="xia2 dials",
        )
        api = AutoProcIntegration(DataCollection=dc, AutoProcProgram=app)
        alchemy.add_all([dcg, dc, api, app, pj])
        for ext in ("expt", "refl"):
            alchemy.add(
                AutoProcProgramAttachment(
                    AutoProcProgram=app,
                    filePath=f"/path/to/xia2-dials-{i}",
                    fileName=f"integrated.{ext}",
                )
            )
    alchemy.commit()
    return [dc.dataCollectionId for dc in dcs]


def test_multiplex(insert_multiplex_input, testconfig, testdb, mocker):
    session = ispyb.sqlalchemy.session(testconfig)
    dcids = insert_multiplex_input
    message = {
        "recipe": {
            "1": {
                "service": "DLS Trigger",
                "queue": "trigger",
                "parameters": {
                    "target": "multiplex",
                    "dcid": dcids[-1],
                    "wavelength": "1.03936",
                    "comment": "xia2.multiplex triggered by automatic xia2-dials",
                    "automatic": True,
                    "ispyb_parameters": None,
                    "related_dcids": [
                        {
                            "dcids": dcids[:-1],
                        }
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
    t = mock.create_autospec(workflows.transport.common_transport.CommonTransport)
    rw = RecipeWrapper(message=message, transport=t)
    trigger.ispyb = testdb
    trigger.session = session
    send = mocker.spy(rw, "send")
    trigger.trigger(rw, {"some": "header"}, message)
    send.assert_called_once_with({"result": [mocker.ANY]}, transaction=mocker.ANY)
    kall = send.mock_calls[0]
    name, args, kwargs = kall
    pjid = args[0]["result"][0]
    # Need a new session to reflect the data inserted by stored procedures
    session = ispyb.sqlalchemy.session(testconfig)
    pj = (
        session.query(ProcessingJob).filter(ProcessingJob.processingJobId == pjid).one()
    )
    assert pj.displayName == "xia2.multiplex"
    assert pj.recipe == "postprocessing-xia2-multiplex"
    assert pj.dataCollectionId == dcids[-1]
    assert pj.automatic


@pytest.fixture
def insert_dimple_input(alchemy):
    dcg = DataCollectionGroup(sessionId=55167)
    dc = DataCollection(
        DataCollectionGroup=dcg,
        BLSAMPLEID=398827,
        startImageNumber=1,
        numberOfImages=180,
    )
    alchemy.add_all([dc, dcg])
    alchemy.commit()
    return dc.dataCollectionId


def test_dimple_trigger(insert_dimple_input, testconfig, testdb, mocker, tmp_path):
    session = ispyb.sqlalchemy.session(testconfig)
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
    t = mock.create_autospec(workflows.transport.common_transport.CommonTransport)
    rw = RecipeWrapper(message=message, transport=t)
    trigger.ispyb = testdb
    trigger.session = session
    send = mocker.spy(rw, "send")
    trigger.trigger(rw, {"some": "header"}, message)
    send.assert_called_once_with({"result": mocker.ANY}, transaction=mocker.ANY)
    kall = send.mock_calls[0]
    name, args, kwargs = kall
    pjid = args[0]["result"]
    # Need a new session to reflect the data inserted by stored procedures
    session = ispyb.sqlalchemy.session(testconfig)
    pj = (
        session.query(ProcessingJob).filter(ProcessingJob.processingJobId == pjid).one()
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


def test_ep_predict(testconfig, testdb, mocker):
    session = ispyb.sqlalchemy.session(testconfig)
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
                        "anomalousscatterer": "S",
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
    t = mock.create_autospec(workflows.transport.common_transport.CommonTransport)
    rw = RecipeWrapper(message=message, transport=t)
    trigger.ispyb = testdb
    trigger.session = session
    send = mocker.spy(rw, "send")
    trigger.trigger(rw, {"some": "header"}, message)
    send.assert_called_once_with({"result": mocker.ANY}, transaction=mocker.ANY)
    kall = send.mock_calls[0]
    name, args, kwargs = kall
    pjid = args[0]["result"]
    # Need a new session to reflect the data inserted by stored procedures
    session = ispyb.sqlalchemy.session(testconfig)
    pj = (
        session.query(ProcessingJob).filter(ProcessingJob.processingJobId == pjid).one()
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


def test_fast_ep(testconfig, testdb, mocker):
    session = ispyb.sqlalchemy.session(testconfig)
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
                        "anomalousscatterer": "S",
                    },
                    "scaling_id": "123456",
                    "mtz": "/path/to/fast_dp/fast_dp.mtz",
                },
            }
        },
        "recipe-pointer": 1,
    }
    trigger = DLSTrigger()
    t = mock.create_autospec(workflows.transport.common_transport.CommonTransport)
    rw = RecipeWrapper(message=message, transport=t)
    trigger.ispyb = testdb
    trigger.session = session
    send = mocker.spy(rw, "send")
    trigger.trigger(rw, {"some": "header"}, message)
    send.assert_called_once_with({"result": mocker.ANY}, transaction=mocker.ANY)
    kall = send.mock_calls[0]
    name, args, kwargs = kall
    pjid = args[0]["result"]
    # Need a new session to reflect the data inserted by stored procedures
    session = ispyb.sqlalchemy.session(testconfig)
    pj = (
        session.query(ProcessingJob).filter(ProcessingJob.processingJobId == pjid).one()
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
