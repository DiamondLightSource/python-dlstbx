from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace
from unittest.mock import ANY, Mock

import pydantic
import pytest
from ispyb.sqlalchemy import DataCollection, DataCollectionGroup, GridInfo

from dlstbx.wrapper.dc_sim import DCSimParameters, DCSimWrapper


class FakeSession:
    def __init__(self, query_result=None):
        self.query_result = query_result
        self.added = []
        self.commits = 0

    def query(self, *_args):
        return self

    def options(self, *_args):
        return self

    def join(self, *_args):
        return self

    def outerjoin(self, *_args):
        return self

    def filter(self, *_args):
        return self

    def first(self):
        return self.query_result

    def one_or_none(self):
        return self.query_result

    def add(self, instance):
        self.added.append(instance)

    def commit(self):
        self.commits += 1
        for instance in self.added:
            if isinstance(instance, DataCollectionGroup):
                instance.dataCollectionGroupId = 900
            elif isinstance(instance, DataCollection):
                instance.dataCollectionId = 901


def values_for_model(model, prefix="source"):
    return {
        column.name: f"{prefix}_{column.name}"
        for column in model.__table__.columns
        if column.name
        not in {"dataCollectionGroupId", "dataCollectionId", "gridInfoId"}
    }


def test_dc_sim_parameters_validate_and_expose_attributes():
    params = DCSimParameters(
        beamline="i02-1",
        src_visit="nt44198-1",
        src_allowed_visits=["nt44198"],
        dest_allowed_visits=["nt44198"],
        dflt_proposal="nt44198",
        dflt_i02_visits={"i02-1": 7},
        dc_info={"dataCollectionGroupId": "42", "runStatus": "DataCollection"},
        dcg_dcids=["100", 101],
        dcid="100",
        dest_visit=None,
        copy_dcg=None,
    )

    assert params.beamline == "i02-1"
    assert params.dcid == 100
    assert params.dcg_dcids == [100, 101]
    assert params.dc_info.dataCollectionGroupId == 42
    assert params.copy_dcg is None


def test_dc_sim_parameters_reject_invalid_values():
    with pytest.raises(pydantic.ValidationError):
        DCSimParameters(
            beamline="i02-1",
            src_visit="nt44198-1",
            src_allowed_visits=["nt44198"],
            dest_allowed_visits=["nt44198"],
            dflt_proposal="nt44198",
            dflt_i02_visits={"i02-1": 7},
            dc_info={"dataCollectionGroupId": 42},
            dcg_dcids=[],
            dcid=0,
        )


def test_retrieve_sessionid_returns_session_id():
    wrapper = DCSimWrapper()
    session = FakeSession((SimpleNamespace(sessionId=123), SimpleNamespace()))

    assert wrapper.retrieve_sessionid(session, "nt44198-1") == 123


@pytest.mark.parametrize(
    ("result", "message"),
    [
        (None, "Query to obtain sessionid failed"),
        (
            (SimpleNamespace(sessionId=None), SimpleNamespace()),
            "Could not find sessionid",
        ),
    ],
)
def test_retrieve_sessionid_raises_when_visit_is_not_found(result, message):
    wrapper = DCSimWrapper()

    with pytest.raises(ValueError, match=message):
        wrapper.retrieve_sessionid(FakeSession(result), "nt44198-1")


def test_get_dc_and_grid_info_returns_records():
    wrapper = DCSimWrapper()
    dc = SimpleNamespace(dataCollectionId=42)
    grid_info = SimpleNamespace(gridInfoId=7)

    assert wrapper.get_dc_and_grid_info_from_dcid(FakeSession((dc, grid_info)), 42) == (
        dc,
        grid_info,
    )


def test_get_dc_and_grid_info_raises_for_missing_record():
    wrapper = DCSimWrapper()

    with pytest.raises(ValueError, match="Could not find data collection for dcid=42"):
        wrapper.get_dc_and_grid_info_from_dcid(FakeSession(None), 42)


def test_copy_data_collection_overrides_ids_and_fields():
    wrapper = DCSimWrapper()
    source_dc = SimpleNamespace(**values_for_model(DataCollection))
    session = FakeSession()

    new_dcid = wrapper.copy_data_collection(
        session,
        source_dc,
        new_session_id=101,
        new_data_collection_group_id=202,
        comments="simulation",
    )

    copied = session.added[0]
    assert new_dcid == 901
    assert copied.SESSIONID == 101
    assert copied.dataCollectionGroupId == 202
    assert copied.comments == "simulation"
    assert copied.imageSuffix == source_dc.imageSuffix
    assert session.commits == 1


def test_copy_data_collection_group_updates_session_and_timestamps():
    wrapper = DCSimWrapper()
    source = SimpleNamespace(**values_for_model(DataCollectionGroup))
    session = FakeSession(source)

    new_dcgid = wrapper.copy_data_collection_group(session, 12, 34)

    copied = session.added[0]
    assert new_dcgid == 900
    assert copied.sessionId == 34
    assert isinstance(copied.startTime, datetime)
    assert copied.endTime == copied.startTime
    assert copied.comments == source.comments
    assert session.commits == 1


def test_copy_grid_info_relinks_record_to_new_data_collection():
    wrapper = DCSimWrapper()
    source = SimpleNamespace(
        **{
            column.name: f"source_{column.name}"
            for column in GridInfo.__table__.columns
            if column.name not in {"gridInfoId", "dataCollectionId"}
        }
    )
    session = FakeSession()

    wrapper.copy_grid_info(session, source, new_dcid=45)

    copied = session.added[0]
    assert copied.dataCollectionId == 45
    assert copied.gridInfoId is None
    for column in GridInfo.__table__.columns:
        if column.name not in {"gridInfoId", "dataCollectionId"}:
            assert getattr(copied, column.name) == getattr(source, column.name)
    assert session.commits == 1


def test_simulate_datacollection_copies_cbf_images_and_snapshots(tmp_path, monkeypatch):
    wrapper = DCSimWrapper()
    source_dir = tmp_path / "source"
    dest_dir = tmp_path / "destination"
    jpgs_dest_dir = tmp_path / "snapshots"
    source_dir.mkdir()
    dest_dir.mkdir()
    jpgs_dest_dir.mkdir()
    (source_dir / "image_0001.cbf").write_text("one")
    (source_dir / "image_0003.cbf").write_text("three")
    snapshot = tmp_path / "crystal.jpg"
    snapshot.write_text("snapshot")
    dc = SimpleNamespace(
        imageDirectory=str(source_dir),
        imageSuffix="cbf",
        fileTemplate="image_####.cbf",
        startImageNumber=1,
        numberOfImages=3,
        xtalSnapshotFullPath1=str(snapshot),
        xtalSnapshotFullPath2=None,
        xtalSnapshotFullPath3=None,
        xtalSnapshotFullPath4=None,
    )
    monkeypatch.setattr(
        wrapper, "get_dc_and_grid_info_from_dcid", lambda *_: (dc, None)
    )
    copy_dc = Mock(return_value=501)
    copy_grid = Mock()
    monkeypatch.setattr(wrapper, "copy_data_collection", copy_dc)
    monkeypatch.setattr(wrapper, "copy_grid_info", copy_grid)

    new_dcid, is_grid_scan = wrapper.simulate_datacollection(
        FakeSession(), 25, dest_dir, jpgs_dest_dir, 30, 40
    )

    assert new_dcid == 501
    assert not is_grid_scan
    assert sorted(path.name for path in dest_dir.iterdir()) == [
        "image_0001.cbf",
        "image_0003.cbf",
    ]
    assert (jpgs_dest_dir / "crystal.jpg").read_text() == "snapshot"
    assert copy_dc.call_args.kwargs["xtalSnapshotFullPath1"] == str(
        jpgs_dest_dir / "crystal.jpg"
    )
    assert copy_dc.call_args.kwargs["new_session_id"] == 30
    assert copy_dc.call_args.kwargs["new_data_collection_group_id"] == 40
    copy_grid.assert_not_called()


def test_simulate_datacollection_copies_hdf5_and_nexus_files(tmp_path, monkeypatch):
    wrapper = DCSimWrapper()
    source_dir = tmp_path / "source"
    dest_dir = tmp_path / "destination"
    jpgs_dest_dir = tmp_path / "snapshots"
    source_dir.mkdir()
    dest_dir.mkdir()
    jpgs_dest_dir.mkdir()
    for name in (
        "sample_2_000001.h5",
        "sample_2_000002.h5",
        "sample_2.nxs",
        "other.h5",
    ):
        (source_dir / name).write_text(name)
    dc = SimpleNamespace(
        imageDirectory=str(source_dir),
        imageSuffix="h5",
        imagePrefix="sample",
        dataCollectionNumber=2,
        xtalSnapshotFullPath1=None,
        xtalSnapshotFullPath2=None,
        xtalSnapshotFullPath3=None,
        xtalSnapshotFullPath4=None,
    )
    grid_info = SimpleNamespace(gridInfoId=4)
    monkeypatch.setattr(
        wrapper, "get_dc_and_grid_info_from_dcid", lambda *_: (dc, grid_info)
    )
    copy_dc = Mock(return_value=502)
    copy_grid = Mock()
    monkeypatch.setattr(wrapper, "copy_data_collection", copy_dc)
    monkeypatch.setattr(wrapper, "copy_grid_info", copy_grid)

    assert wrapper.simulate_datacollection(
        FakeSession(), 25, dest_dir, jpgs_dest_dir, 30, 40
    ) == (502, True)
    assert {path.name for path in dest_dir.iterdir()} == {
        "sample_2_000001.h5",
        "sample_2_000002.h5",
        "sample_2.nxs",
    }
    copy_grid.assert_called_once_with(db_session=ANY, source_gi=grid_info, new_dcid=502)


@pytest.mark.parametrize(
    ("suffix", "template", "message"),
    [
        ("cbf", "image.cbf", "does not contain a valid '#' pattern"),
        ("tif", "image_####.tif", "Invalid image suffix tif"),
    ],
)
def test_simulate_datacollection_rejects_bad_image_format(
    tmp_path, monkeypatch, suffix, template, message
):
    wrapper = DCSimWrapper()
    dc = SimpleNamespace(
        imageDirectory=str(tmp_path),
        imageSuffix=suffix,
        fileTemplate=template,
        startImageNumber=1,
        numberOfImages=1,
    )
    monkeypatch.setattr(
        wrapper, "get_dc_and_grid_info_from_dcid", lambda *_: (dc, None)
    )

    with pytest.raises(ValueError, match=message):
        wrapper.simulate_datacollection(FakeSession(), 25, tmp_path, tmp_path, 30, 40)
