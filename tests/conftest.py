# pytest configuration file
from __future__ import annotations

import importlib.metadata
import os
from typing import List
from unittest.mock import patch

import ispyb.sqlalchemy
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from dlstbx import mimas
from dlstbx.mimas.specification import BeamlineSpecification, DCClassSpecification


@pytest.fixture(scope="session")
def testconfig():
    """Return the path to a configuration file pointing to a test database."""
    config_file = os.getenv("ISPYB_TEST_CREDENTIALS")
    if config_file and os.path.exists(config_file):
        return config_file
    pytest.skip(
        "No configuration file for test database found. Skipping database tests"
    )


@pytest.fixture
def testdb(testconfig):
    """Return an ISPyB connection object for the test database configuration."""
    with ispyb.open(testconfig) as conn:
        yield conn


@pytest.fixture(scope="session")
def db_engine(testconfig):
    """Yields a SQLAlchemy engine"""
    engine = create_engine(
        ispyb.sqlalchemy.url(testconfig), connect_args={"use_pure": True}
    )
    yield engine
    engine.dispose()


@pytest.fixture(scope="session")
def db_session_factory(db_engine):
    """Returns a SQLAlchemy scoped session factory"""
    return sessionmaker(bind=db_engine)


@pytest.fixture(scope="function")
def db_session(db_session_factory):
    """Yields a SQLAlchemy connection which is rollbacked after the test"""
    session_ = db_session_factory()
    yield session_
    session_.rollback()
    session_.close()


is_i99 = BeamlineSpecification("i99")
is_rotation = DCClassSpecification(mimas.MimasDCClass.ROTATION)


@mimas.match_specification(is_i99 & is_rotation)
def handle_i99_rotation(
    scenario: mimas.MimasScenario, **kwargs
) -> List[mimas.Invocation]:
    return [
        mimas.MimasRecipeInvocation(DCID=scenario.DCID, recipe="foo"),
        mimas.MimasISPyBJobInvocation(
            DCID=scenario.DCID,
            recipe="bar",
            autostart=True,
            source="foobar",
        ),
    ]


@mimas.match_specification(is_i99)
def handle_i99(scenario: mimas.MimasScenario, **kwargs) -> List[mimas.Invocation]:
    return [
        mimas.MimasRecipeInvocation(DCID=scenario.DCID, recipe="spam"),
    ]


@pytest.fixture
def with_dummy_plugins():
    real_entry_points = importlib.metadata.entry_points(group="zocalo.mimas.handlers")
    fake_entry_points = [
        importlib.metadata.EntryPoint(
            name="i99", value=f"{__name__}:handle_i99", group="zocalo.mimas.handlers"
        ),
        importlib.metadata.EntryPoint(
            name="i99_rotation",
            value=f"{__name__}:handle_i99_rotation",
            group="zocalo.mimas.handlers",
        ),
    ]
    combined = list(real_entry_points) + fake_entry_points

    with patch(
        "dlstbx.mimas.importlib.metadata.entry_points",
        return_value=combined,
    ):
        mimas._get_handlers.cache_clear()
        yield
    mimas._get_handlers.cache_clear()
