# pytest configuration file
import os
from typing import List

import ispyb.sqlalchemy
import pkg_resources
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
def handle_i99_rotation(scenario: mimas.MimasScenario) -> List[mimas.Invocation]:
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
def handle_i99(scenario: mimas.MimasScenario) -> List[mimas.Invocation]:
    return [
        mimas.MimasRecipeInvocation(DCID=scenario.DCID, recipe="spam"),
    ]


@pytest.fixture
def with_dummy_plugins():
    # Get the current distribution and entry map
    dist = pkg_resources.get_distribution("dlstbx")
    entry_map = pkg_resources.get_entry_map("dlstbx", group="zocalo.mimas.handlers")

    # Create the fake entry point definitions and add the mapping
    entry_map["i99"] = pkg_resources.EntryPoint.parse(
        f"i99 = {__name__}:handle_i99", dist=dist
    )
    entry_map["i99_rotation"] = pkg_resources.EntryPoint.parse(
        f"i99_rotation = {__name__}:handle_i99_rotation", dist=dist
    )
    mimas._get_handlers.cache_clear()
    yield
    # cleanup
    del entry_map["i99"]
    del entry_map["i99_rotation"]
    mimas._get_handlers.cache_clear()
