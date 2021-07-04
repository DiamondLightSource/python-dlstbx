# pytest configuration file
import os

import ispyb.sqlalchemy
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


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
