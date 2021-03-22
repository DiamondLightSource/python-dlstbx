# pytest configuration file
import os
import ispyb.sqlalchemy
import pytest


@pytest.fixture
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


@pytest.fixture
def alchemy(testconfig):
    session = ispyb.sqlalchemy.session(testconfig)
    try:
        yield session
    finally:
        session.close()
