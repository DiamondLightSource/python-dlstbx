from __future__ import annotations

from unittest import mock

import pytest
from workflows.recipe.wrapper import RecipeWrapper
from workflows.transport.offline_transport import OfflineTransport

from dlstbx.services.archiver import DLSArchiver, Dropfile


@pytest.fixture
def mock_transport():
    """Create a mock transport object."""
    transport = mock.Mock()
    txn = mock.Mock()
    transport.transaction_begin.return_value = txn
    return transport, txn


@pytest.fixture
def archiver_service(mock_transport):
    """Create a DLSArchiver service instance with mocked transport."""
    transport, _ = mock_transport
    service = DLSArchiver()
    service._transport = transport
    return service


def create_test_files(tmp_path, file_list):
    """Helper to create test files in tmp_path."""
    files = []
    for filename in file_list:
        filepath = tmp_path / filename
        filepath.parent.mkdir(parents=True, exist_ok=True)
        filepath.write_text(f"Content of {filename}")
        files.append(str(filepath))
    return files


def create_recipe_message_filelist(parameters, files):
    """Create a recipe message for filelist archiving."""
    message = {
        "recipe": {
            "1": {
                "service": "DLS Archiver",
                "queue": "archive.filelist",
                "parameters": parameters,
            },
        },
        "recipe-pointer": "1",
        "recipe-path": [],
        "environment": {
            "ID": mock.sentinel.GUID,
            "source": mock.sentinel.source,
            "timestamp": mock.sentinel.timestamp,
        },
        "payload": {"filelist": files},
    }
    return message


def create_recipe_message_dcid(parameters):
    """Create a recipe message for dcid archiving."""
    message = {
        "recipe": {
            "1": {
                "service": "DLS Archiver",
                "queue": "archive.pattern",
                "parameters": parameters,
            },
        },
        "recipe-pointer": "1",
        "recipe-path": [],
        "environment": {
            "ID": mock.sentinel.GUID,
            "source": mock.sentinel.source,
            "timestamp": mock.sentinel.timestamp,
        },
        "payload": {},
    }
    return message


class TestArchiverFilelist:
    """Tests for the archive_filelist method."""

    def test_archive_filelist_creates_dropfile(
        self, archiver_service, mock_transport, tmp_path
    ):
        """Test that a dropfile is created when valid files are provided."""
        transport, txn = mock_transport
        archiver_service._transport = transport

        # Create test files
        files = create_test_files(tmp_path, ["file1.txt", "file2.txt"])

        # Create recipe parameters
        dropfile_path = tmp_path / "dropfile.xml"
        parameters = {
            "visit": "cm00001-1",
            "beamline": "i03",
            "dropfile": str(dropfile_path),
            "filelist": files,
        }

        # Create message
        message = create_recipe_message_filelist(parameters, files)
        header = {
            "message-id": "test-message-id",
            "subscription": "test-subscription",
        }

        # Create wrapper and call method
        rw = RecipeWrapper(message=message, transport=OfflineTransport())
        rw._transport = OfflineTransport()
        archiver_service.archive_filelist(rw, header, message.get("payload"))

        # Verify dropfile was created
        assert dropfile_path.exists(), "Dropfile should be created"

        # Verify content is XML
        content = dropfile_path.read_text()
        assert "<?xml version" in content
        assert "CM00001" in content  # Visit codes are converted to uppercase
        assert "i03" in content

    def test_archive_filelist_with_forbidden_visit_code(
        self, archiver_service, mock_transport, tmp_path
    ):
        """Test that no dropfile is created for forbidden visit codes."""
        transport, txn = mock_transport
        archiver_service._transport = transport

        # Create test files
        files = create_test_files(tmp_path, ["file1.txt"])

        # Create recipe parameters with forbidden visit code
        dropfile_path = tmp_path / "dropfile.xml"
        parameters = {
            "visit": "in12345-1",
            "beamline": "i03",
            "dropfile": str(dropfile_path),
            "filelist": files,
            "forbidden-visit-codes": ["in", "il"],
        }

        # Create message
        message = create_recipe_message_filelist(parameters, files)
        header = {
            "message-id": "test-message-id",
            "subscription": "test-subscription",
        }

        # Create wrapper and call method
        rw = RecipeWrapper(message=message, transport=OfflineTransport())
        rw._transport = OfflineTransport()
        archiver_service.archive_filelist(rw, header, message.get("payload"))

        # Verify no dropfile was created
        assert not dropfile_path.exists(), (
            "Dropfile should not be created for forbidden visit"
        )

        # Verify transaction was committed
        transport.transaction_commit.assert_called_with(txn)

    def test_archive_filelist_empty_files_list(self, archiver_service, mock_transport):
        """Test that empty file list is rejected."""
        transport, txn = mock_transport
        archiver_service._transport = transport

        # Create recipe parameters with empty file list
        parameters = {
            "visit": "cm00001-1",
            "beamline": "i03",
            "filelist": [],
        }

        # Create message with empty filelist
        message = create_recipe_message_filelist(parameters, [])
        header = {
            "message-id": "test-message-id",
            "subscription": "test-subscription",
        }

        # Create wrapper and call method
        rw = RecipeWrapper(message=message, transport=OfflineTransport())
        rw._transport = OfflineTransport()
        archiver_service.archive_filelist(rw, header, message.get("payload"))

        # Verify NACK was called
        transport.nack.assert_called_once_with(header)

    def test_archive_filelist_missing_files(
        self, archiver_service, mock_transport, tmp_path
    ):
        """Test that missing files are handled correctly."""
        transport, txn = mock_transport
        archiver_service._transport = transport

        # Create only one test file
        files = create_test_files(tmp_path, ["file1.txt"])
        # Add a missing file to the list
        missing_file = str(tmp_path / "missing.txt")
        files.append(missing_file)

        # Create recipe parameters
        dropfile_path = tmp_path / "dropfile.xml"
        parameters = {
            "visit": "cm00001-1",
            "beamline": "i03",
            "dropfile": str(dropfile_path),
            "filelist": files,
        }

        # Create message
        message = create_recipe_message_filelist(parameters, files)
        header = {
            "message-id": "test-message-id",
            "subscription": "test-subscription",
        }

        # Create wrapper and call method
        rw = RecipeWrapper(message=message, transport=OfflineTransport())
        rw._transport = OfflineTransport()
        archiver_service.archive_filelist(rw, header, message.get("payload"))

        # Verify dropfile was still created
        assert dropfile_path.exists(), (
            "Dropfile should be created even with missing files"
        )


class TestArchiverDCID:
    """Tests for the archive_dcid method."""

    def test_archive_dcid_creates_dropfile(
        self, archiver_service, mock_transport, tmp_path
    ):
        """Test that a dropfile is created for valid dcid archiving."""
        transport, txn = mock_transport
        archiver_service._transport = transport

        # Create test files matching a pattern
        image_dir = (
            tmp_path / "data" / "cm" / "cm00001" / "i03" / "2024" / "Jan" / "cm00001-1"
        )
        image_dir.mkdir(parents=True, exist_ok=True)

        # Create a few test image files
        pattern = str(image_dir / "image_%06d.h5")
        for i in range(1, 4):
            filepath = image_dir / f"image_{i:06d}.h5"
            filepath.write_text(f"Image data {i}")

        # Create recipe parameters
        dropfile_path = tmp_path / "dropfile.xml"
        parameters = {
            "visit": "cm00001-1",
            "beamline": "i23",
            "pattern": pattern,
            "pattern-start": "1",
            "pattern-end": "3",
            "dropfile": str(dropfile_path),
        }

        # Create message
        message = create_recipe_message_dcid(parameters)
        header = {
            "message-id": "test-message-id",
            "subscription": "test-subscription",
        }

        # Create wrapper and call method
        rw = RecipeWrapper(message=message, transport=OfflineTransport())
        rw._transport = OfflineTransport()
        archiver_service.archive_dcid(rw, header, message.get("payload"))

        # Verify dropfile was created
        assert dropfile_path.exists(), "Dropfile should be created"

        # Verify content
        content = dropfile_path.read_text()
        assert "<?xml version" in content
        assert "CM00001" in content
        assert "i23" in content

    def test_archive_dcid_with_forbidden_visit_code(
        self, archiver_service, mock_transport, tmp_path
    ):
        """Test that no dropfile is created for forbidden visit codes in dcid."""
        transport, txn = mock_transport
        archiver_service._transport = transport

        # Create test files
        image_dir = (
            tmp_path / "data" / "cm" / "TEST" / "i03" / "2024" / "Jan" / "TEST-1"
        )
        image_dir.mkdir(parents=True, exist_ok=True)
        pattern = str(image_dir / "image_%06d.h5")
        for i in range(1, 2):
            filepath = image_dir / f"image_{i:06d}.h5"
            filepath.write_text(f"Image data {i}")

        # Create recipe parameters with forbidden visit code
        dropfile_path = tmp_path / "dropfile.xml"
        parameters = {
            "visit": "in12345-1",
            "pattern": pattern,
            "pattern-start": "1",
            "pattern-end": "1",
            "dropfile": str(dropfile_path),
            "forbidden-visit-codes": ["in", "il"],
        }

        # Create message
        message = create_recipe_message_dcid(parameters)
        header = {
            "message-id": "test-message-id",
            "subscription": "test-subscription",
        }

        # Create wrapper and call method
        rw = RecipeWrapper(message=message, transport=OfflineTransport())
        rw._transport = OfflineTransport()
        archiver_service.archive_dcid(rw, header, message.get("payload"))

        # Verify no dropfile was created
        assert not dropfile_path.exists(), (
            "Dropfile should not be created for forbidden visit"
        )

        # Verify transaction was committed
        transport.transaction_commit.assert_called_with(txn)

    def test_archive_dcid_no_files_found(
        self, archiver_service, mock_transport, tmp_path
    ):
        """Test behavior when no files match the pattern."""
        transport, txn = mock_transport
        archiver_service._transport = transport

        # Create a pattern that doesn't match any files
        pattern = str(tmp_path / "nonexistent" / "image_%06d.h5")

        # Create recipe parameters
        dropfile_path = tmp_path / "dropfile.xml"
        parameters = {
            "visit": "cm00001-1",
            "beamline": "i23",
            "pattern": pattern,
            "pattern-start": "1",
            "pattern-end": "3",
            "dropfile": str(dropfile_path),
        }

        # Create message
        message = create_recipe_message_dcid(parameters)
        header = {
            "message-id": "test-message-id",
            "subscription": "test-subscription",
        }

        # Create wrapper and call method
        rw = RecipeWrapper(message=message, transport=OfflineTransport())
        rw._transport = OfflineTransport()
        archiver_service.archive_dcid(rw, header, message.get("payload"))

        # Verify no dropfile was created (empty dropfile is skipped)
        assert not dropfile_path.exists(), (
            "Dropfile should not be created when no files found"
        )


class TestVisitValidation:
    """Tests for visit code validation."""

    def test_visit_is_archivable_allowed_visit(self, archiver_service):
        """Test that allowed visit codes pass validation."""
        result = archiver_service.visit_is_archivable("cm00001-1", ("TEST", "SKIP"))
        assert result is True

    def test_visit_is_archivable_forbidden_visit(self, archiver_service):
        """Test that forbidden visit codes fail validation."""
        result = archiver_service.visit_is_archivable("TEST-1", ("TEST", "SKIP"))
        assert result is False

    def test_visit_is_archivable_empty_forbidden_list(self, archiver_service):
        """Test that all visits are allowed when forbidden list is empty."""
        result = archiver_service.visit_is_archivable("TEST-1", ())
        assert result is True

    def test_visit_is_archivable_partial_match(self, archiver_service):
        """Test that only prefix matching counts."""
        # TEST-1 should match forbidden code TEST
        result = archiver_service.visit_is_archivable("TEST-1", ("TEST",))
        assert result is False

        # TESTING-1 should match forbidden code TEST (starts with TEST)
        result = archiver_service.visit_is_archivable("TESTING-1", ("TEST",))
        assert result is False

        # NOTTEST-1 should NOT match forbidden code TEST
        result = archiver_service.visit_is_archivable("NOTTEST-1", ("TEST",))
        assert result is True


class TestDropfileGeneration:
    """Tests for dropfile XML generation."""

    def test_dropfile_basic_structure(self, tmp_path):
        """Test that dropfile creates valid XML structure."""
        # Create test file
        testfile = tmp_path / "testdata.h5"
        testfile.write_text("test data")

        # Create dropfile
        df = Dropfile("cm00001-1", "i03", "testdir")
        df.add(str(testfile))
        df.close()

        # Get XML string
        xml_str = df.to_string().decode("latin-1")

        # Verify structure
        assert '<?xml version="1.0" ?>' in xml_str
        assert "<icat" in xml_str
        assert "<visit_id>CM00001-1</visit_id>" in xml_str
        assert "<instrument>i03</instrument>" in xml_str
        assert "<name>testdata.h5</name>" in xml_str
        assert f"<location>{str(testfile)}</location>" in xml_str

    def test_dropfile_multiple_files(self, tmp_path):
        """Test dropfile with multiple files."""
        # Create test files
        testfiles = []
        for i in range(3):
            testfile = tmp_path / f"data_{i}.h5"
            testfile.write_text(f"test data {i}")
            testfiles.append(str(testfile))

        # Create dropfile
        df = Dropfile("cm00001-1", "i03", "testdir")
        for testfile in testfiles:
            df.add(testfile)
        df.close()

        # Get XML string
        xml_str = df.to_string().decode("latin-1")

        # Verify all files are in the XML
        for testfile in testfiles:
            assert f"<name>data_{testfiles.index(testfile)}.h5</name>" in xml_str

    def test_dropfile_visit_uppercase_conversion(self):
        """Test that visit codes are converted to uppercase in dropfile."""
        df = Dropfile("cm00001-1", "i03", "testdir")
        df.close()

        xml_str = df.to_string().decode("latin-1")
        assert "<visit_id>CM00001-1</visit_id>" in xml_str
