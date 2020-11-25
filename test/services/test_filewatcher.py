import time
from unittest import mock

import workflows.transport.common_transport
from workflows.recipe.wrapper import RecipeWrapper

from dlstbx.services.filewatcher import DLSFileWatcher


def generate_recipe_message(parameters, output):
    """Helper function for tests."""
    message = {
        "recipe": {
            1: {
                "service": "DLS Filewatcher",
                "queue": "filewatcher",
                "parameters": parameters,
                "output": output,
            },
            2: {"service": "DLS Per-Image-Analysis", "queue": "transient.output"},
            "start": [(1, [])],
        },
        "recipe-pointer": 1,
        "recipe-path": [],
        "environment": {
            "ID": mock.sentinel.GUID,
            "source": mock.sentinel.source,
            "timestamp": mock.sentinel.timestamp,
        },
        "payload": mock.sentinel.payload,
    }
    return message


def test_filewatcher_watch_pattern_timeout(mocker, tmpdir):
    mock_transport = mock.Mock()
    filewatcher = DLSFileWatcher()
    setattr(filewatcher, "_transport", mock_transport)
    filewatcher.initializing()
    t = mock.create_autospec(workflows.transport.common_transport.CommonTransport)
    pattern = "image%06d"
    images = [tmpdir.join(pattern % (i + 1)) for i in range(10)]
    m = generate_recipe_message(
        parameters={
            "pattern": tmpdir.join(pattern).strpath,
            "pattern-start": "1",
            "pattern-end": "10",
            "expected-per-image-delay": "0.01",
            "timeout": 1,
        },
        output={"any": 1},
    )
    rw = RecipeWrapper(message=m, transport=t)
    # Spy on the rw.send_to method
    send_to = mocker.spy(rw, "send_to")
    filewatcher.watch_files(rw, {"some": "header"}, mock.sentinel.message)
    images[0].write("content")
    filewatcher.watch_files(
        rw, {"some": "header"}, t.send.mock_calls[-1].args[1]["payload"]
    )
    send_to.assert_has_calls(
        [
            mock.call(
                "first",
                {"file": images[0], "file-number": 1, "file-pattern-index": 1},
                transaction=mock.ANY,
            ),
            mock.call(
                "every",
                {"file": images[0], "file-number": 1, "file-pattern-index": 1},
                transaction=mock.ANY,
            ),
            mock.call(
                1,
                {"file": images[0], "file-number": 1, "file-pattern-index": 1},
                transaction=mock.ANY,
            ),
            mock.call(
                "1",
                {"file": images[0], "file-number": 1, "file-pattern-index": 1},
                transaction=mock.ANY,
            ),
        ]
    )
    # Sleep in order that we can hit the timeout
    time.sleep(2)
    # Call watch_files and assert that it has called rw.send_to as expected
    filewatcher.watch_files(
        rw, {"some": "header"}, t.send.mock_calls[-1].args[1]["payload"]
    )
    send_to.assert_has_calls(
        [
            mock.call(
                "timeout",
                {
                    "file": images[1],
                    "file-number": 2,
                    "file-pattern-index": 2,
                    "success": False,
                },
                transaction=mock.ANY,
            ),
            mock.call(
                "any", {"files-expected": 10, "files-seen": 1}, transaction=mock.ANY
            ),
            mock.call(
                "finally",
                {"files-expected": 10, "files-seen": 1, "success": False},
                transaction=mock.ANY,
            ),
        ]
    )


def test_filewatcher_watch_list_timeout(mocker, tmpdir):
    mock_transport = mock.Mock()
    filewatcher = DLSFileWatcher()
    setattr(filewatcher, "_transport", mock_transport)
    filewatcher.initializing()
    t = mock.create_autospec(workflows.transport.common_transport.CommonTransport)
    files = [tmpdir.join("header"), tmpdir.join("end")]
    m = generate_recipe_message(
        parameters={
            "list": [f.strpath for f in files],
            "timeout": 0.5,
            "log-timeout-as-info": True,
        },
        output={"any": 1},
    )
    rw = RecipeWrapper(message=m, transport=t)
    send_to = mocker.spy(rw, "send_to")
    filewatcher.watch_files(rw, {"some": "header"}, mock.sentinel.message)
    files[0].write("content")
    filewatcher.watch_files(
        rw, {"some": "header"}, t.send.mock_calls[-1].args[1]["payload"]
    )
    send_to.assert_has_calls(
        [
            mock.call(
                "first",
                {"file": files[0], "file-list-index": 1},
                transaction=mock.ANY,
            ),
            mock.call(
                "every",
                {"file": files[0], "file-list-index": 1},
                transaction=mock.ANY,
            ),
            mock.call(
                1,
                {"file": files[0], "file-list-index": 1},
                transaction=mock.ANY,
            ),
            mock.call(
                "1",
                {"file": files[0], "file-list-index": 1},
                transaction=mock.ANY,
            ),
        ]
    )
    # Sleep in order that we can hit the timeout
    time.sleep(2)
    filewatcher.watch_files(
        rw, {"some": "header"}, t.send.mock_calls[-1].args[1]["payload"]
    )
    send_to.assert_has_calls(
        [
            mock.call(
                "timeout",
                {"file": files[1], "file-list-index": 2, "success": False},
                transaction=mock.ANY,
            ),
            mock.call(
                "any", {"files-expected": 2, "files-seen": 1}, transaction=mock.ANY
            ),
            mock.call(
                "finally",
                {"files-expected": 2, "files-seen": 1, "success": False},
                transaction=mock.ANY,
            ),
        ]
    )


def test_parse_everys():
    assert DLSFileWatcher._parse_everys({"every": 2}) == {}
    assert DLSFileWatcher._parse_everys({"every-1": 3}) == {1: "every-1"}
    assert DLSFileWatcher._parse_everys({"every-2": 4}) == {2: "every-2"}
