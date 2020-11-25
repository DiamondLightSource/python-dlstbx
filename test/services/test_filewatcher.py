import time
import pytest
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


def test_filewatcher_watch_pattern(mocker, tmpdir):
    mock_transport = mock.Mock()
    filewatcher = DLSFileWatcher()
    setattr(filewatcher, "_transport", mock_transport)
    filewatcher.initializing()
    t = mock.create_autospec(workflows.transport.common_transport.CommonTransport)
    pattern = "image%06d"
    image_ids = range(5, 11)
    images = [tmpdir.join(pattern % i) for i in image_ids]
    m = generate_recipe_message(
        parameters={
            "pattern": tmpdir.join(pattern).strpath,
            "pattern-start": f"{image_ids[0]}",
            "pattern-end": f"{image_ids[-1]}",
            "expected-per-image-delay": "0.1",
            "timeout": 1,
            "burst-wait": 10,
        },
        output={"any": 1, "select-2": 2},
    )
    rw = RecipeWrapper(message=m, transport=t)
    # Spy on the rw.send_to method
    send_to = mocker.spy(rw, "send_to")
    checkpoint = mocker.spy(rw, "checkpoint")
    filewatcher.watch_files(rw, {"some": "header"}, mock.sentinel.message)
    checkpoint.assert_any_call(
        {"filewatcher-status": {"seen-files": 0, "start-time": mock.ANY}},
        delay=10,
        transaction=mock.ANY,
    )
    for i, (image_id, image) in enumerate(zip(image_ids, images)):
        image.write("content")
        filewatcher.watch_files(
            rw, {"some": "header"}, t.send.mock_calls[-1].args[1]["payload"]
        )
        if image_id == image_ids[0]:
            send_to.assert_any_call(
                "first",
                {
                    "file": image,
                    "file-number": i + 1,
                    "file-pattern-index": image_ids[i],
                },
                transaction=mock.ANY,
            )
        send_to.assert_has_calls(
            [
                mock.call(
                    "every",
                    {
                        "file": image,
                        "file-number": i + 1,
                        "file-pattern-index": image_ids[i],
                    },
                    transaction=mock.ANY,
                ),
                mock.call(
                    i + 1,
                    {
                        "file": image,
                        "file-number": i + 1,
                        "file-pattern-index": image_ids[i],
                    },
                    transaction=mock.ANY,
                ),
                mock.call(
                    f"{i + 1}",
                    {
                        "file": image,
                        "file-number": i + 1,
                        "file-pattern-index": image_ids[i],
                    },
                    transaction=mock.ANY,
                ),
            ],
            any_order=True,
        )
    send_to.assert_has_calls(
        [
            mock.call(
                "last",
                {
                    "file": images[-1],
                    "file-number": len(images),
                    "file-pattern-index": image_ids[-1],
                },
                transaction=mock.ANY,
            ),
            mock.call(
                "any",
                {"files-expected": len(images), "files-seen": len(images)},
                transaction=mock.ANY,
            ),
            mock.call(
                "finally",
                {
                    "files-expected": len(images),
                    "files-seen": len(images),
                    "success": True,
                },
                transaction=mock.ANY,
            ),
        ],
        any_order=True,
    )


def test_filewatcher_watch_list(mocker, tmpdir):
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
            "burst-wait": 5,
        },
        output={"any": 1},
    )
    rw = RecipeWrapper(message=m, transport=t)
    # Spy on the rw.send_to method
    send_to = mocker.spy(rw, "send_to")
    checkpoint = mocker.spy(rw, "checkpoint")
    filewatcher.watch_files(rw, {"some": "header"}, mock.sentinel.message)
    checkpoint.assert_any_call(
        {"filewatcher-status": {"seen-files": 0, "start-time": mock.ANY}},
        delay=5,
        transaction=mock.ANY,
    )
    for i, f in enumerate(files):
        f.write("content")
        filewatcher.watch_files(
            rw, {"some": "header"}, t.send.mock_calls[-1].args[1]["payload"]
        )
        if i == 0:
            send_to.assert_any_call(
                "first", {"file": f, "file-list-index": i + 1}, transaction=mock.ANY
            )
        send_to.assert_has_calls(
            [
                mock.call(
                    "every", {"file": f, "file-list-index": i + 1}, transaction=mock.ANY
                ),
                mock.call(
                    i + 1, {"file": f, "file-list-index": i + 1}, transaction=mock.ANY
                ),
                mock.call(
                    f"{i + 1}",
                    {"file": f, "file-list-index": i + 1},
                    transaction=mock.ANY,
                ),
            ],
            any_order=True,
        )
    send_to.assert_has_calls(
        [
            mock.call(
                "last",
                {"file": files[-1], "file-list-index": len(files)},
                transaction=mock.ANY,
            ),
            mock.call(
                "any",
                {"files-expected": len(files), "files-seen": len(files)},
                transaction=mock.ANY,
            ),
            mock.call(
                "finally",
                {
                    "files-expected": len(files),
                    "files-seen": len(files),
                    "success": True,
                },
                transaction=mock.ANY,
            ),
        ],
        any_order=True,
    )


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
            "log-timeout-as-info": True,
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
                "first", {"file": files[0], "file-list-index": 1}, transaction=mock.ANY
            ),
            mock.call(
                "every", {"file": files[0], "file-list-index": 1}, transaction=mock.ANY
            ),
            mock.call(
                1, {"file": files[0], "file-list-index": 1}, transaction=mock.ANY
            ),
            mock.call(
                "1", {"file": files[0], "file-list-index": 1}, transaction=mock.ANY
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


def test_parse_selections():
    assert DLSFileWatcher._parse_selections({"select": 2}) == {}
    assert DLSFileWatcher._parse_selections({"select-1": 3}) == {1: "select-1"}
    assert DLSFileWatcher._parse_selections({"select-2": 4}) == {2: "select-2"}


@pytest.mark.parametrize(
    "nth_file,expected",
    [
        (1, ["first", "every", 1, "1", "select-2", "every-3"]),
        (2, ["every", 2, "2"]),
        (4, ["every", 4, "4", "every-3"]),
        (9, ["every", 9, "9", "select-2", "last"]),
    ],
)
def test_notify_for_found_file(nth_file, expected):
    notify_function = mock.Mock()
    DLSFileWatcher._notify_for_found_file(
        nth_file=nth_file,
        filecount=9,
        selections={2: "select-2"},
        everys={3: "every-3"},
        notify_function=notify_function,
    )
    for notify in expected:
        notify_function.assert_any_call(notify)
    for notify in {"first", "every", nth_file, "select-2", "every-3", "last"} - set(
        expected
    ):
        with pytest.raises(AssertionError):
            notify_function.assert_any_call(notify)
