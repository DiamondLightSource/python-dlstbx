import collections
import pytest
import os
import threading
import time
from unittest import mock

import workflows.transport.common_transport
from workflows.recipe.wrapper import RecipeWrapper

import dlstbx.services.filewatcher
from dlstbx.services.filewatcher import DLSFileWatcher
from dlstbx.swmr import h5maker


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
            3: {"service": "DLS Per-Image-Analysis", "queue": "transient.output"},
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


def test_filewatcher_watch_pattern(mocker, tmp_path):
    mock_transport = mock.Mock()
    filewatcher = DLSFileWatcher()
    setattr(filewatcher, "_transport", mock_transport)
    filewatcher.initializing()
    t = mock.create_autospec(workflows.transport.common_transport.CommonTransport)
    pattern = "image%06d"
    image_ids = range(5, 11)
    images = [tmp_path / (pattern % i) for i in image_ids]
    m = generate_recipe_message(
        parameters={
            "pattern": os.fspath(tmp_path / pattern),
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
        image.write_text("content")
        filewatcher.watch_files(
            rw, {"some": "header"}, t.send.mock_calls[-1].args[1]["payload"]
        )
        if image_id == image_ids[0]:
            send_to.assert_any_call(
                "first",
                {
                    "file": str(image),
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
                        "file": str(image),
                        "file-number": i + 1,
                        "file-pattern-index": image_ids[i],
                    },
                    transaction=mock.ANY,
                ),
                mock.call(
                    i + 1,
                    {
                        "file": str(image),
                        "file-number": i + 1,
                        "file-pattern-index": image_ids[i],
                    },
                    transaction=mock.ANY,
                ),
                mock.call(
                    f"{i + 1}",
                    {
                        "file": str(image),
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
                    "file": str(images[-1]),
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


def test_filewatcher_watch_list(mocker, tmp_path):
    mock_transport = mock.Mock()
    filewatcher = DLSFileWatcher()
    setattr(filewatcher, "_transport", mock_transport)
    filewatcher.initializing()
    t = mock.create_autospec(workflows.transport.common_transport.CommonTransport)
    files = [tmp_path / "header", tmp_path / "end"]
    m = generate_recipe_message(
        parameters={
            "list": [str(f) for f in files],
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
        f.write_text("content")
        filewatcher.watch_files(
            rw, {"some": "header"}, t.send.mock_calls[-1].args[1]["payload"]
        )
        if i == 0:
            send_to.assert_any_call(
                "first",
                {"file": str(f), "file-list-index": i + 1},
                transaction=mock.ANY,
            )
        send_to.assert_has_calls(
            [
                mock.call(
                    "every",
                    {"file": str(f), "file-list-index": i + 1},
                    transaction=mock.ANY,
                ),
                mock.call(
                    i + 1,
                    {"file": str(f), "file-list-index": i + 1},
                    transaction=mock.ANY,
                ),
                mock.call(
                    f"{i + 1}",
                    {"file": str(f), "file-list-index": i + 1},
                    transaction=mock.ANY,
                ),
            ],
            any_order=True,
        )
    send_to.assert_has_calls(
        [
            mock.call(
                "last",
                {"file": str(files[-1]), "file-list-index": len(files)},
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


def test_filewatcher_watch_pattern_timeout(mocker, tmp_path):
    mock_transport = mock.Mock()
    filewatcher = DLSFileWatcher()
    setattr(filewatcher, "_transport", mock_transport)
    filewatcher.initializing()
    t = mock.create_autospec(workflows.transport.common_transport.CommonTransport)
    pattern = "image%06d"
    images = [tmp_path / (pattern % (i + 1)) for i in range(10)]
    m = generate_recipe_message(
        parameters={
            "pattern": str(tmp_path / pattern),
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
    images[0].write_text("content")
    filewatcher.watch_files(
        rw, {"some": "header"}, t.send.mock_calls[-1].args[1]["payload"]
    )
    send_to.assert_has_calls(
        [
            mock.call(
                "first",
                {"file": str(images[0]), "file-number": 1, "file-pattern-index": 1},
                transaction=mock.ANY,
            ),
            mock.call(
                "every",
                {"file": str(images[0]), "file-number": 1, "file-pattern-index": 1},
                transaction=mock.ANY,
            ),
            mock.call(
                1,
                {"file": str(images[0]), "file-number": 1, "file-pattern-index": 1},
                transaction=mock.ANY,
            ),
            mock.call(
                "1",
                {"file": str(images[0]), "file-number": 1, "file-pattern-index": 1},
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
                    "file": str(images[1]),
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


def test_filewatcher_watch_list_timeout(mocker, tmp_path):
    mock_transport = mock.Mock()
    filewatcher = DLSFileWatcher()
    setattr(filewatcher, "_transport", mock_transport)
    filewatcher.initializing()
    t = mock.create_autospec(workflows.transport.common_transport.CommonTransport)
    files = [tmp_path / "header", tmp_path / "end"]
    m = generate_recipe_message(
        parameters={
            "list": [str(f) for f in files],
            "timeout": 0.5,
            "log-timeout-as-info": True,
        },
        output={"any": 1},
    )
    rw = RecipeWrapper(message=m, transport=t)
    send_to = mocker.spy(rw, "send_to")
    filewatcher.watch_files(rw, {"some": "header"}, mock.sentinel.message)
    files[0].write_text("content")
    filewatcher.watch_files(
        rw, {"some": "header"}, t.send.mock_calls[-1].args[1]["payload"]
    )
    send_to.assert_has_calls(
        [
            mock.call(
                "first",
                {"file": str(files[0]), "file-list-index": 1},
                transaction=mock.ANY,
            ),
            mock.call(
                "every",
                {"file": str(files[0]), "file-list-index": 1},
                transaction=mock.ANY,
            ),
            mock.call(
                1, {"file": str(files[0]), "file-list-index": 1}, transaction=mock.ANY
            ),
            mock.call(
                "1", {"file": str(files[0]), "file-list-index": 1}, transaction=mock.ANY
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
                {"file": str(files[1]), "file-list-index": 2, "success": False},
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


@pytest.mark.parametrize("select_n_images", (151, 250))
def test_file_selection(select_n_images):
    select_n_images = 250
    for filecount in list(range(1, 255)) + list(range(3600, 3700)):
        selection = lambda x: dlstbx.services.filewatcher.is_file_selected(
            x, select_n_images, filecount
        )
        l = list(filter(selection, range(1, filecount + 1)))

        # Check that correct number of images were selected
        assert len(l) == min(filecount, select_n_images)

        # Check that selection was evenly distributed
        if filecount > 1:
            diffs = [n - l[i - 1] for i, n in enumerate(l) if i]
            assert 1 <= len(collections.Counter(diffs)) <= 2, (filecount, diffs)


def test_filewatcher_watch_swmr(mocker, tmp_path):
    h5_prefix = tmp_path / "foo"
    master_h5 = os.fspath(h5_prefix) + "_master.h5"

    delay = 2
    per_image_delay = 0.1

    x = threading.Thread(
        target=h5maker.main,
        args=(h5_prefix,),
        kwargs=dict(
            block_size=10,
            nblocks=10,
            delay=delay,
            per_image_delay=per_image_delay,
            shuffle=False,
        ),
    )
    x.start()

    mock_transport = mocker.Mock()
    filewatcher = DLSFileWatcher()
    setattr(filewatcher, "_transport", mock_transport)
    filewatcher.initializing()
    t = mocker.create_autospec(workflows.transport.common_transport.CommonTransport)
    m = generate_recipe_message(
        parameters={
            "hdf5": master_h5,
            "expected-per-image-delay": "0.01",
            "timeout": 10,
            "log-timeout-as-info": True,
        },
        output={
            "any": 2,
            "select-10": 3,
        },
    )
    rw = RecipeWrapper(message=m, transport=t)
    # Spy on the rw.send_to method
    send_to = mocker.spy(rw, "send_to")
    time.sleep(delay)
    filewatcher.watch_files(rw, {"some": "header"}, mocker.sentinel.message)
    for i in range(100):
        time.sleep(per_image_delay + 0.01)
        filewatcher.watch_files(
            rw, {"some": "header"}, t.send.mock_calls[-1].args[1]["payload"]
        )
    x.join()
    send_to.assert_any_call(
        "first",
        {
            "hdf5": master_h5,
            "hdf5-index": 0,
            "file": master_h5,
            "file-number": 0,
            "parameters": {"scan_range": "1,1"},
        },
        transaction=mocker.ANY,
    )
    for i in range(100):
        send_to.assert_has_calls(
            [
                mocker.call(
                    i + 1,
                    {
                        "hdf5": master_h5,
                        "hdf5-index": i,
                        "file": master_h5,
                        "file-number": i,
                        "parameters": {"scan_range": f"{i+1},{i+1}"},
                    },
                    transaction=mocker.ANY,
                ),
                mocker.call(
                    f"{i+1}",
                    {
                        "hdf5": master_h5,
                        "hdf5-index": i,
                        "file": master_h5,
                        "file-number": i,
                        "parameters": {"scan_range": f"{i+1},{i+1}"},
                    },
                    transaction=mocker.ANY,
                ),
                mocker.call(
                    "every",
                    {
                        "hdf5": master_h5,
                        "hdf5-index": i,
                        "file": master_h5,
                        "file-number": i,
                        "parameters": {"scan_range": f"{i+1},{i+1}"},
                    },
                    transaction=mocker.ANY,
                ),
            ],
            any_order=True,
        )
    send_to.assert_has_calls(
        [
            mocker.call(
                "last",
                {
                    "hdf5": master_h5,
                    "hdf5-index": 99,
                    "file": master_h5,
                    "file-number": i,
                    "parameters": {"scan_range": f"100,100"},
                },
                transaction=mocker.ANY,
            ),
            mocker.call(
                "any",
                {"images-expected": 100, "images-seen": 100},
                transaction=mocker.ANY,
            ),
            mocker.call(
                "finally",
                {"images-expected": 100, "images-seen": 100, "success": True},
                transaction=mocker.ANY,
            ),
        ],
        any_order=True,
    )
    for i in (0, 11, 22, 33, 44, 55, 66, 77, 88, 99):
        send_to.assert_any_call(
            "select-10",
            {
                "hdf5": master_h5,
                "hdf5-index": i,
                "file": master_h5,
                "file-number": i,
                "parameters": {"scan_range": f"{i+1},{i+1}"},
            },
            transaction=mocker.ANY,
        )


def test_filewatcher_watch_swmr_timeout(mocker, tmp_path):
    h5_prefix = tmp_path / "foo"
    master_h5 = os.fspath(h5_prefix) + "_master.h5"

    mock_transport = mocker.Mock()
    filewatcher = DLSFileWatcher()
    setattr(filewatcher, "_transport", mock_transport)
    filewatcher.initializing()
    t = mocker.create_autospec(workflows.transport.common_transport.CommonTransport)
    m = generate_recipe_message(
        parameters={
            "hdf5": master_h5,
            "timeout": 0.5,
            "log-timeout-as-info": True,
        },
        output={
            "any": 2,
        },
    )
    rw = RecipeWrapper(message=m, transport=t)
    # Spy on the rw.send_to method
    send_to = mocker.spy(rw, "send_to")
    filewatcher.watch_files(rw, {"some": "header"}, mocker.sentinel.message)
    time.sleep(2)
    filewatcher.watch_files(
        rw, {"some": "header"}, t.send.mock_calls[-1].args[1]["payload"]
    )
    send_to.assert_has_calls(
        [
            mocker.call(
                "timeout",
                {"file": master_h5, "hdf5-index": 0, "success": False},
                transaction=mocker.ANY,
            ),
            mocker.call(
                "finally",
                {"images-expected": None, "images-seen": 0, "success": False},
                transaction=mocker.ANY,
            ),
        ],
    )

    send_to.reset_mock()
    h5maker.main(h5_prefix, block_size=2, nblocks=2)
    data_h5 = h5_prefix.with_name(h5_prefix.name + "_000000.h5")
    data_h5.unlink()
    filewatcher.watch_files(rw, {"some": "header"}, mocker.sentinel.message)
    time.sleep(2)
    filewatcher.watch_files(
        rw, {"some": "header"}, t.send.mock_calls[-1].args[1]["payload"]
    )
    send_to.assert_has_calls(
        [
            mocker.call(
                "timeout",
                {"file": master_h5, "hdf5-index": 0, "success": False},
                transaction=mocker.ANY,
            ),
            mocker.call(
                "finally",
                {"images-expected": 4, "images-seen": 0, "success": False},
                transaction=mocker.ANY,
            ),
        ]
    )


def test_filewatcher_watch_swmr_h5py_error(mocker, tmp_path, caplog):
    # Test that the filewatcher gracefully handles errors reading h5py files
    h5_prefix = tmp_path / "foo"
    master_h5 = os.fspath(h5_prefix) + "_master.h5"

    mock_transport = mocker.Mock()
    filewatcher = DLSFileWatcher()
    setattr(filewatcher, "_transport", mock_transport)
    filewatcher.initializing()
    t = mocker.create_autospec(workflows.transport.common_transport.CommonTransport)
    m = generate_recipe_message(
        parameters={
            "hdf5": master_h5,
        },
        output={},
    )
    rw = RecipeWrapper(message=m, transport=t)
    with open(master_h5, "w") as fh:
        fh.write("content")
    filewatcher.watch_files(rw, {"some": "header"}, mocker.sentinel.message)
    assert f"Error reading {master_h5}" in caplog.text
    t.nack.assert_called_once()

    t.reset_mock()
    h5maker.main(h5_prefix, block_size=2, nblocks=2)
    data_h5 = h5_prefix.with_name(h5_prefix.name + "_000000.h5")
    data_h5.write_text("content")
    filewatcher.watch_files(rw, {"some": "header"}, mocker.sentinel.message)
    assert f"Error reading {data_h5}" in caplog.text
    t.nack.assert_called_once()
