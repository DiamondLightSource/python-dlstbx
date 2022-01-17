from __future__ import annotations


def test_can_import_dlstbx_version():
    import dlstbx.cli.version

    assert dlstbx.cli.version
