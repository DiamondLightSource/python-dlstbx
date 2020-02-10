from __future__ import absolute_import, division, print_function


def test_can_import_dlstbx_version():
    import dlstbx.command_line.version

    assert dlstbx.command_line.version
