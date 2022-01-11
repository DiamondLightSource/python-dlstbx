from __future__ import annotations


def load_all_tests():
    """Import all python files (except test_*) in directories. This is required
    for registration of system tests.
    :param paths: A path or list of paths containing files to import.
    """
    import importlib
    import pkgutil

    for _, name, _ in pkgutil.iter_modules(__path__):
        if not name.startswith("test_"):
            importlib.import_module("." + name, __name__)


def get_all_tests():
    import dlstbx.system_test.common

    return dlstbx.system_test.common.CommonSystemTest.register
