from __future__ import annotations

import pytest
import zocalo.configuration

import dlstbx.system_test
from dlstbx.system_test.common import CommonSystemTest


def test_validation_should_fail_on_syntax_error_in_function(mocker):
    """Here we test that the validation of system tests actually works.
    This is as meta as it gets."""
    # Instantiate test class. This should be valid
    zc = mocker.MagicMock(zocalo.configuration.Configuration)
    t = CommonSystemTest(zc)
    t.validate()

    def broken_function():
        undefined_name  # noqa

    setattr(t, "test_function", broken_function)

    # Now validation should fail
    with pytest.raises(NameError):
        t.validate()


def test_validation_should_fail_on_broken_function_signatures(mocker):
    """Here we test that the validation of system tests actually works.
    This is as meta as it gets."""
    # Instantiate test class. This should be valid
    zc = mocker.MagicMock(zocalo.configuration.Configuration)
    t = CommonSystemTest(zc)
    t.validate()

    def valid_function():
        t.send_message(
            queue="somewhere",
            headers={"something": "something"},
            message="more something",
        )

    setattr(t, "test_function", valid_function)
    t.validate()

    # Now validation should fail
    def broken_function():
        t.send_message(
            headers={"something": "something"},
            message="more something",
            undefined_parameter=True,
        )

    setattr(t, "test_function", broken_function)
    with pytest.raises(TypeError):
        t.validate()


def test_validation_must_set_validation_boolean_correctly(mocker):
    """Here we test that the validation of system tests actually works.
    This is as meta as it gets."""
    # Instantiate test class. This should be valid
    zc = mocker.MagicMock(zocalo.configuration.Configuration)
    t = CommonSystemTest(zc)
    t.validate()

    def only_works_under_validation():
        assert t.validation

    setattr(t, "test_function", only_works_under_validation)
    t.validate()
    with pytest.raises(AssertionError):
        t.test_function()

    def never_works_under_validation():
        assert not t.validation

    setattr(t, "test_function", never_works_under_validation)
    with pytest.raises(AssertionError):
        t.validate()
    t.test_function()


def test_validate_all_system_tests():
    """Now check that all defined system tests pass validation, ie. do not
    have syntax errors and properly call messaging functions."""
    dlstbx.system_test.load_all_tests()

    # The dispatcher system tests needs to lookup zocalo.recipe_directory
    zc = zocalo.configuration.from_file()
    zc.activate()

    for name, cls in dlstbx.system_test.get_all_tests().items():
        print("Validating", name)
        cls(zc, target_queue="foo").validate()
