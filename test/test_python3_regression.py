def test_no_new_python3_incompatible_code_is_introduced_into_this_module():
    import dlstbx
    import pytest
    import libtbx.test_utils.python3_regression as py3test

    result = py3test.find_new_python3_incompatible_code(dlstbx)
    if result is None:
        pytest.skip("No python3 interpreter available")
    elif result:
        pytest.fail(result)
