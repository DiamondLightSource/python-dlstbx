from unittest import mock
import procrunner
import pytest
import py.path
import workflows
import workflows.transport.common_transport
import workflows.recipe.wrapper


@pytest.fixture
def make_wrapper(mocker, monkeypatch, tmpdir):
    def _make_wrapper(
        wrapper,
        recipe_wrapper,
        expected_output_files=None,
        expected_output_directories=None,
        expected_individual_files=None,
        expected_procrunner_calls=None,
    ):
        return WrapperWrapper(
            wrapper,
            mocker,
            monkeypatch,
            recipe_wrapper,
            expected_output_files=expected_output_files,
            expected_output_directories=expected_output_directories,
            expected_individual_files=expected_individual_files,
            expected_procrunner_calls=expected_procrunner_calls,
        )

    return _make_wrapper


class WrapperWrapper:
    def __init__(
        self,
        wrapper,
        mocker,
        monkeypatch,
        recipe_wrapper,
        expected_output_files=None,
        expected_output_directories=None,
        expected_individual_files=None,
        expected_procrunner_calls=None,
    ):
        self.expected_output_files = expected_output_files
        self.expected_output_directories = expected_output_directories
        self.expected_individual_files = expected_individual_files
        self.expected_procrunner_calls = expected_procrunner_calls
        self.setup_mocks(mocker, monkeypatch)

        # construct the wrapper and set the recipe wrapper
        self.wrapper = wrapper()
        self.recwrap = workflows.recipe.wrapper.RecipeWrapper(
            message=recipe_wrapper, transport=self._mock_transport
        )
        self.wrapper.set_recipe_wrapper(self.recwrap)
        job_parameters = self.wrapper.recwrap.recipe_step["job_parameters"]
        self.working_directory = py.path.local(job_parameters.get("working_directory"))
        self.results_directory = py.path.local(job_parameters.get("results_directory"))

    def run(self):
        # actually run the wrapper
        self.wrapper.run()

    def setup_mocks(self, mocker, monkeypatch):
        # setup various mocked functionality
        self._mock_symlink = mocker.patch(
            "dlstbx.util.symlink.create_parent_symlink", autospec=True
        )
        self._mock_procrunner = mocker.patch.object(procrunner, "run", autospec=True)
        self._mock_procrunner.return_value = procrunner.ReturnObject(
            exitcode=0, runtime=10, timeout=None
        )
        self._mock_ensure = mocker.patch.object(py.path.local, "ensure", autospec=True)
        self._mock_ensure.return_value = True
        self._mock_copy = mocker.patch.object(py.path.local, "copy", autospec=True)

        def mock_check(this, file=1, exists=1):
            if this.ext == ".error":
                return False
            else:
                return True

        monkeypatch.setattr(py.path.local, "check", mock_check)

        def mock_listdir(this, fil=None, sort=None):
            if (
                self.expected_output_directories
                and this.basename in self.expected_output_directories
            ):
                return [
                    this.join(f)
                    for f in self.expected_output_directories[this.basename]
                ]
            else:
                return [this.join(f) for f in self.expected_output_files]

        monkeypatch.setattr(py.path.local, "listdir", mock_listdir)

        self._mock_transport = mock.create_autospec(
            workflows.transport.common_transport.CommonTransport
        )
        self._mock_send_to = mocker.patch.object(
            workflows.recipe.wrapper.RecipeWrapper, "send_to"
        )

    def verify(self):
        if self.expected_procrunner_calls is not None:
            self._mock_procrunner.assert_has_calls(self.expected_procrunner_calls)

        create_symlink = self.wrapper.recwrap.recipe_step["job_parameters"].get(
            "create_symlink"
        )
        if create_symlink:
            # test expected symlink calls
            self._mock_symlink.assert_has_calls(
                [
                    mock.call(self.working_directory.strpath, create_symlink),
                    mock.call(self.results_directory.strpath, create_symlink),
                ]
            )

        expected_count = 0
        if self.expected_output_files is not None:
            expected_count += len(self.expected_output_files)
        if self.expected_output_directories is not None:
            expected_count += len(self.expected_output_directories)
        # check expected results files were copied to results directory
        assert self._mock_copy.call_count == expected_count

        if self.expected_individual_files:
            for file_type, files in self.expected_individual_files.items():
                for file_name in files:
                    file_full = self.results_directory.join(file_name)
                    self._mock_send_to.assert_any_call(
                        "result-individual-file",
                        {
                            "file_type": file_type,
                            "file_name": file_full.basename,
                            "file_path": file_full.dirname,
                        },
                    )
        self._mock_send_to.assert_any_call("result-all-files", {"filelist": mock.ANY})
        self._mock_send_to.assert_any_call("ispyb", {"ispyb_command_list": mock.ANY})
