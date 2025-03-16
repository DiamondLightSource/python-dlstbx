from __future__ import annotations

import shutil
import subprocess
from pathlib import Path
from unittest import mock

import pytest
import workflows
import workflows.recipe.wrapper
import workflows.transport.common_transport


@pytest.fixture
def make_wrapper(mocker, monkeypatch, tmpdir):
    def _make_wrapper(
        wrapper,
        recipe_wrapper,
        expected_final_files=None,
        expected_output_files=None,
        expected_output_directories=None,
        expected_individual_files=None,
        expected_subprocess_calls=None,
    ):
        return WrapperWrapper(
            wrapper,
            mocker,
            monkeypatch,
            recipe_wrapper,
            expected_final_files=expected_final_files,
            expected_output_files=expected_output_files,
            expected_output_directories=expected_output_directories,
            expected_individual_files=expected_individual_files,
            expected_subprocess_calls=expected_subprocess_calls,
        )

    return _make_wrapper


class WrapperWrapper:
    def __init__(
        self,
        wrapper,
        mocker,
        monkeypatch,
        recipe_wrapper,
        expected_final_files=None,
        expected_output_files=None,
        expected_output_directories=None,
        expected_individual_files=None,
        expected_subprocess_calls=None,
    ):
        self.expected_final_files = expected_final_files
        self.expected_output_files = expected_output_files
        self.expected_output_directories = expected_output_directories
        self.expected_individual_files = expected_individual_files
        self.expected_subprocess_calls = expected_subprocess_calls
        self.setup_mocks(mocker, monkeypatch)

        # construct the wrapper and set the recipe wrapper
        self.wrapper = wrapper()
        self.recwrap = workflows.recipe.wrapper.RecipeWrapper(
            message=recipe_wrapper, transport=self._mock_transport
        )
        self.wrapper.set_recipe_wrapper(self.recwrap)
        job_parameters = self.wrapper.recwrap.recipe_step["job_parameters"]
        self.working_directory = Path(job_parameters.get("working_directory"))
        self.results_directory = Path(job_parameters.get("results_directory"))
        self.final_directory = Path(job_parameters.get("pipeline-final").get("path"))

    def run(self):
        # actually run the wrapper
        self.wrapper.run()

    def setup_mocks(self, mocker, monkeypatch):
        # setup various mocked functionality
        self._mock_symlink = mocker.patch(
            "dlstbx.util.symlink.create_parent_symlink", autospec=True
        )
        self._mock_subprocess = mocker.patch.object(subprocess, "run", autospec=True)
        self._mock_subprocess.return_value = subprocess.CompletedProcess(
            args=[], returncode=0, stdout=None, stderr=None
        )
        self._mock_mkdir = mocker.patch.object(Path, "mkdir", autospec=True)
        self._mock_copy = mocker.patch.object(shutil, "copy", autospec=True)

        def mock_iterdir(this, fil=None, sort=None):
            if (
                self.expected_output_directories
                and this.name in self.expected_output_directories
            ):
                return [this / f for f in self.expected_output_directories[this.name]]
            else:
                return [this / f for f in self.expected_output_files]

        monkeypatch.setattr(Path, "iterdir", mock_iterdir)

        self._mock_transport = mock.create_autospec(
            workflows.transport.common_transport.CommonTransport
        )
        self._mock_send_to = mocker.patch.object(
            workflows.recipe.wrapper.RecipeWrapper, "send_to"
        )

    def verify(self):
        if self.expected_subprocess_calls is not None:
            self._mock_subprocess.assert_has_calls(self.expected_subprocess_calls)

        create_symlink = self.wrapper.recwrap.recipe_step["job_parameters"].get(
            "create_symlink"
        )
        if create_symlink:
            # test expected symlink calls
            self._mock_symlink.assert_has_calls(
                [
                    mock.call(self.working_directory, create_symlink),
                    mock.call(self.results_directory, create_symlink),
                ]
            )

        expected_count = 0
        if self.expected_final_files is not None:
            expected_count += len(self.expected_final_files)
        if self.expected_output_files is not None:
            expected_count += len(self.expected_output_files)
        if self.expected_output_directories is not None:
            expected_count += len(self.expected_output_directories)
        # check expected results files were copied to results directory
        assert self._mock_copy.call_count == expected_count

        if self.expected_individual_files:
            for file_type, files in self.expected_individual_files.items():
                for file_name in files:
                    if file_name in self.expected_final_files:
                        file_full = self.final_directory / file_name
                    elif file_name in self.expected_output_files:
                        file_full = self.results_directory / file_name
                    self._mock_send_to.assert_any_call(
                        "result-individual-file",
                        {
                            "file_type": file_type,
                            "file_name": file_full.name,
                            "file_path": str(file_full.parent),
                            "importance_rank": mock.ANY,
                        },
                    )
        self._mock_send_to.assert_any_call("result-all-files", {"filelist": mock.ANY})
        self._mock_send_to.assert_any_call("ispyb", {"ispyb_command_list": mock.ANY})
