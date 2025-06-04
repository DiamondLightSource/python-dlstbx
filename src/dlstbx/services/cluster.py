from __future__ import annotations

import datetime
import errno
import getpass
import json
import logging
import math
import os
import pathlib
import re
from typing import Optional

import pkg_resources
import pydantic
import requests
import workflows.recipe
from workflows.services.common_service import CommonService
from zocalo.configuration import Configuration
from zocalo.util import slurm


class JobSubmissionParameters(pydantic.BaseModel):
    scheduler: str = "slurm"
    partition: str = "cs04r"
    job_name: Optional[str] = None  #
    environment: Optional[dict[str, str]] = None
    cpus_per_task: Optional[int] = None
    tasks: Optional[int] = None  # slurm only
    nodes: Optional[int] = None  # slurm only
    memory_per_node: Optional[int] = None  # slurm only
    gpus_per_node: Optional[int] = None  # slurm only
    min_memory_per_cpu: Optional[int] = pydantic.Field(
        None, description="Minimum real memory per cpu (MB)"
    )
    time_limit: Optional[datetime.timedelta] = None
    gpus: Optional[int] = None
    exclusive: bool = False
    account: Optional[str] = None  # account in slurm terminology
    commands: list[str] | str
    qos: Optional[str] = None
    qsub_submission_parameters: Optional[str] = (
        None  # temporary support for legacy recipes
    )


class JobSubmissionValidationError(ValueError):
    pass


def submit_to_slurm(
    params: JobSubmissionParameters,
    working_directory: pathlib.Path,
    logger: logging.Logger,
    zc: Configuration,
    scheduler: str,
    recipewrapper: str,
) -> int | None:
    api = slurm.SlurmRestApi.from_zocalo_configuration(zc, cluster=scheduler)
    script = params.commands
    if not isinstance(script, str):
        script = "\n".join(script)
    if scheduler == "iris":
        tmp_script = [
            "#!/bin/bash",
            f"cat > {pathlib.Path(recipewrapper).name} << 'EOF'",
        ]
        with open(recipewrapper) as fp:
            tmp_script.extend(fp.readlines())
        tmp_script.append(f"EOF\n{script}")
        script = "\n".join(tmp_script)
    else:
        script = f"#!/bin/bash\n. /etc/profile.d/modules.sh\n{script}"

    if params.environment:
        environment = [f"{k}={v}" for k, v in params.environment.items()]
    else:
        # The environment must not be empty, see
        # https://github.com/DiamondLightSource/python-dlstbx/pull/228.
        # If a recipe requires a environment variable, add it to minimal_environment here.
        minimal_environment = {"USER"}
        # Only attempt to copy variables that already exist in the submitter's environment.
        minimal_environment &= set(os.environ)
        environment = [f"{k}={os.environ[k]}" for k in minimal_environment] or [
            "USER=gda2"
        ]
    # Account needs to be set to the user name if not running as gda2
    if api.user_name != "gda2":
        params.account = api.user_name

    logger.debug(f"Submitting script to Slurm:\n{script}")
    jdm_params = {
        "account": params.account,
        "cpus_per_task": params.cpus_per_task,
        "current_working_directory": os.fspath(working_directory),
        "environment": environment,
        "name": params.job_name,
        "nodes": str(params.nodes) if params.nodes else params.nodes,
        "partition": params.partition,
        "qos": params.qos,
        "tasks": params.tasks,
    }
    if params.min_memory_per_cpu:
        jdm_params["memory_per_cpu"] = slurm.models.Uint64NoVal(
            number=params.min_memory_per_cpu, set=True
        )
    if params.memory_per_node:
        jdm_params["memory_per_node"] = slurm.models.Uint64NoVal(
            number=params.memory_per_node, set=True
        )
    if params.time_limit:
        time_limit_minutes = math.ceil(params.time_limit.total_seconds() / 60)
        jdm_params["time_limit"] = slurm.models.Uint32NoVal(
            number=time_limit_minutes, set=True
        )
    if params.gpus_per_node:
        jdm_params["tres_per_node"] = f"gres/gpu:{params.gpus_per_node}"
    if params.gpus:
        jdm_params["tres_per_job"] = f"gres/gpu:{params.gpus}"

    job_submission = slurm.models.JobSubmitReq(
        script=script, job=slurm.models.JobDescMsg(**jdm_params)
    )
    try:
        response = api.submit_job(job_submission)
    except requests.HTTPError as e:
        logger.error(f"Failed Slurm job submission: {e}\n{e.response.text}")
        return None
    if response.error:
        error_message = f"{response.error_code}: {response.error}"
        logger.error(f"Failed Slurm job submission: {error_message}")
        return None
    return response.job_id


class DLSCluster(CommonService):
    """A service to interface zocalo with functions to start new
    jobs on the clusters."""

    # Human readable service name
    _service_name = "DLS Cluster service"

    # Logger name
    _logger_name = "dlstbx.services.cluster"

    def initializing(self):
        """Subscribe to the cluster submission queue.
        Received messages must be acknowledged."""
        self.log.info("Cluster service starting")

        if not self.environment_is_valid():
            self._request_termination()
            return

        self.schedulers = {
            f.name: f.load()
            for f in pkg_resources.iter_entry_points(
                "zocalo.services.cluster.schedulers"
            )
        }
        self.log.debug(f"Supported schedulers: {', '.join(self.schedulers.keys())}")
        workflows.recipe.wrap_subscribe(
            self._transport,
            "cluster.submission",
            self.run_submit_job,
            acknowledgement=True,
            log_extender=self.extend_log,
        )

    def environment_is_valid(self):
        """Check that the cluster submission environment is sane. Specifically, that
        there is no ~/.sge_request file interfering with cluster job submissions.
        """
        sge_file = pathlib.Path("~").expanduser() / ".sge_request"
        if sge_file.exists():
            contents = sge_file.read_bytes().strip()
            if contents:
                self.log.error(
                    "Rejecting service initialisation: file %s is not empty. "
                    "This may interfere with service operation. ",
                    str(sge_file),
                )
                return False

            self.log.info(
                "Note: empty file %s found during service startup", str(sge_file)
            )
        return True

    def modify_commands_to_environment(
        self,
        commands: str | list[str],
        active_envs: list[str],
        dials_dev_command: str | None,
        rabbit_host: str | None,
    ) -> str | list[str]:
        if len(active_envs) == 1:
            active_env = active_envs[0]
            if isinstance(commands, str):
                commands = [commands]
            _updated_commands = []
            dials_replace_patterns = [
                r"\bmodule load dials/latest\b",
                r"\bmodule load dials/nightly\b",
                r"module load dials\b(?!/)",
            ]
            combined_pattern = "|".join(dials_replace_patterns)
            for _command in commands:
                if dials_dev_command and (
                    match := re.search(combined_pattern, _command)
                ):
                    _command = _command.replace(match.group(), dials_dev_command)
                if "dlstbx.wrap" in _command and "-e" not in _command.split():
                    wrap_cmd = f"dlstbx.wrap -e {active_env}"
                    if active_env == "devrmq":
                        wrap_cmd += f" --rabbit-host={rabbit_host}"
                    _command = _command.replace("dlstbx.wrap", wrap_cmd, 1)
                _updated_commands.append(_command)
            commands = _updated_commands
        return commands

    def replace_zocalo_tmp_with_user_tmp(
        self, working_directory: pathlib.Path
    ) -> pathlib.Path:
        """Replace the zocalo tmp directory with the user tmp directory if not running running as gda2"""
        if (
            getpass.getuser() != "gda2"
            and working_directory.parts[:4] == pathlib.Path("/dls/tmp/zocalo").parts
        ):
            working_directory_parts = list(working_directory.parts)
            working_directory_parts.insert(3, getpass.getuser())
            working_directory = pathlib.Path(*working_directory_parts)
            self.log.debug(
                f"Cluster service not running as gda2 and trying to create directory in /dls/tmp/zocalo, using {working_directory} instead"
            )
        return working_directory

    @staticmethod
    def _recursive_mkdir(path):
        try:
            os.makedirs(path)
        except OSError as exc:
            if exc.errno == errno.EEXIST and os.path.isdir(path):
                pass
            else:
                raise

    def run_submit_job(self, rw, header, message):
        """Submit cluster job according to message."""
        parameters = rw.recipe_step["parameters"]
        if parameters.get("cluster_submission_parameters"):
            self.log.error(
                f"Deprecated cluster parameters encountered in recipe_ID: {rw.environment['ID']}"
            )
            self._transport.nack(header)
            return

        try:
            params = JobSubmissionParameters(**parameters.get("cluster", {}))
        except pydantic.ValidationError as e:
            self.log.error(f"Invalid JobSubmissionParameters: {e}")
            self._transport.nack(header)
            return

        if params.scheduler not in self.schedulers.keys():
            self.log.error(
                f"Unsupported cluster scheduler '{params.scheduler}' encountered in recipe_ID: {rw.environment['ID']}"
            )
            self._transport.nack(header)
            return

        is_live_zocalo = self._environment["live"]

        if not is_live_zocalo:
            active_envs = self.config.active_environments
            dials_dev = (
                self.config._plugin_configurations["dials-dev"]
                if "dials-dev" in self.config._plugin_configurations
                else {}
            )
            dials_dev_command = dials_dev.get("dials-command")
            rabbit_host = self.transport.defaults.get("--rabbit-host")
            params.commands = self.modify_commands_to_environment(
                params.commands, active_envs, dials_dev_command, rabbit_host
            )

        if not isinstance(params.commands, str):
            params.commands = "\n".join(params.commands)

        if "recipefile" in parameters:
            recipefile = parameters["recipefile"]
            try:
                self._recursive_mkdir(os.path.dirname(recipefile))
            except OSError as e:
                if e.errno == errno.ENOENT:
                    self.log.error(
                        "Error in underlying filesystem: %s", str(e), exc_info=True
                    )
                    self._transport.nack(header)
                    return
                raise
            self.log.debug("Writing recipe to %s", recipefile)
            params.commands = params.commands.replace("$RECIPEFILE", recipefile)
            with open(recipefile, "w") as fh:
                fh.write(rw.recipe.pretty())
        if "recipeenvironment" in parameters:
            recipeenvironment = parameters["recipeenvironment"]
            try:
                self._recursive_mkdir(os.path.dirname(recipeenvironment))
            except OSError as e:
                if e.errno == errno.ENOENT:
                    self.log.error(
                        "Error in underlying filesystem: %s", str(e), exc_info=True
                    )
                    self._transport.nack(header)
                    return
                raise
            self.log.debug("Writing recipe environment to %s", recipeenvironment)
            params.commands = params.commands.replace("$RECIPEENV", recipeenvironment)
            with open(recipeenvironment, "w") as fh:
                json.dump(
                    rw.environment, fh, sort_keys=True, indent=2, separators=(",", ": ")
                )
        if "recipewrapper" in parameters:
            recipewrapper = pathlib.Path(parameters["recipewrapper"])
            # Replace zocalo tmp with user tmp if not running in live zocalo
            if not is_live_zocalo:
                recipewrapper = self.replace_zocalo_tmp_with_user_tmp(recipewrapper)
            try:
                recipewrapper.parent.mkdir(parents=True, exist_ok=True)
            except OSError as e:
                if e.errno == errno.ENOENT:
                    self.log.error(
                        "Error in underlying filesystem: %s", str(e), exc_info=True
                    )
                else:
                    self.log.error(
                        "Could not create working directory: %s", str(e), exc_info=True
                    )
                self._transport.nack(header)
                return
            self.log.debug("Storing serialized recipe wrapper in %s", recipewrapper)
            params.commands = params.commands.replace(
                "$RECIPEWRAP", recipewrapper.as_posix()
            )
            with open(recipewrapper, "w") as fh:
                json.dump(
                    {
                        "recipe": rw.recipe.recipe,
                        "recipe-pointer": rw.recipe_pointer,
                        "environment": rw.environment,
                        "recipe-path": rw.recipe_path,
                        "payload": rw.payload,
                    },
                    fh,
                    indent=2,
                    separators=(",", ": "),
                )

        if "workingdir" not in parameters or not parameters["workingdir"].startswith(
            "/"
        ):
            self.log.error(
                "No absolute working directory specified. Will not run cluster job"
            )
            self._transport.nack(header)
            return
        working_directory = pathlib.Path(parameters["workingdir"])
        # Replace zocalo tmp with user tmp if not running in live zocalo
        if not is_live_zocalo:
            working_directory = self.replace_zocalo_tmp_with_user_tmp(working_directory)
        try:
            working_directory.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            self.log.error(
                "Could not create working directory: %s", str(e), exc_info=True
            )
            self._transport.nack(header)
            return

        submit_to_scheduler = self.schedulers.get(params.scheduler)

        jobnumber = submit_to_scheduler(
            params,
            working_directory,
            self.log,
            zc=self.config,
            scheduler=params.scheduler,
            recipewrapper=recipewrapper,
        )
        if not jobnumber:
            self._transport.nack(header)
            return

        # Conditionally acknowledge receipt of the message
        txn = self._transport.transaction_begin(subscription_id=header["subscription"])
        self._transport.ack(header, transaction=txn)

        # Send results onwards
        rw.set_default_channel("job_submitted")
        rw.send({"jobid": jobnumber}, transaction=txn)

        # Commit transaction
        self._transport.transaction_commit(txn)
        self.log.info(
            f"Submitted job {jobnumber} to '{params.scheduler}' on partition '{params.partition}'"
        )
