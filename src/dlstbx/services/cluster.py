from __future__ import annotations

import datetime
import errno
import json
import logging
import math
import os
import pathlib
import re
import subprocess
import time
from typing import Optional

import pkg_resources
import pydantic
import requests
import workflows.recipe
from workflows.services.common_service import CommonService
from zocalo.configuration import Configuration
from zocalo.util import slurm

cluster_queue_mapping: dict[str, dict[str, str]] = {
    "cluster": {
        "default": "medium.q",
        "bottom": "bottom.q",
        "low": "low.q",
        "medium": "medium.q",
        "high": "high.q",
        "admin": "admin.q",
        "tempservices": "tempservices.q",
    },
    "testcluster": {
        "default": "test-medium.q",
        "low": "test-low.q",
        "medium": "test-medium.q",
        "high": "test-high.q",
        "admin": "test-admin.q",
    },
}


class JobSubmissionParameters(pydantic.BaseModel):
    scheduler: str = "grid_engine"
    cluster: str = "cluster"
    partition: Optional[str]
    job_name: Optional[str]  #
    environment: Optional[dict[str, str]] = None
    cpus_per_task: Optional[int] = None
    tasks: Optional[int] = None  # slurm only
    nodes: Optional[int]  # slurm only
    memory_per_node: Optional[int] = None  # slurm only
    gpus_per_node: Optional[str] = None  # slurm only
    min_memory_per_cpu: Optional[int] = pydantic.Field(
        None, description="Minimum real memory per cpu (MB)"
    )
    time_limit: Optional[datetime.timedelta] = None
    gpus: Optional[int] = None
    exclusive: bool = False
    account: Optional[str]  # account in slurm terminology
    commands: list[str] | str
    qos: Optional[str]
    queue: Optional[str]  # legacy for grid engine
    qsub_submission_parameters: Optional[str]  # temporary support for legacy recipes
    transfer_input_files: Optional[list[str]]  # datasyncer: list of input objects to
    #                                            transfer from submitter node


class JobSubmissionValidationError(ValueError):
    pass


def format_timedelta_to_HHMMSS(td: datetime.timedelta) -> str:
    td_in_seconds = td.total_seconds()
    hours, remainder = divmod(td_in_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02.0f}:{minutes:02.0f}:{seconds:02.0f}"


units = {"B": 1, "KB": 2**10, "MB": 2**20, "GB": 2**30, "TB": 2**40}


# based on https://stackoverflow.com/a/60708339
def parse_size(size):
    size = size.upper()
    if not re.match(r" ", size):
        size = re.sub(r"([KMGT]?B)", r" \1", size)
    number, unit = [string.strip() for string in size.split()]
    return int(float(number) * units[unit])


def submit_to_grid_engine(
    params: JobSubmissionParameters,
    working_directory: pathlib.Path,
    logger: logging.Logger,
    **kwargs,
) -> int | None:
    # validate
    assert params.cluster is not None
    if params.account and 1 < len(params.account.strip()) and "{" not in params.account:
        if params.cluster == "hamilton" and params.account == "dls":
            raise JobSubmissionValidationError(
                "Project 'dls' is not allowed on Hamilton"
            )
    elif params.cluster == "hamilton":
        raise JobSubmissionValidationError(
            f"No cluster project set for job ({params.account}) on Hamilton. "
            "Cluster project is mandatory for submission."
        )

    submission_params: list
    if params.qsub_submission_parameters:
        submission_params = params.qsub_submission_parameters.split()
    else:
        submission_params = []

    if params.job_name:
        submission_params.extend(["-N", params.job_name])
    if params.cpus_per_task:
        submission_params.extend(["-pe", "smp", str(params.cpus_per_task)])
    if params.min_memory_per_cpu:
        submission_params.extend(["-l", f"mfree={params.min_memory_per_cpu}M"])
    if params.time_limit:
        HHMMSS = format_timedelta_to_HHMMSS(params.time_limit)
        submission_params.extend(["-l", f"h_rt={HHMMSS}"])
    if params.exclusive:
        submission_params.extend(["-l", "exclusive"])
    if params.gpus:
        submission_params.extend(["-l", f"gpus={params.gpus}"])
    if params.account:
        submission_params.extend(["-P", params.account])

    cluster_queue = params.queue
    if cluster_queue is not None:
        mapped_queue = cluster_queue_mapping[params.cluster].get(cluster_queue)
        if mapped_queue:
            logger.debug(
                f"Mapping requested cluster queue {cluster_queue} on cluster {params.cluster} to {mapped_queue}"
            )
        else:
            mapped_queue = cluster_queue_mapping[params.cluster].get("default")
            if mapped_queue:
                logger.info(
                    f"Requested cluster queue {cluster_queue} not available on cluster {params.cluster}, mapping to {mapped_queue} instead"
                )
        if mapped_queue:
            submission_params = ["-q", mapped_queue] + submission_params
        else:
            logger.warning(
                f"Requested cluster queue {cluster_queue} not available on cluster {params.cluster}, no default queue set"
            )

    commands = params.commands
    if not isinstance(commands, str):
        commands = " ".join(commands)

    submission = [
        ". /etc/profile.d/modules.sh",
        "module load global/" + params.cluster,
        # thanks to Modules 3.2 weirdness qsub may now be a function
        # calling the real qsub command, but eating up its parameters.
        "unset -f qsub",
        f"qsub {' '.join(submission_params)} << EOF",
        "#!/bin/bash",
        ". /etc/profile.d/modules.sh",
        "cd " + os.fspath(working_directory),
        commands,
        "EOF",
    ]
    logger.debug(
        f"Cluster ({params.cluster}) submission parameters: {submission_params}"
    )
    logger.debug(f"Commands: {commands}")
    logger.debug(f"Working directory: {working_directory}")
    result = subprocess.run(
        ["/bin/bash"],
        input="\n".join(submission).encode("latin1"),
        cwd=working_directory,
        capture_output=True,
    )
    if result.returncode:
        logger.error(
            "Could not submit cluster job:\n%s\n%s",
            result.stdout.decode("latin1"),
            result.stderr.decode("latin1"),
        )
        return None
    assert b"has been submitted" in result.stdout
    jobnumber = result.stdout.split()[2].decode("latin1")
    return int(jobnumber)


def submit_to_slurm(
    params: JobSubmissionParameters,
    working_directory: pathlib.Path,
    logger: logging.Logger,
    zc: Configuration,
    scheduler: str,
) -> int | None:
    api = slurm.SlurmRestApi.from_zocalo_configuration(zc, cluster=scheduler)

    script = params.commands
    if not isinstance(script, str):
        script = "\n".join(script)
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
        environment = [f"{k}={os.environ[k]}" for k in minimal_environment]

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
        logger.error(f"Failed Slurm job submission: {e}\n" f"{e.response.text}")
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
        legacy_cluster_submission_parameters = parameters.get(
            "cluster_submission_parameters"
        )
        if isinstance(legacy_cluster_submission_parameters, str):
            # String containing Grid Engine submission parameters
            self.log.warning(
                f"Legacy cluster parameters encountered in recipe_ID: {rw.environment['ID']}"
            )
            cluster = parameters["cluster"]
            cluster_submission_parameters = parameters.get(
                "cluster_submission_parameters"
            )
            account = parameters.get("cluster_project")
            queue = parameters.get("cluster_queue")
            commands = parameters.get("cluster_commands")
            params = JobSubmissionParameters(
                scheduler="grid_engine",
                cluster=cluster,
                account=account,
                commands=commands,
                qsub_submission_parameters=cluster_submission_parameters,
                queue=queue,
            )
        else:
            params = JobSubmissionParameters(**parameters.get("cluster", {}))

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
            recipewrapper = parameters["recipewrapper"]
            try:
                self._recursive_mkdir(os.path.dirname(recipewrapper))
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
            params.commands = params.commands.replace("$RECIPEWRAP", recipewrapper)
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
        try:
            working_directory.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            self.log.error(
                "Could not create working directory: %s", str(e), exc_info=True
            )
            self._transport.nack(header)
            return

        if params.transfer_input_files:
            try:
                import datasyncer
            except ImportError:
                self.log.error(
                    "File upload via datasyncer has failed. Cannot import datasyncer module."
                )
                self._transport.nack(header)
                return
            timestamp = time.time()
            transfer_status = "active"
            runtime = 0
            try:
                transfer_id = message["datasyncher"]["transfer_id"]
                transfer_status = datasyncer.status(transfer_id)
                runtime = timestamp - message["datasyncher"]["timestamp"]
            except (TypeError, KeyError):
                transfer_id = datasyncer.transfer(params.transfer_input_files)
                msg = {
                    "datasyncher": {
                        "transfer_id": transfer_id,
                        "timestamp": timestamp,
                    }
                }
                txn = self._transport.transaction_begin(
                    subscription_id=header["subscription"]
                )
                self._transport.ack(header, transaction=txn)
                rw.checkpoint(msg, delay=10, transaction=txn)
                self.log.info(
                    f"Start transfering input files with transfer_id {transfer_id}"
                )
                self._transport.transaction_commit(txn)
                return

            if transfer_status == "succeeded":
                self.log.info(
                    f"Transfering input files for transfer_id {transfer_id} succeeded in {runtime:.2f}s"
                )
            elif transfer_status == "active":
                txn = self._transport.transaction_begin(
                    subscription_id=header["subscription"]
                )
                self._transport.ack(header, transaction=txn)
                rw.checkpoint(message, delay=10, transaction=txn)
                self.log.info(
                    f"Transfering input files for transfer_id {transfer_id} is running for {runtime:.2f}s"
                )
                self._transport.transaction_commit(txn)
                return
            elif transfer_status == "failed":
                self.log.error(
                    f"File upload via datasyncher for transfer_id {transfer_id} failed after {runtime:.2f}s"
                )
                self._transport.nack(header)
                return
            else:
                self.log.error(
                    f"Recieved unknown transfer status value for transfter_id {transfer_id}: {transfer_status}"
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
        self.log.info(f"Submitted job {jobnumber} to {params.cluster}")
