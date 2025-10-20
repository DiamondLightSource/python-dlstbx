from __future__ import annotations

import io
from pprint import pformat

import minio
import workflows.recipe
from workflows.services.common_service import CommonService

from dlstbx.util import iris
from dlstbx.util.iris import get_minio_client


class S3EchoCollector(CommonService):
    """
    A service that keeps status of uploads to S3 Echo object store and does garbage collection of unreferenced data.
    """

    # Human readable service name
    _service_name = "S3EchoCollector"

    # Logger name
    _logger_name = "dlstbx.services.s3echocollector"

    # STFC S3 Echo credentials
    _s3echo_credentials = "/dls_sw/apps/zocalo/secrets/credentials-echo-mx.cfg"

    def initializing(self):
        """
        Register callback functions to upload and download data from  S3 Echo object store.
        """
        self.log.info(f"{S3EchoCollector._service_name} starting")

        self.minio_client: minio.Minio = get_minio_client(
            S3EchoCollector._s3echo_credentials
        )

        self._message_delay = 5

        workflows.recipe.wrap_subscribe(
            self._transport,
            "s3echo.start",
            self.on_start,
            acknowledgement=True,
            log_extender=self.extend_log,
        )

        workflows.recipe.wrap_subscribe(
            self._transport,
            "s3echo.end",
            self.on_end,
            acknowledgement=True,
            log_extender=self.extend_log,
        )

    def on_start(self, rw, header, message):
        """
        Process request for uploading images to S3 Echo object store.
        """
        # Conditionally acknowledge receipt of the message
        txn = rw.transport.transaction_begin(subscription_id=header["subscription"])
        rw.transport.ack(header, transaction=txn)

        params = rw.recipe_step["parameters"]
        minio_client = get_minio_client(S3EchoCollector._s3echo_credentials)

        bucket_name = params["bucket"]
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)

        # Write {dcid}_info file that contains list of processingjobid values for all job invocations
        # requiring data for a given {dcid} value. Every new processing job invocation adds corresponding
        # processingjobid value to the list.
        obj_pid = f"{params['dcid']}_info"
        dc_objects = [obj.object_name for obj in minio_client.list_objects(bucket_name)]
        pid_list = [params["rpid"]]
        try:
            response = None
            if obj_pid in dc_objects:
                response = minio_client.get_object(bucket_name, obj_pid)
                if response:
                    pid_record = response.data.decode()
                    pid_list.extend(pid_record.split())
            pid_record = " ".join(pid_list)
            str_buffer = io.BytesIO(pid_record.encode())
            buffer_length = str_buffer.getbuffer().nbytes
            result = minio_client.put_object(
                bucket_name, obj_pid, str_buffer, buffer_length
            )
            self.log.debug(
                f"Written {result.object_name} object for {params['rpid']} procecessingJobId value; etag: {result.etag}",
            )
            if images := params.get("images"):
                try:
                    image_files = iris.get_image_files(None, images, self.log)
                    rw.environment.update({"s3echo_upload": image_files})
                except Exception:
                    self.log.exception("Error uploading image files to S3 Echo")
            if response:
                self.log.debug("Sending message to watch endpoint")
                rw.send_to("watch", message, transaction=txn)
            else:
                self.log.debug("Sending message to upload endpoint")
                rw.send_to("upload", message, transaction=txn)
        finally:
            if response:
                response.close()
                response.release_conn()

        rw.transport.transaction_commit(txn)

    def on_end(self, rw, header, message):
        """
        Remove reference to image data in S3 Echo object store after end of processing.
        """
        # Conditionally acknowledge receipt of the message
        txn = rw.transport.transaction_begin(subscription_id=header["subscription"])
        rw.transport.ack(header, transaction=txn)

        params = rw.recipe_step["parameters"]
        minio_client = get_minio_client(S3EchoCollector._s3echo_credentials)
        bucket_name = params["bucket"]
        obj_pid = f"{params['dcid']}_info"
        try:
            pid_list = []
            response = minio_client.get_object(bucket_name, obj_pid)
            if response:
                pid_record = response.data.decode()
                pid_list = pid_record.split()
                self.log.debug(
                    f"List of autoprocprocess_id values for jobs referencing data for dcid {params['dcid']}: {pformat(pid_list)}"
                )
                # If {dcid}_info file contains only processingjobid value of the current job, remove data from
                # S3 Echo object store as no other jobs require data for this dcid. Otherwise, just remove the current
                # processingjobid value from the list.
                if pid_list == [params["rpid"]]:
                    dc_objects = {
                        obj.object_name
                        for obj in minio_client.list_objects(bucket_name)
                        if obj.object_name is not None
                    }
                    for obj_name in dc_objects:
                        if obj_name.startswith(f"{params['dcid']}_"):
                            minio_client.remove_object(bucket_name, obj_name)
                else:
                    pid_list.remove(params["rpid"])
                    pid_record = " ".join(pid_list)
                    str_buffer = io.BytesIO(pid_record.encode())
                    buffer_length = str_buffer.getbuffer().nbytes
                    minio_client.put_object(
                        bucket_name, obj_pid, str_buffer, buffer_length
                    )
        finally:
            if response:
                response.close()
                response.release_conn()

        rw.transport.transaction_commit(txn)
