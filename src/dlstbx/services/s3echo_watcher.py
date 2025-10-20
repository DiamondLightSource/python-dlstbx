from __future__ import annotations

from pathlib import Path
from pprint import pformat

import minio
import workflows.recipe
from minio.error import S3Error
from workflows.services.common_service import CommonService

from dlstbx.util.iris import (
    get_minio_client,
    get_presigned_urls,
)


class S3EchoWatcher(CommonService):
    """
    A service that watches file upload status to S3 Echo object store.
    """

    # Human readable service name
    _service_name = "S3EchoWatcher"

    # Logger name
    _logger_name = "dlstbx.services.s3echowatcher"

    # STFC S3 Echo credentials
    _s3echo_credentials = "/dls_sw/apps/zocalo/secrets/credentials-echo-mx.cfg"

    def initializing(self):
        """
        Register callback functions to upload and download data from  S3 Echo object store.
        """
        self.log.info(f"{S3EchoWatcher._service_name} starting")

        self.minio_client: minio.Minio = get_minio_client(
            S3EchoWatcher._s3echo_credentials
        )

        self._message_delay = 5

        workflows.recipe.wrap_subscribe(
            self._transport,
            "s3echo.watch",
            self.watch,
            acknowledgement=True,
            log_extender=self.extend_log,
        )

    def watch(self, rw, header, message):
        """
        Upload images from DLS filesystem to S3 Echo object store.
        """
        # Conditionally acknowledge receipt of the message
        txn = rw.transport.transaction_begin(subscription_id=header["subscription"])
        rw.transport.ack(header, transaction=txn)

        params = rw.recipe_step["parameters"]
        minio_client = get_minio_client(S3EchoWatcher._s3echo_credentials)

        bucket_name = params["bucket"]
        if not minio_client.bucket_exists(bucket_name):
            # Stop processing message. No files to watch
            rw.transport.transaction_commit(txn)
            return

        # We have a list of files to upload set in recipe environment and we receive
        # a list of already uploaded files via message. To find which files still
        # need to be uploaded to S3 Echo we pattern match filenames of all files
        # set in environment to list of uploaded file names in the message that
        # are prefixed with processingjobid value.
        s3echo_upload_files = rw.environment.get("s3echo_upload", {})
        upload_file_list = s3echo_upload_files.keys()
        if s3_urls := message.get("s3_urls", {}) if isinstance(message, dict) else {}:
            upload_file_list = sorted(
                {
                    file_name
                    for file_name in s3echo_upload_files
                    if all(file_name not in upload_name for upload_name in s3_urls)
                }
            )
        filename = next(iter(upload_file_list))
        self.log.debug(f"Looking for {filename} file upload status.")
        try:
            result = self.minio_client.stat_object(
                bucket_name, "_".join([params["dcid"], filename])
            )
            filepath = s3echo_upload_files.get(filename)
            file_size = Path(filepath).stat().st_size
        except minio.error.S3Error:
            # File hasn't been uploaded yet
            self.log.debug(f"File {filename} hasn't been uploaded yet.")
            rw.checkpoint(
                message,
                delay=self._message_delay,
                transaction=txn,
            )
            # Stop processing message
            rw.transport.transaction_commit(txn)
            return
        if file_size != result.size:
            # File is still being uploaded
            self.log.debug(f"File {filename} has been partially uploaded.")
            rw.checkpoint(
                message,
                delay=self._message_delay,
                transaction=txn,
            )
            # Stop processing message
            rw.transport.transaction_commit(txn)
            return
        try:
            upload_s3_url = get_presigned_urls(
                minio_client,
                params["bucket"],
                params["dcid"],
                [
                    filepath,
                ],
                False,
                self.log,
            )
        except S3Error as err:
            self.log.exception(
                f"Error uploading following files to S3 bucket {params['bucket']}:\n{pformat(rw.environment['s3echo_upload'])}"
            )
            rw.send_to("failure", message, transaction=txn)
            raise err
        else:
            # If all files have been uploaded, we add dictionary with uploaded file info to the
            # recipe environment and send it to success channel. Otherwise, we upload a single file,
            # add it to the dictionary of uploaded files and checkpoint message containing it.
            s3_urls.update(upload_s3_url)
            self.log.debug(
                f"File {filename} has been uploaded to the S3 Echo object store."
            )
            if len(s3_urls) < len(s3echo_upload_files):
                rw.checkpoint(
                    {"s3_urls": s3_urls},
                    delay=self._message_delay,
                    transaction=txn,
                )
            else:
                rw.environment["s3_urls"] = s3_urls
                rw.send_to("success", "Finished processing", transaction=txn)
        # Commit transaction
        rw.transport.transaction_commit(txn)
