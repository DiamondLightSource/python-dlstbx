from __future__ import annotations

from pathlib import Path
from pprint import pformat

import workflows.recipe
from minio.error import S3Error
from workflows.services.common_service import CommonService

from dlstbx.util.iris import (
    get_minio_client,
    get_presigned_urls,
    remove_objects_from_s3,
    retrieve_results_from_s3,
    update_dcid_info_file,
)


class S3EchoUploader(CommonService):
    """
    A service that transfers data between S3 Echo object store and DLS filesystems.
    """

    # Human readable service name
    _service_name = "S3EchoUploader"

    # Logger name
    _logger_name = "dlstbx.services.s3echouploader"

    # STFC S3 Echo credentials
    _s3echo_credentials = "/dls_sw/apps/zocalo/secrets/credentials-echo-mx.cfg"

    def initializing(self):
        """
        Register callback functions to upload and download data from  S3 Echo object store.
        """
        self.log.info(f"{S3EchoUploader._service_name} starting")

        self._message_delay = 5

        workflows.recipe.wrap_subscribe(
            self._transport,
            "s3echo.upload",
            self.on_upload,
            acknowledgement=True,
            log_extender=self.extend_log,
        )

        workflows.recipe.wrap_subscribe(
            self._transport,
            "s3echo.download",
            self.on_download,
            acknowledgement=True,
            log_extender=self.extend_log,
        )

    def on_upload(self, rw, header, message):
        """
        Upload images from DLS filesystem to S3 Echo object store.
        """
        # Conditionally acknowledge receipt of the message
        txn = rw.transport.transaction_begin(subscription_id=header["subscription"])
        rw.transport.ack(header, transaction=txn)

        params = rw.recipe_step["parameters"]
        dcid = int(params["dcid"])
        minio_client = get_minio_client(S3EchoUploader._s3echo_credentials)

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
        try:
            filename = next(iter(upload_file_list))
            filepath = s3echo_upload_files.get(filename)
        except StopIteration:
            self.log.exception(
                f"No more files to upload to S3 bucket {params['bucket']} after receiving following file list:\n{s3_urls}"
            )
            rw.send_to("success", message, transaction=txn)
        else:
            try:
                upload_s3_url = get_presigned_urls(
                    minio_client,
                    params["bucket"],
                    params["dcid"],
                    [
                        filepath,
                    ],
                    True,
                    self.log,
                )
            except S3Error as err:
                update_dcid_info_file(
                    minio_client, params["bucket"], dcid, -1, None, self.log
                )
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
                if len(s3_urls) < len(s3echo_upload_files):
                    rw.checkpoint(
                        {"s3_urls": s3_urls},
                        delay=self._message_delay,
                        transaction=txn,
                    )
                else:
                    update_dcid_info_file(
                        minio_client, params["bucket"], dcid, 1, None, self.log
                    )
                    rw.environment["s3_urls"] = s3_urls
                    rw.send_to("success", "Finished processing", transaction=txn)
        # Commit transaction
        rw.transport.transaction_commit(txn)

    def on_download(self, rw, header, message):
        """
        Download files from S3 Echo object store to DLS filesystem. Remove image files, if requested.
        """
        # Conditionally acknowledge receipt of the message
        txn = rw.transport.transaction_begin(subscription_id=header["subscription"])
        rw.transport.ack(header, transaction=txn)

        params = rw.recipe_step["parameters"]
        minio_client = get_minio_client(S3EchoUploader._s3echo_credentials)
        try:
            retrieve_results_from_s3(
                minio_client,
                params["bucket"],
                Path(params["working_directory"]),
                params["rpid"],
                params["filename"],
                self.log,
            )
        except S3Error:
            self.log.exception(
                f"Error reading {params['rpid']}_{params['filename']} from S3 bucket {params['bucket']}"
            )
            rw.send_to("failure", message, transaction=txn)
        else:
            rw.send_to("success", message, transaction=txn)

        # For downstream tasks processing data are removed here as uploads are done per prcessing job.
        # For data reduction tasks data is shared between different pipelines as removed by S3EchoCollector service.
        if s3_urls := rw.environment.get("s3_urls") and params.get("cleanup", True):
            remove_objects_from_s3(minio_client, params["bucket"], s3_urls, self.log)

        # Commit transaction
        rw.transport.transaction_commit(txn)
