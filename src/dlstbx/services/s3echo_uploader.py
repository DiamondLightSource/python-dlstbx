from __future__ import annotations

from pathlib import Path

import workflows.recipe
from minio.error import S3Error
from workflows.services.common_service import CommonService

from dlstbx.util.iris import (
    get_minio_client,
    get_presigned_urls_images,
    remove_images_from_s3,
    retrieve_results_from_s3,
)


class S3EchoUploader(CommonService):
    """
    A service that transfers data between S3 Echo object store and DLS filesystems.
    """

    # Human readable service name
    _service_name = "S3Echouploader"

    # Logger name
    _logger_name = "dlstbx.services.s3echouploader"

    # STFC S3 Echo credentials
    _s3echo_credentials = "/dls_sw/apps/zocalo/secrets/credentials-echo-mx.cfg"

    def initializing(self):
        """
        Register callback functions to upload and download data from  S3 Echo object store.
        """
        self.log.info(f"{S3EchoUploader._service_name} starting")

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
        minio_client = get_minio_client(S3EchoUploader._s3echo_credentials)
        try:
            s3_urls = get_presigned_urls_images(
                minio_client,
                params["bucket"],
                params["rpid"],
                params["images"],
                self.log,
            )
        except S3Error:
            self.log.exception(
                f"Error writing {params['rpid']}_{params['images']} to S3 bucket {params['bucket']}"
            )
            rw.send_to("failure", message, transaction=txn)
        else:
            rw.environment["s3_urls"] = s3_urls
            rw.send_to("success", message, transaction=txn)

        # Commit transaction
        rw.transport.transaction_commit(txn)

    def on_download(self, rw, header, message):
        """
        Download files from S3 Echo object store tto DLS filesystem. Remove image files, if requested.
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

        if params.get("remove", False):
            try:
                remove_images_from_s3(
                    minio_client,
                    params["bucket"],
                    params["rpid"],
                    params["remove"]["images"],
                    self.log,
                )
            except S3Error:
                self.log.exception(
                    f"Exception raised while trying to remove files from S3 object store: {params['remove']['images']}"
                )

        # Commit transaction
        rw.transport.transaction_commit(txn)
