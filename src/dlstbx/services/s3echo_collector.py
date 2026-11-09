from __future__ import annotations

from typing import Annotated, Any

import minio
import pydantic
from workflows.recipe.typed import NonEmpty, typed_handler
from workflows.services.common_service import CommonService

from dlstbx.util import iris
from dlstbx.util.iris import get_minio_client, update_dcid_info_file


class CollectorParameters(pydantic.BaseModel):
    bucket: Annotated[str, NonEmpty]
    rpid: int
    dcid: int | None = None
    # Pairs of (DCID, image master file)
    related_images: list[tuple[int, Any]] | None = None


class StartParameters(CollectorParameters):
    images: Any = None

    @pydantic.model_validator(mode="after")
    def check_dcid(self) -> StartParameters:
        if self.images and self.dcid is None:
            raise ValueError("dcid is required when uploading images")
        return self


class EndParameters(CollectorParameters):
    @pydantic.model_validator(mode="after")
    def check_dcid(self) -> EndParameters:
        if self.related_images is None and self.dcid is None:
            raise ValueError("Either dcid or related_images is required")
        return self

    @property
    def datasets(self) -> list[tuple[int, Any]]:
        """The related images, or else the single data collection"""
        if self.related_images is not None:
            return self.related_images
        assert self.dcid is not None  # ensured by check_dcid
        return [(self.dcid, None)]


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

        self.subscribe(
            "s3echo.start",
            self.on_start,
        )

        self.subscribe(
            "s3echo.end",
            self.on_end,
        )

    @typed_handler
    def on_start(self, rw, header, message, parameters: StartParameters):
        """
        Process request for uploading images to S3 Echo object store.
        """
        # Conditionally acknowledge receipt of the message
        txn = rw.transport.transaction_begin(subscription_id=header["subscription"])
        rw.transport.ack(header, transaction=txn)

        minio_client = get_minio_client(S3EchoCollector._s3echo_credentials)

        bucket_name = parameters.bucket
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
        rpid = parameters.rpid

        s3echo_upload_files = {}
        if images := parameters.images:
            dcid = parameters.dcid
            response_info = update_dcid_info_file(
                minio_client, bucket_name, dcid, 0, rpid, self.log
            )
            image_files = iris.get_image_files(images, self.log)
            s3echo_upload_files.update(
                {name: (dcid, pth) for name, pth in image_files.items()}
            )
            if not response_info:
                self.log.debug("Sending message to upload endpoint")
                rw.send_to(
                    "upload", {"s3echo_upload": {dcid: image_files}}, transaction=txn
                )
            rw.environment.update({"s3echo_upload": s3echo_upload_files})
            self.log.debug("Sending message to watch endpoint")
            rw.send_to("watch", message, transaction=txn)
        elif parameters.related_images:
            for dcid, image_master_file in parameters.related_images:
                response_info = update_dcid_info_file(
                    minio_client, bucket_name, dcid, 0, rpid, self.log
                )
                image_files = iris.get_related_images_files_from_h5(
                    image_master_file, self.log
                )
                s3echo_upload_files.update(
                    {name: (dcid, pth) for name, pth in image_files.items()}
                )
                if not response_info:
                    self.log.debug("Sending message to upload endpoint")
                    rw.send_to(
                        "upload",
                        {"s3echo_upload": {dcid: image_files}},
                        transaction=txn,
                    )
            rw.environment.update({"s3echo_upload": s3echo_upload_files})
            self.log.debug("Sending message to watch endpoint")
            rw.send_to("watch", message, transaction=txn)
        rw.transport.transaction_commit(txn)

    @typed_handler
    def on_end(self, rw, header, message, parameters: EndParameters):
        """
        Remove reference to image data in S3 Echo object store after end of processing.
        """
        # Conditionally acknowledge receipt of the message
        txn = rw.transport.transaction_begin(subscription_id=header["subscription"])
        rw.transport.ack(header, transaction=txn)

        minio_client = get_minio_client(S3EchoCollector._s3echo_credentials)
        bucket_name = parameters.bucket
        rpid = parameters.rpid

        for dcid, _ in parameters.datasets:
            response_info = update_dcid_info_file(
                minio_client, bucket_name, dcid, None, None, self.log
            )
            if not response_info:
                self.log.warning(f"No {dcid}_info data read from the object store")
            elif response_info["status"] == -1 or (
                response_info["status"] == 1 and response_info["pid"] == [rpid]
            ):
                dc_objects = {
                    obj.object_name
                    for obj in minio_client.list_objects(bucket_name)
                    if obj.object_name is not None
                }
                for obj_name in dc_objects:
                    if obj_name.startswith(f"{dcid}_"):
                        minio_client.remove_object(bucket_name, obj_name)
            else:
                update_dcid_info_file(
                    minio_client, bucket_name, dcid, None, -rpid, self.log
                )

        rw.transport.transaction_commit(txn)
