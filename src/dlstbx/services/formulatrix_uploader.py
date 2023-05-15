from __future__ import annotations

import pathlib
import re
import time
import xml.etree.ElementTree as ET

import pydantic
import workflows.recipe
from PIL import Image
from workflows.services.common_service import CommonService

from dlstbx.util import ChainMapWithReplacement


class Container(pydantic.BaseModel):
    containerId: pydantic.NonNegativeInt | None = None
    containerType: str | None = None
    visit: str


class FormulatrixUploaderPayload(pydantic.BaseModel):
    xml: pathlib.Path
    inspection_id: pydantic.NonNegativeInt | None = None
    container: Container | None = None
    sample_id: pydantic.NonNegativeInt | None = None
    blsampleimageid: pydantic.NonNegativeInt | None = None
    thumb_height: pydantic.PositiveInt
    thumb_width: pydantic.PositiveInt

    @pydantic.validator("container", pre=True)
    def validate_container(cls, v):
        if isinstance(v, str):
            import ast

            # potentially convert from string representation of a dictionary
            v = ast.literal_eval(v)
        print(f"{v=}")
        return v


class DLSFormulatrixUploader(CommonService):
    """
    Business logic component. Given a data collection ID and some description
    of event circumstances (beamline, visit, experiment description, start or end of
    scan) this service decides what recipes should be run with what settings.
    """

    # Human readable service name
    _service_name = "DLS Formulatrix Uploader"

    # Logger name
    _logger_name = "dlstbx.services.formulatrix_uploader"

    _types = {
        "CrystalQuickX": {"well_per_row": 12, "drops_per_well": 2},
        "MitegenInSitu": {"well_per_row": 12, "drops_per_well": 2},
        "MitegenInSitu_3_Drop": {"well_per_row": 12, "drops_per_well": 3},
        "FilmBatch": {"well_per_row": 12, "drops_per_well": 1},
        "ReferencePlate": {"well_per_row": 2, "drops_per_well": 1},
    }

    def initializing(self):
        """Subscribe to the formulatrix.uploader queue. Received messages must be acknowledged."""
        self.log.info("Formulatrix uploader starting")

        workflows.recipe.wrap_subscribe(
            self._transport,
            "formulatrix.upload",
            self.process,
            acknowledgement=True,
            log_extender=self.extend_log,
        )

        workflows.recipe.wrap_subscribe(
            self._transport,
            "formulatrix.aggregate",
            self.aggregate,
            acknowledgement=True,
            exclusive=True,
            log_extender=self.extend_log,
            prefetch_count=65535,
        )
        self._register_idle(60, self.submit)

    def process(self, rw: workflows.recipe.RecipeWrapper, header: dict, message: dict):
        """Process an incoming event."""

        parameters = ChainMapWithReplacement(
            message.get("parameters", {}) if isinstance(message, dict) else {},
            rw.recipe_step.get("parameters", {}),
            substitutions=rw.environment,
        )
        self.log.debug(f"{parameters=}")
        self.log.debug(f"{rw.environment=}")

        success = False
        try:
            if parameters["task"].lower() == "ef":
                success = self.process_ef_images(rw, parameters)
            elif parameters["task"].lower() == "z":
                success = self.process_zslice_images(rw, parameters)
        except pydantic.ValidationError as e:
            self.log.error(e, exc_info=True)
            success = False

        if success:
            rw.transport.ack(header)
        else:
            rw.transport.nack(header)
        return

    @pydantic.validate_arguments(config=dict(arbitrary_types_allowed=True))
    def process_ef_images(
        self,
        rw: workflows.recipe.RecipeWrapper,
        payload: FormulatrixUploaderPayload,
    ):
        self.log.debug(f"{payload=}")
        xml = payload.xml
        image = xml.with_suffix(".jpg")
        if not image.exists():
            self.log.warning(f"Corresponding image {image} not found for {xml}")
            return False

        st = xml.stat()
        if time.time() - st.st_mtime > 10 and st.st_size > 0:
            tree = ET.parse(xml)
            root = tree.getroot()

            # deal with xml namespace
            ns = root.tag.split("}")[0].strip("{")
            nss = {"oppf": ns}

            inspection_id = re.sub(r"\-.*", "", root.find("oppf:ImagingId", nss).text)  # type: ignore
            self.log.info(f"inspection: {inspection_id}")

            if payload.container is None:
                rw.send_to(
                    "ispyb",
                    {
                        "inspection_id": inspection_id,
                    },
                )
                return True

            drop = root.find("oppf:Drop", nss).text  # type: ignore
            position = self._get_position(drop, payload.container.containerType)
            self.log.info(f"Drop: {drop} position: {position}")

            if position is None:
                self.log.error(
                    f"Could not match drop: {drop} to position for containerType {payload.container.containerType}",
                )
                return True

            if payload.sample_id is None:
                rw.send_to(
                    "ispyb",
                    {
                        "ispyb_command_list": [
                            {
                                "ispyb_command": "retrieve_sample_for_container_id_and_location",
                                "position": position,
                                "container_id": payload.container.containerId,
                                "store_result": "ispyb_sample_id",
                            }
                        ]
                    },
                )
                return True

            #     if sampleid is None:
            #         self._move_files(image, xml, "nosample")
            #         continue

            # Check if the visit dir exists yet
            visit = payload.container.visit
            proposal, _ = visit.split("-")
            visit_dir = pathlib.Path(f"/dls/mx/data/{proposal}/{visit}")
            visit_dir = pathlib.Path(
                f"/dls/tmp/rjgildea/mx/data/{proposal}/{visit}"
            )  # XXX
            visit_dir.mkdir(parents=True, exist_ok=True)  # XXX
            if not visit_dir.exists():
                self.log.error("Visit directory {visit_dir} does not exist")
                return False

            # Keep images in visit/imaging/containerid/inspectionid
            new_path = pathlib.Path(
                f"{visit_dir}/imaging/{payload.container.containerId}/{inspection_id}"
            )
            new_path.mkdir(parents=True, exist_ok=True)

            if payload.blsampleimageid is None:
                mppx = float(
                    root.find("oppf:SizeInMicrons", nss).find("oppf:Width", nss).text  # type: ignore
                ) / float(
                    root.find("oppf:SizeInPixels", nss).find("oppf:Width", nss).text  # type: ignore
                )
                mppy = float(
                    root.find("oppf:SizeInMicrons", nss).find("oppf:Height", nss).text  # type: ignore
                ) / float(
                    root.find("oppf:SizeInPixels", nss).find("oppf:Height", nss).text  # type: ignore
                )
                rw.send_to(
                    "ispyb",
                    {
                        "ispyb_command_list": [
                            {
                                "ispyb_command": "upsert_sample_image",
                                "sample_id": payload.sample_id,
                                "microns_per_pixel_x": mppx,
                                "microns_per_pixel_y": mppy,
                                "inspection_id": inspection_id,
                                "store_result": "ispyb_blsampleimageid",
                            },
                            {
                                "ispyb_command": "upsert_sample_image",
                                "sample_id": payload.sample_id,
                                "microns_per_pixel_x": mppx,
                                "microns_per_pixel_y": mppy,
                                "inspection_id": inspection_id,
                                "image_full_path": f"{new_path}/$ispyb_blsampleimageid.jpg",
                            },
                        ],
                    },
                )
                return True

            # Use blsampleimageid as file name as we are sure this is unique
            new_file = new_path / f"{payload.blsampleimageid}.jpg"

            # move image
            self.log.info(f"flip and copy: {image} to {new_file}")
            try:
                im = Image.open(image)
                im_flipped = im.transpose(Image.FLIP_TOP_BOTTOM)
                im_flipped.save(new_file)
            except OSError:
                self.log.exception(f"Error copying image file {image} to {new_file}")
                return False

            # create a thumbnail
            try:
                im_flipped.thumbnail((payload.thumb_width, payload.thumb_height))
                thumbnail = new_file.with_stem(new_file.stem + "th")
            except OSError:
                self.log.exception(f"Error opening image file {new_file}")
                return False

            try:
                im_flipped.save(thumbnail)
            except OSError as e:
                self.log.error(f"Error saving image thumbnail {thumbnail}: {e}")

            # # cleanup
            # try:
            #     image.unlink()
            # except OSError as e:
            #     self.log.error(f"Error deleting image file {image}: {e}")
            # try:
            #     xml.unlink()
            # except OSError as e:
            #     self.log.error(f"Error deleting XML file {xml}: {e}")
            # return True

    def _get_position(self, text_position, platetype):
        well, drop = text_position.split(".")

        drop = int(drop)
        row = ord(well[0]) - 65
        col = int(well[1:]) - 1

        # Need to know what type of plate this is to know how many columns it's got
        # This should be in the database, currently in json format embedded in this collection:
        # http://ispyb.diamond.ac.uk/beta/client/js/modules/shipment/collections/platetypes.js
        if platetype not in self._types:
            self.log.error(f"Unknown plate type: {platetype}")
            return

        ty = self._types[platetype]

        # Position is a linear sequence left to right across the plate
        return (
            (ty["well_per_row"] * row * ty["drops_per_well"])
            + (col * ty["drops_per_well"])
            + (drop - 1)
            + 1
        )
