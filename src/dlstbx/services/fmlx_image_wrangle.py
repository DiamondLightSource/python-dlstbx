from glob import glob
import logging
import os
import shutil
from typing import Any, Callable, Dict, NamedTuple
import pkg_resources
from PIL import Image

import workflows.recipe
from workflows.services.common_service import CommonService

logger = logging.getLogger('dlstbx.services.fmlximagewrangler')

class PluginInterface(NamedTuple):
    rw: workflows.recipe.wrapper.RecipeWrapper
    parameters: Callable[[str], Any]
    message: Dict[str, Any]

PluginParameter = PluginInterface  # backwards-compatibility, 20210702

class ImageUploaderService(CommonService):
    """
    A service that takes images from a formulatrix imager and puts them into
    the correct format for CHiMP
    """
    
    # Human readable service name
    _service_name = "DLS Formulatrix Image Wrangler"

    # Logger name
    _logger_name = "dlstbx.services.fmlximagewrangler"

    def initializing(self):
        """Subscribe to a queue. Received messages must be acknowledged."""
        self.log.info("Formulatrix image wrangler service starting")
        self.image_functions: dict[str, Callable] = {
            e.name: e.load()
            for e in pkg_resources.iter_entry_points("zocalo.services.fmlximagewrangler.plugins")
        }
        workflows.recipe.wrap_subscribe(
            self._transport,
            "fmlximagewrangler",
            self.image_call,
            acknowledgement=True,
            log_extender=self.extend_log,
        )

    def move_dir(self, src, dst):
        """
        Moves files in src dir to dst dir depending on their age
        """
        # TODO: write this so an image is moved as soon as it appears on disk - filewatcher?
        # TODO: move instead of copy?
        # TODO: sensible logging

        filelist = glob.glob(f'{src}/*')
        for f in filelist:
            st = os.stat(f)
            if time.time() - st.st_mtime > 10 and st.st_size > 0:
                new_f = os.path.join(dst, os.path.basename(f))
                _,ext = os.path.splitext(f)
                if ext=='.tif':
                    self.flip_tif(f, new_f)
                else:
                    shutil.copyfile(f, new_f)
                os.unlink(f)

        try:
            os.rmdir(src)
        except OSError as e:
            pass

    def flip_tif(self, tif, dst):
        """
        Flip tif images and save to dst
        """
        img = Image.open(tif)
        img_flipped = img.transpose(Image.FLIP_TOP_BOTTOM)
        img_flipped.save(dst)

    def get_container_dirs(self, dirs):
        """
        Generate a dict of all containers with their most recent z-slice directories (dates)
        """
        containers = dict()
        dir_containers = [glob.glob(f'{dir}/*/') for dir in dirs]
        containers = self.get_dir_barcodes(dir_containers, dirs, containers)
        return containers

    def get_dir_barcodes(self, globlst, dirs, containers):
        """
        Add '<barcode>':'<dir>' to containers dict for each dir in dirs
        """
        barcodes = [os.path.basename(os.path.abspath(f)) for f in globlst]
        for barcode, dir in zip(barcodes, dirs):
            containers[barcode] = dir
        return containers

    def get_visit_dir():
        # TODO: write this to use paramaters from recipe(?) instead of config file:
        # Does the visit appear in ispyb before the images appear on disk, or is ispyb somehow
        # otherwise aware of the imaging?
        pass

    def make_dirs():
        # TODO: find right place for this
        # purpose: mkdir depending on user type and if dir exists or not
        pass

    def zslice_to_visit():
        """
        Move the z-slice images from the configured 'archive_dir' to their target_dir which is a folder
        named by the container barcode in the tmp folder in the container's visit dir.
        """
        # at this point we have to read ispyb - do we read ispyb first or what?
        # Current:
        # 1: dict of containers with z-slice directories (get_container_dirs)
        # 2: for every barcode retreive related ispyb entry (format?)
        # 3: extract visit directory path from ispyb info
        # 4: make a tmp dir in visit directory
        # 5: move all files to respective tmp visit dir
        # 6: remove redundant images from archive

        pass

    def ef_to_visit():
        """
        Move the ef images from the configured 'archive_dir' to their target_dir which is a folder
        named by the container barcode in the tmp folder in the container's visit dir.
        """
        # THIS IS THE DEFAULT
        # similar to zslice_to_visit but handles ef images described by an xml file
        # takes a list(?) of xml files from [where]
        # 1: check a jpg corresponding to xml exists and its been there for at least 10 seconds (finished copying?)
        # 2: read xml into tree
        # 3: get inspection id from xml
        # 4: read ispyb to get container by inspectionid
        # 5: make directory in visit folder 
        # 6: move files based on xml 
        # 7: update image location in ispyb
        # 8: flip the image
        # 9: create a thumbnail (where does that go/what is it used for?)

        pass

    def move_files():
        pass

    def get_container():
        # this reads ispyb
        pass

    def get_position():
        pass

    def get_samples():
        pass

