import os

import h5py

from dials.array_family import flex
from dxtbx.format.FormatNexusEigerDLS import FormatNexusEigerDLS

from dlstbx.format.FormatNXmx import FormatNXmx

# Hack to switch off the FormatNexusEigerDLS format class
FormatNexusEigerDLS.understand = lambda image_file: False


def get_bit_depth_from_meta(meta_file_name):
    with h5py.File(meta_file_name, "r") as f:
        return int(f["/_dectris/bit_depth_image"][()][0])


def find_meta_filename(master_like):
    meta_filename = None
    f = h5py.File(master_like, "r")

    def _local_visit(name):
        obj = f[name]
        if not hasattr(obj, "keys"):
            return None
        for k in obj.keys():
            kclass = obj.get(k, getlink=True, getclass=True)
            if kclass is h5py._hl.group.ExternalLink:
                kfile = obj.get(k, getlink=True).filename
                if kfile.split(".")[0].endswith("meta"):
                    return kfile

    master_dir = os.path.split(master_like)[0]
    meta_filename = f.visit(_local_visit)

    return os.path.join(master_dir, meta_filename)


class FormatNXmxDLS(FormatNXmx):

    _cached_file_handle = None

    @staticmethod
    def understand(image_file):
        with h5py.File(image_file, "r") as handle:
            name = FormatNXmxDLS.get_instrument_name(handle)
            if name is None:
                return False
            if name.lower() in (b"i03", b"i04", b"i24", b"vmxi"):
                return True
            if name.upper().startswith(b"DLS "):
                return True
        return False

    def __init__(self, image_file, **kwargs):
        """Initialise the image structure from the given file."""

        super().__init__(image_file, **kwargs)
        # Get the bit depth from the meta.h5 in order to distinguish masked and
        # saturated pixels. Ideally we would get this from
        # /entry/instrument/detector/bit_depth_readout.
        # See https://jira.diamond.ac.uk/browse/MXGDA-3674
        try:
            meta = find_meta_filename(image_file)
            self._bit_depth_image = get_bit_depth_from_meta(meta)
        except Exception:
            self._bit_depth_image = 16

    def _start(self):
        super()._start()
        # Due to a bug the dimensions (but not the values) of the pixel_mask array
        # are reversed. See https://jira.diamond.ac.uk/browse/MXGDA-3675.
        for m in self._static_mask:
            m.reshape(flex.grid(reversed(m.all())))

    def get_raw_data(self, index):
        data = super().get_raw_data(index)
        if self._bit_depth_image:
            # if 32 bit then it is a signed int, I think if 8, 16 then it is
            # unsigned with the highest two values assigned as masking values
            if self._bit_depth_image == 32:
                top = 2 ** 31
            else:
                top = 2 ** self._bit_depth_image
            d1d = data.as_1d()
            d1d.set_selected(d1d == top - 1, -1)
            d1d.set_selected(d1d == top - 2, -2)
        return data
