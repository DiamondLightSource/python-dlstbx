import h5py

from dials.array_family import flex
from dxtbx.format.FormatNexusEiger import FormatNexusEiger
from dxtbx.format.nexus import dataset_as_flex

import dlstbx.nexus


class FormatNXmx(FormatNexusEiger):

    _cached_file_handle = None

    @staticmethod
    def understand(image_file):
        return True

    def __init__(self, image_file, **kwargs):
        """Initialise the image structure from the given file."""
        super().__init__(image_file, **kwargs)

    def _start(self):
        self._static_mask = None

        with h5py.File(self._image_file, "r") as fh:
            nxmx = dlstbx.nexus.nxmx.NXmx(fh)
            nxsample = nxmx.entries[0].samples[0]
            nxinstrument = nxmx.entries[0].instruments[0]
            nxdetector = nxinstrument.detectors[0]
            nxbeam = nxinstrument.beams[0]

            self._goniometer_model = dlstbx.nexus.get_dxtbx_goniometer(nxsample)
            self._beam_model = dlstbx.nexus.get_dxtbx_beam(nxbeam)
            self._detector_model = dlstbx.nexus.get_dxtbx_detector(nxdetector, nxbeam)
            self._scan_model = dlstbx.nexus.get_dxtbx_scan(nxsample, nxdetector)
            self._static_mask = dlstbx.nexus.get_static_mask(nxdetector)

    def _beam(self, index=None):
        return self._beam_model

    def get_static_mask(self, index=None, goniometer=None):
        return self._static_mask

    def get_raw_data(self, index):
        if self._cached_file_handle is None:
            self._cached_file_handle = h5py.File(self._image_file, "r")

        nxmx = dlstbx.nexus.nxmx.NXmx(self._cached_file_handle)
        nxdata = nxmx.entries[0].data[0]
        data = nxdata[nxdata.signal]
        _, height, width = data.shape
        data_as_flex = dataset_as_flex(
            data, (slice(index, index + 1, 1), slice(0, height, 1), slice(0, width, 1))
        )
        data_as_flex.reshape(flex.grid(data_as_flex.all()[1:]))
        return data_as_flex
