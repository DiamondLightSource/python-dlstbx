from __future__ import annotations

import glob as glob_module
import os
import time
from typing import Optional

import dxtbx
import dxtbx.nexus
import h5py
import hdf5plugin  # noqa: F401 - registers HDF5 filters
import numpy as np
import nxmx
import pydantic
import workflows.recipe
from resonet.utils.predict_dxtbx import ImagePredictDxtbx
from tqdm import tqdm
from workflows.services.common_service import CommonService

from dlstbx.util import ChainMapWithReplacement


class ResonetResolutionParameters(pydantic.BaseModel):
    glob_pattern: str
    max_proc: Optional[int] = None


class DLSResonetResolution(CommonService):
    """Runs resonet resolution (or multilattice) estimation on a batch of images."""

    _service_name = "DLS ResoNet Resolution"
    _logger_name = "dlstbx.services.resonet_resolution"

    def initializing(self):
        kind = self.config.storage.get("kind", "reso")
        gpu = self.config.storage.get("gpu", False)
        dev = "cuda:0" if gpu else "cpu"
        model_key = f"{kind}_model"
        arch_key = f"{kind}_arch"
        model_path = self.config.storage.get(model_key)
        arch = self.config.storage.get(arch_key, "res50")

        if not model_path:
            self.log.critical(
                "Required environment variable %r not set; cannot start service",
                model_key,
            )
            self._request_termination()
            return

        self.log.info(
            "Initialising %s predictor from %s (arch=%s, dev=%s)",
            kind,
            model_path,
            arch,
            dev,
        )
        self._predictor = ImagePredictDxtbx(
            **{"dev": dev, model_key: model_path, arch_key: arch}
        )
        self._predictor.quads = [-2]
        self._predictor.cache_raw_image = False
        self._kind = kind

        workflows.recipe.wrap_subscribe(
            self._transport,
            "resonet.resolution",
            self.process,
            acknowledgement=True,
            log_extender=self.extend_log,
        )

    def load_image_from_file(self, image_file):
        """Generator yielding (raw_image, frame_index) for each frame in an HDF5 file."""
        with h5py.File(image_file, swmr=True) as f:
            nxmx_obj = nxmx.NXmx(f)
            nxsample = nxmx_obj.entries[0].samples[0]
            nxinstrument = nxmx_obj.entries[0].instruments[0]
            nxdetector = nxinstrument.detectors[0]
            nxdata = nxmx_obj.entries[0].data[0]
            dependency_chain = nxmx.get_dependency_chain(nxsample.depends_on)
            scan_axis = None
            for t in dependency_chain:
                if (
                    t.transformation_type == "rotation"
                    and len(t) > 1
                    and not np.all(t[()] == t[0])
                ):
                    scan_axis = t
                    break
            if scan_axis is None:
                for t in dependency_chain:
                    if len(t) > 1 and not np.all(t[()] == t[0]):
                        scan_axis = t
                        break
            if scan_axis is None:
                scan_axis = nxsample.depends_on
            num_images = len(scan_axis)
            for frame_idx, j in enumerate(tqdm(range(num_images), unit=" images")):
                (raw_image,) = dxtbx.nexus.get_raw_data(nxdata, nxdetector, j)
                yield raw_image, frame_idx

    def eat_images(self, glob_s, max_proc=None):
        """Process all images matching glob_s.

        Returns a tuple of (results, n_found) where results is a list of dicts
        containing per-frame resolution or multilattice probability estimates.
        Replaces the Pyro4-RPC / MPI-distributed execution from image_eater.py.
        """
        fnames = sorted(glob_module.glob(glob_s))
        n_found = len(fnames)
        self.log.info("Found %d images matching %s", n_found, glob_s)

        results = []
        t_reads = []
        t_infers = []
        seen = 0

        for i_f, f in enumerate(fnames):
            if max_proc is not None and i_f >= max_proc:
                break

            try:
                loader = dxtbx.load(f)
                det = loader.get_detector()
                beam = loader.get_beam()
            except Exception as e:
                self.log.warning("Could not open %s: %s", f, e)
                continue

            if len(det) > 1:
                self.log.warning("Skipping %s: multi-panel detectors not supported", f)
                continue

            self._predictor.xdim, self._predictor.ydim = det[0].get_image_size()
            self._predictor.pixsize_mm = det[0].get_pixel_size()[0]
            self._predictor.detdist_mm = abs(det[0].get_distance())
            self._predictor.wavelen_Angstrom = beam.get_wavelength()
            self._predictor._set_geom_tensor()

            frame_count = 0
            for raw_image, frame_idx in self.load_image_from_file(f):
                if max_proc is not None and frame_count >= max_proc:
                    break

                t = time.time()
                raw_image = raw_image.as_numpy_array()
                if raw_image.dtype != np.float32:
                    raw_image = raw_image.astype(np.float32)
                t_reads.append(time.time() - t)

                t = time.time()
                self._predictor._set_pixel_tensor(raw_image)
                if self._kind == "reso":
                    d = self._predictor.detect_resolution()
                    t_infers.append(time.time() - t)
                    results.append(
                        {
                            "file": f,
                            "frame": frame_idx,
                            "resolution": round(float(d), 4),
                        }
                    )
                    self.log.debug(
                        "%s [%d]  resolution=%.3f Å  (%d/%d)",
                        os.path.basename(f),
                        frame_idx,
                        d,
                        i_f + 1,
                        n_found,
                    )
                else:
                    pval = self._predictor.detect_multilattice_scattering(binary=False)
                    t_infers.append(time.time() - t)
                    results.append(
                        {
                            "file": f,
                            "frame": frame_idx,
                            "multilattice_probability": round(float(pval), 6),
                        }
                    )
                    self.log.debug(
                        "%s [%d]  multilattice_probability=%.4f  (%d/%d)",
                        os.path.basename(f),
                        frame_idx,
                        pval,
                        i_f + 1,
                        n_found,
                    )
                seen += 1
                frame_count += 1

        if t_reads:
            self.log.info(
                "Done. Processed %d shots. Median read=%.4f ms, inference=%.4f ms",
                seen,
                float(np.median(t_reads)) * 1e3,
                float(np.median(t_infers)) * 1e3,
            )
        return results, n_found

    def process(self, rw, header, message):
        parameters = ChainMapWithReplacement(
            message.get("parameters", {}) if isinstance(message, dict) else {},
            rw.recipe_step.get("parameters", {}),
            substitutions=rw.environment,
        )
        try:
            payload = ResonetResolutionParameters(**parameters)
        except pydantic.ValidationError as e:
            self.log.error("Invalid parameters for resonet.resolution: %s", e)
            rw.transport.nack(header)
            return

        try:
            results, n_found = self.eat_images(payload.glob_pattern, payload.max_proc)
        except Exception as e:
            self.log.error("Unexpected error in eat_images: %s", e, exc_info=True)
            txn = rw.transport.transaction_begin(subscription_id=header["subscription"])
            rw.transport.nack(header, transaction=txn)
            rw.transport.transaction_commit(txn)
            return

        result_message = {
            "glob_pattern": payload.glob_pattern,
            "n_found": n_found,
            "n_processed": len(results),
            "results": results,
        }
        for key in (x for x in message if x.startswith("file")):
            result_message[key] = message[key]

        txn = rw.transport.transaction_begin(subscription_id=header["subscription"])
        rw.transport.ack(header, transaction=txn)
        rw.send(result_message, transaction=txn)
        rw.transport.transaction_commit(txn)
