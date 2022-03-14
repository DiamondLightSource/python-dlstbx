from __future__ import annotations

import logging
import math
import pathlib
import time
from typing import Optional, Tuple, Union

import h5py
import hdf5plugin
import numpy as np

logger = logging.getLogger("dlstbx.h5rewrite")


class Visitor:
    def __init__(
        self,
        dest: h5py.File,
        compression: Optional[int] = None,
        compression_opts: Optional[tuple] = None,
    ):
        self.dest = dest
        self.compression = compression
        self.compression_opts = compression_opts

    def _create_dataset(self, dataset: h5py.Dataset, dest: h5py.File) -> h5py.Dataset:
        """Thin wrapper around Group.create_dataset.

        Applies compression options if requested.
        """
        dset = dest.create_dataset(
            dataset.name,
            data=dataset,
            compression=self.compression if dataset.shape else None,
            compression_opts=self.compression_opts if dataset.shape else None,
        )
        dset.attrs.update(dataset.attrs)
        return dset

    def __call__(self, name: str, node: Union[h5py.Dataset, h5py.Group]):
        if (
            node.attrs.get("NX_class") == b"NXdata"
            or node.parent.attrs.get("NX_class") == b"NXdata"
        ):
            return
        if isinstance(node, h5py.Dataset):
            # Faithfully copy the dataset to the destination
            logger.debug(f"Copying dataset: {name}")
            dset = self._create_dataset(node, self.dest)
            dset.attrs.update(node.attrs)
        else:
            # Create the group in the destination, and then loop over all children to
            # identify links, as these would normally be skipped by visititems
            logger.debug(f"Copying group: {name}")
            group = self.dest.require_group(name)
            group.attrs.update(node.attrs)

            for item in node.keys():
                try:
                    child = node[item]
                except KeyError:
                    logger.warning(
                        f"Error acessing '{item}' in '{name}'", exc_info=True
                    )
                    continue
                if isinstance(child, h5py.Group):
                    continue
                link = node.get(item, getlink=True)
                if isinstance(link, h5py.ExternalLink):
                    dest_path = pathlib.Path(self.dest.filename)
                    external = dest_path.parent.absolute().joinpath(f"{link.filename}")
                    with h5py.File(external, "w", libver="latest") as data_file:
                        dset = self._create_dataset(child, data_file)
                    group[item] = h5py.ExternalLink(external, link.path)
                    logger.debug(
                        f"ExternalLink: {'/'.join((node.name, item))} -> {external}:{link.path}"
                    )
                elif isinstance(link, (h5py.SoftLink, h5py.HardLink)):
                    try:
                        ref_name = node[child.ref].name
                    except ValueError:
                        logger.warning("Unable to access link {link}")
                    if ref_name == child.name:
                        # This is the original copy and the visitor will visit this
                        # dataset above
                        continue
                    if isinstance(link, h5py.HardLink):
                        logger.debug(f"Creating HardLink: {child.name} -> {ref_name}")
                        if ref_name not in self.dest:
                            dset = self._create_dataset(node[child.ref], self.dest)
                        group[child.name] = self.dest[ref_name]
                    else:
                        logger.debug(f"Creating SoftLink: {child.name} -> {ref_name}")
                        if ref_name not in self.dest:
                            dset = self._create_dataset(node[child.ref], self.dest)
                        group[child.name] = h5py.SoftLink(ref_name)


def rewrite(
    master_h5: pathlib.Path,
    out_h5: pathlib.Path,
    zeros: bool = False,
    image_range: Optional[Tuple[int, int]] = None,
    delay: Optional[float] = None,
) -> None:
    """Re-write an HDF5 file as a VDS/SWMR file.

    Args:
        master_h5 (pathlib.Path): Path to input master h5 file
        out_h5 (pathlib.Path): Path to output h5 file
        zeros (bool): Output zeros in place of the original data (default=False)
        image_range (tuple): Zero-indexed image range selection
        delay (float): Time delay (in seconds) between writing each image
    """

    if image_range:
        assert len(image_range) == 2
        start, end = image_range
        assert start < end

    with h5py.File(master_h5, "r") as fs:
        data_files = []

        entry_data = fs["entry/data"]
        data = entry_data[entry_data.attrs.get("signal", "data")]
        axes = entry_data.attrs.get("axes")
        if image_range:
            n_images = end - start
        else:
            n_images = data.shape[0]
            start, end = 0, n_images

        vds_block_size = 100
        vds_nblocks = int(math.ceil(n_images / vds_block_size))

        bitshuffle_compression = hdf5plugin.Bitshuffle(
            nelems=0,  # block_size, let Bitshuffle choose its value
            lz4=True,
        )

        assert not out_h5.exists(), f"Refusing to overwrite existing file {out_h5}"
        with h5py.File(out_h5, "w", libver="latest") as fd:
            visit = Visitor(
                fd,
                **bitshuffle_compression,
            )
            fs.visititems(visit)

            vds = h5py.VirtualLayout(
                shape=(n_images,) + data.shape[1:], dtype=data.dtype
            )
            dest_path = fd.filename
            if dest_path.endswith("_master.h5"):
                dest_path = dest_path[:-10]
            dest_path = pathlib.Path(dest_path)
            for i in range(vds_nblocks):
                n_images_block = min(n_images - (i * vds_block_size), vds_block_size)
                filename = dest_path.parent.joinpath(f"{dest_path.stem}_{i:06d}.h5")
                vds[
                    i * vds_block_size : i * vds_block_size + n_images_block
                ] = h5py.VirtualSource(
                    filename, "data", shape=(n_images_block,) + data.shape[1:]
                )
            fd.create_virtual_dataset("/entry/data/data", vds, fillvalue=-1)
            fd[entry_data.name].attrs.update(entry_data.attrs)
            if axes:
                if axes in fd[entry_data.name]:
                    fd[entry_data.name][axes].resize((n_images,))
                    fd[entry_data.name][axes][...] = entry_data[axes][start:end]
                    assert (
                        fd["/entry/sample/transformations/omega"]
                        == fd["/entry/data/omega"]
                    )
                else:
                    fd[entry_data.name].create_dataset(
                        axes, data=entry_data[axes][start:end]
                    )

        for i in range(vds_nblocks):
            filename = dest_path.parent.joinpath(f"{dest_path.stem}_{i:06d}.h5")
            assert (
                not filename.exists()
            ), f"Refusing to overwrite existing file {filename}"
            data_file = h5py.File(filename, "w", libver="latest")
            data_file.create_dataset(
                "data",
                shape=(vds_block_size,) + data.shape[1:],
                chunks=(1,) + data.shape[1:],
                dtype=data.dtype,
                **bitshuffle_compression,
            )
            data_file.swmr_mode = True
            data_files.append(data_file)
        for i in range(start, end):
            if delay:
                time.sleep(delay)
            i_block, j = divmod(i - start, vds_block_size)
            if zeros:
                data_files[i_block]["data"][j] = np.zeros(
                    data.shape[1:], dtype=data.dtype
                )
            else:
                data_files[i_block]["data"][j] = data[i]
            data_files[i_block].flush()
            logger.debug(f"{data_files[i_block].filename} {j} {i}")

        return
