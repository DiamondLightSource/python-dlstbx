import argparse
import bitshuffle.h5
import h5py
import logging
import math
import numpy as np
import pathlib
import time
from typing import Union


logger = logging.getLogger("dlstbx.h5rewrite")


class Visitor:
    def __init__(
        self,
        dest,
        compression=None,
        compression_opts=None,
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
                    external = dest_path.parent.joinpath(f"{dest_path.stem}_{item}.h5")
                    with h5py.File(external, "w", libver="latest") as data_file:
                        dset = self._create_dataset(child, data_file)
                    group[item] = h5py.ExternalLink(external, link.path)
                    logger.debug(
                        f"ExternalLink: {'/'.join((node.name, item))} -> {external}:{link.path}"
                    )
                elif isinstance(link, (h5py.SoftLink, h5py.HardLink)):
                    ref_name = node[child.ref].name
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


def rewrite(master_h5, out_h5, zeros=False, image_range=None, delay=None):
    if image_range:
        assert len(image_range) == 2
        start, end = image_range
        assert start < end

    with h5py.File(master_h5, "r") as fs:
        data_files = []

        entry_data = fs["entry/data"]
        data = entry_data[entry_data.attrs["signal"]]
        axes = entry_data.attrs.get("axes")
        if image_range:
            n_images = end - start
        else:
            n_images = data.shape[0]
            start, end = 0, n_images

        vds_block_size = 100
        vds_nblocks = int(math.ceil(n_images / vds_block_size))
        compression = bitshuffle.h5.H5FILTER
        compression_opts = (
            0,  # block_size, let Bitshuffle choose its value
            bitshuffle.h5.H5_COMPRESS_LZ4,
        )

        with h5py.File(out_h5, "w", libver="latest") as fd:
            visit = Visitor(
                fd,
                compression=compression,
                compression_opts=compression_opts,
            )
            fs.visititems(visit)

            vds = h5py.VirtualLayout(
                shape=(vds_block_size * vds_nblocks,) + data.shape[1:], dtype=data.dtype
            )
            dest_path = pathlib.Path(fd.filename)
            for i in range(vds_nblocks):
                filename = dest_path.parent.joinpath(f"{dest_path.stem}_{i:06d}.h5")
                vds[i * vds_block_size : (i + 1) * vds_block_size] = h5py.VirtualSource(
                    filename, "data", shape=(vds_block_size,) + data.shape[1:]
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
            data_file = h5py.File(filename, "w", libver="latest")
            data_file.create_dataset(
                "data",
                shape=(vds_block_size,) + data.shape[1:],
                chunks=(1,) + data.shape[1:],
                compression=compression,
                compression_opts=compression_opts,
                dtype=data.dtype,
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Rewrite a nexus file.")
    parser.add_argument("input_h5", type=pathlib.Path, help="the input h5 filepath")
    parser.add_argument("output_h5", type=pathlib.Path, help="the output h5 filepath")
    parser.add_argument(
        "--zeros", dest="zeros", action="store_true", help="replace data with zeros"
    )
    parser.add_argument(
        "--range", type=int, nargs=2, help="zero-indexed image range selection"
    )
    parser.add_argument(
        "--delay", type=float, help="time delay between writing each image"
    )
    parser.add_argument("-v", "--verbose", dest="verbose", action="store_true")

    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO, format="%(message)s"
    )
    rewrite(
        args.input_h5,
        args.output_h5,
        zeros=args.zeros,
        image_range=args.range,
        delay=args.delay,
    )
