import argparse
import bitshuffle.h5
import h5py
import logging
import numpy as np
import pathlib
from typing import Union


logger = logging.getLogger("dlstbx.h5rewrite")


class Visitor:
    def __init__(
        self,
        dest,
        compression=None,
        compression_opts=None,
        zeros=False,
        image_range=None,
    ):
        self.dest = dest
        self.compression = compression
        self.compression_opts = compression_opts
        self.zeros = zeros
        assert len(image_range) == 2
        assert image_range[0] < image_range[1]
        self.image_range = image_range

    def _create_dataset(self, dataset: h5py.Dataset, dest: h5py.File) -> h5py.Dataset:
        """Thin wrapper around Group.create_dataset.

        Applies compression options, selects a range of images and replaces data with
        zeros if requested.
        """
        shape = dataset.shape
        if shape and (
            dataset.name.startswith("/entry/data") or dataset.name.startswith("/data")
        ):
            if self.image_range:
                start, end = self.image_range
                assert 0 <= start < shape[0]
                assert 0 < end <= shape[0]
            else:
                start, end = (0, shape[0])
            shape = (end - start, *shape[1:])
            if self.zeros and dataset.name.startswith("/data"):
                data = np.zeros(shape)
            else:
                data = dataset[start:end]
        else:
            data = dataset
        dset = dest.create_dataset(
            dataset.name,
            data=data,
            compression=self.compression if dataset.shape else None,
            compression_opts=self.compression_opts if dataset.shape else None,
        )
        dset.attrs.update(dataset.attrs)
        return dset

    def __call__(self, name: str, node: Union[h5py.Dataset, h5py.Group]):
        if isinstance(node, h5py.Dataset):
            # Faithfully copy the dataset to the destination
            logger.info(f"Dataset: {name} {node.shape}")
            dset = self._create_dataset(node, self.dest)
            dset.attrs.update(node.attrs)
        else:
            # Create the group in the destination, and then loop over all children to
            # identify links, as these would normally be skipped by visititems.
            logger.info(f"Group: {name}")
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
                    logger.info(f"{child.name} -> {external}")
                elif isinstance(link, (h5py.SoftLink, h5py.HardLink)):
                    ref_name = node[child.ref].name
                    if ref_name == child.name:
                        # This is the original copy and the visitor will visit this
                        # dataset above
                        continue
                    logger.info(f"{child.name} -> {ref_name}")
                    if ref_name not in self.dest:
                        dset = self._create_dataset(node[child.ref], self.dest)
                    group[child.name] = self.dest[ref_name]


def rewrite(master_h5, out_h5, zeros=False, image_range=None):
    if image_range:
        assert len(image_range) == 2
        start, end = image_range
        assert start < end

    with h5py.File(master_h5, "r") as fs, h5py.File(out_h5, "w", libver="latest") as fd:
        compression = bitshuffle.h5.H5FILTER
        compression_opts = (
            0,  # block_size, let Bitshuffle choose its value
            bitshuffle.h5.H5_COMPRESS_LZ4,
        )
        visit = Visitor(
            fd,
            compression=compression,
            compression_opts=compression_opts,
            zeros=zeros,
            image_range=image_range,
        )
        fs.visititems(visit)
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

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    rewrite(args.input_h5, args.output_h5, zeros=args.zeros, image_range=args.range)
