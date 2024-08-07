from __future__ import annotations

import argparse
import logging
import os
import pathlib
import random
import time

import h5py
import numpy

logger = logging.getLogger(__name__)


def main(
    prefix,
    shape=(512, 512),
    block_size=100,
    nblocks=10,
    delay=None,
    per_image_delay=None,
    shuffle=True,
):
    def image():
        return (numpy.random.rand(*shape) * 100).astype(numpy.int16)

    # make many virtual sources... will fill these in later with data

    vds = h5py.VirtualLayout(shape=(block_size * nblocks,) + shape, dtype="i4")
    for j in range(nblocks):
        filename = os.path.split(f"{prefix}_{j:06d}.h5")[-1]
        vds[j * block_size : (j + 1) * block_size] = h5py.VirtualSource(
            filename, "data", shape=(block_size,) + shape
        )[:, :, :]

    # make the top level file with the VDS

    with h5py.File(f"{prefix}_master.h5", "w", libver="latest") as f:
        f.create_virtual_dataset("/entry/data/data", vds, fillvalue=-1)

    if delay:
        time.sleep(delay)

    # now open nblocks h5 files in SWMR mode
    data_files = []

    for j in range(nblocks):
        filename = f"{prefix}_{j:06d}.h5"
        data_file = h5py.File(filename, "w", libver="latest")
        data_file.create_dataset(
            "data",
            shape=(block_size,) + shape,
            chunks=(1,) + shape,
            compression="gzip",
            dtype="i4",
        )
        data_file.swmr_mode = True
        data_files.append(data_file)

    # randomly assign data to them ~ 10 times / s
    to_do = [(j, k) for j in range(nblocks) for k in range(block_size)]
    if shuffle:
        random.shuffle(to_do)

    for b, f in to_do:
        if per_image_delay:
            time.sleep(per_image_delay)
        data_files[b]["data"][f] = image()
        data_files[b].flush()
        logger.info(f"data_{b:06d}.h5 {f} {b*block_size+f} {time.time()}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate a fake HDF5 SWMR file.")
    parser.add_argument(
        "prefix", type=pathlib.Path, help="/path/to/generated/hdf/prefix"
    )
    parser.add_argument(
        "--shape",
        type=int,
        nargs=2,
        help="shape of the output data",
        default=(512, 512),
    )
    parser.add_argument(
        "--block_size",
        type=int,
        help="VDS block size",
        default=100,
    )
    parser.add_argument(
        "--nblocks",
        type=int,
        help="Number of VDS blocks",
        default=10,
    )
    parser.add_argument(
        "--delay",
        type=float,
        help="time delay (in seconds) between writing the master file and the first image",
    )
    parser.add_argument(
        "--per_image_delay",
        type=float,
        help="time delay (in seconds) between writing each image",
    )
    parser.add_argument(
        "--shuffle",
        dest="shuffle",
        action="store_true",
        help="shuffle output order of images",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    main(
        args.prefix,
        shape=tuple(args.shape),
        block_size=args.block_size,
        nblocks=args.nblocks,
        delay=args.delay,
        per_image_delay=args.per_image_delay,
        shuffle=args.shuffle,
    )
