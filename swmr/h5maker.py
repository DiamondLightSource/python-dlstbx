import h5py
import numpy
import random
import time
import os
import sys


def main(prefix, SHAPE=(512, 512), BLOCK=100, NUMBER=10):
    def image():
        return (numpy.random.rand(*SHAPE) * 100).astype(numpy.int16)

    # make many virtual sources... will fill these in later with data

    vds = h5py.VirtualLayout(shape=(BLOCK * NUMBER,) + SHAPE, dtype="i4")
    for j in range(NUMBER):
        filename = os.path.split(f"{prefix}_{j:06d}.h5")[-1]
        vds[j * BLOCK : (j + 1) * BLOCK] = h5py.VirtualSource(
            filename, "data", shape=(BLOCK,) + SHAPE
        )

    # make the top level file with the VDS

    with h5py.File(f"{prefix}_master.h5", "w", libver="latest") as f:
        f.create_virtual_dataset("/entry/data/data", vds, fillvalue=-1)

    time.sleep(5.0)

    # now open NUMBER h5 files in SWMR mode
    data_files = []

    for j in range(NUMBER):
        filename = f"{prefix}_{j:06d}.h5"
        data_file = h5py.File(filename, "w", libver="latest")
        data_file.create_dataset(
            "data",
            shape=(BLOCK,) + SHAPE,
            chunks=(1,) + SHAPE,
            compression="gzip",
            dtype="i4",
        )
        data_file.swmr_mode = True
        data_files.append(data_file)

    # randomly assign data to them ~ 10 times / s
    to_do = [(j, k) for j in range(NUMBER) for k in range(BLOCK)]
    random.shuffle(to_do)

    for b, f in to_do:
        data_files[b]["data"][f] = image()
        data_files[b].flush()
        print(f"data_{b:06d}.h5 {f} {b*BLOCK+f} {time.time()}")


if __name__ == "__main__":
    if len(sys.argv) == 1:
        sys.exit(f"Usage: {sys.argv[0]} /path/to/generated/hdf/prefix")

    prefix = sys.argv[1]

    main(prefix)
