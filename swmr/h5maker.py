import h5py
import numpy
import random
import time


def main():

    SHAPE = (512, 512)

    def image():
        return (numpy.random.rand(*SHAPE) * 100).astype(numpy.int16)

    BLOCK = 100
    NUMBER = 10

    # make many virtual sources of data... will fill these in later with
    # data

    vds = h5py.VirtualLayout(shape=(BLOCK * NUMBER,) + SHAPE, dtype="i4")
    for j in range(NUMBER):
        filename = f"data_{j:06d}.h5"
        vds[j * BLOCK : (j + 1) * BLOCK] = h5py.VirtualSource(
            filename, "data", shape=(BLOCK,) + SHAPE
        )

    # make the top level file with the VDS

    with h5py.File("master.h5", "w", libver="latest") as f:
        f.create_virtual_dataset("/entry/data/data", vds, fillvalue=-1)

    time.sleep(5.0)

    # now open NUMBER h5 files in SWMR mode
    data_files = []

    for j in range(NUMBER):
        filename = f"data_{j:06d}.h5"
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
        print(f"data_{b:06d}.h5 {f} {time.time()}")
        time.sleep(0.1)


if __name__ == "__main__":
    main()
