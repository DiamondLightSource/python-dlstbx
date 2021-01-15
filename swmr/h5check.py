import h5py
import logging
import os
import time
import sys


logger = logging.getLogger(__name__)


def wait_for_frame(h5_data_file, dsetname, frame):
    size = 0
    while not os.path.exists(h5_data_file):
        continue
    with h5py.File(h5_data_file, "r", swmr=True) as h5_file:
        dataset = h5_file[dsetname]
        while size == 0:
            dataset.id.refresh()
            s = dataset.id.get_chunk_info_by_coord((frame, 0, 0))
            size = s.size
    return size


def get_real_frames(master, dataset):
    root = os.path.split(master.filename)[0]
    logger.debug(f"{root}, {master.filename}")
    plist = dataset.id.get_create_plist()

    assert plist.get_layout() == h5py.h5d.VIRTUAL

    virtual_count = plist.get_virtual_count()

    # figure out the real data file, real data set and real frame number
    # which corresponds to the input file, virtual data set and virtual frame

    file_dataset = []
    file_map = {}

    for j in range(virtual_count):
        filename = plist.get_virtual_filename(j)
        dsetname = plist.get_virtual_dsetname(j)

        if filename == ".":
            link = master.get(dsetname, getlink=True)
            filename = os.path.join(root, link.filename)
            dsetname = link.path
        else:
            filename = os.path.join(root, filename)

        file_dataset.append((filename, dsetname))

        vspace = plist.get_virtual_vspace(j)
        frames = vspace.get_regular_hyperslab()[3][0]
        offset = vspace.get_regular_hyperslab()[0][0]

        for k in range(frames):
            file_map[k + offset] = (j, k)

    return file_dataset, file_map


if __name__ == "__main__":
    if len(sys.argv) == 1:
        sys.exit(f"{sys.argv[0]} /path/to/prefix_master.h5 n1 n2 n3 ... nn")

    logging.basicConfig(level=logging.DEBUG, format="%(message)s")
    with h5py.File(sys.argv[1], "r", swmr=True) as f:
        d = f["/entry/data/data"]
        t0 = time.time()
        file_dataset, file_map = get_real_frames(f, d)
        t1 = time.time()
        logger.info(f"Setup took {t1-t0:.3f}s")
        shape = d.shape
        for j in map(int, sys.argv[2:]):
            m, k = file_map[j - 1]
            _f, _d = file_dataset[m]
            size = wait_for_frame(_f, _d, k)
            logger.info(f"{j}, {size}")
