import argparse
import bitshuffle.h5
import h5py
import logging
import numpy as np
import os
import pathlib


logger = logging.getLogger(__name__)


def rewrite(master_h5, out_h5, zeros=False):
    with h5py.File(master_h5, "r") as fs:
        with h5py.File(out_h5, "w", libver="latest") as fd:
            entry_d = fd.create_group("entry")
            entry_d.attrs.update(fs["entry"].attrs)
            fs.copy("entry/definition", entry_d)
            fs.copy("entry/instrument", entry_d)
            fs.copy("entry/sample", entry_d)
            data_s = fs["entry/data"]
            data_d = entry_d.create_group("data")
            data_d.attrs.update(data_s.attrs)
            for item in data_s.keys():
                link = data_s.get(item, getlink=True)
                if not isinstance(link, h5py.ExternalLink):
                    filename = master_h5
                else:
                    filename = os.path.abspath(
                        os.path.join(os.path.dirname(master_h5), link.filename)
                    )
                try:
                    dset_s = data_s[item]
                except KeyError as e:
                    if "unable to open external file" in str(e) and "'" in str(e):
                        logger.warning("Referenced file %s does not exist.", filename)
                        continue
                    raise
                else:
                    block_size = 0  # let Bitshuffle choose its value
                    if zeros:
                        data = np.zeros(dset_s.shape)
                    else:
                        data = dset_s
                    data_d.create_dataset(
                        item,
                        data=data,
                        compression=bitshuffle.h5.H5FILTER,
                        compression_opts=(block_size, bitshuffle.h5.H5_COMPRESS_LZ4),
                    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Rewrite a nexus file.")
    parser.add_argument("input_h5", type=pathlib.Path, help="the input h5 filepath")
    parser.add_argument("output_h5", type=pathlib.Path, help="the output h5 filepath")
    parser.add_argument(
        "--zeros", dest="zeros", action="store_true", help="replace data with zeros"
    )

    args = parser.parse_args()
    rewrite(args.input_h5, args.output_h5, zeros=args.zeros)
