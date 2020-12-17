import argparse
import bitshuffle.h5
import h5py
import logging
import numpy as np
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
                    external = None
                else:
                    external = out_h5.parent.joinpath(f"{out_h5.stem}_{item}.h5")
                try:
                    dset_s = data_s[item]
                except KeyError as e:
                    if "unable to open external file" in str(e) and "'" in str(e):
                        logger.warning("Referenced file %s does not exist.", filename)
                        continue
                    raise
                else:
                    block_size = 0  # let Bitshuffle choose its value
                    compression_opts = (
                        block_size,
                        bitshuffle.h5.H5_COMPRESS_LZ4,
                    )
                    compression = bitshuffle.h5.H5FILTER
                    if zeros:
                        data = np.zeros(dset_s.shape)
                    else:
                        data = dset_s

                    if external:
                        with h5py.File(external, "w", libver="latest") as data_file:
                            data_file.create_dataset(
                                "data",
                                data=data,
                                compression=compression,
                                compression_opts=compression_opts,
                            )
                        data_d["item"] = h5py.ExternalLink(external, "data")
                    else:
                        data_d.create_dataset(
                            item,
                            data=data,
                            external=external,
                            compression=compression,
                            compression_opts=compression_opts,
                        )
                    data_d.attrs.update(data_s.attrs)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Rewrite a nexus file.")
    parser.add_argument("input_h5", type=pathlib.Path, help="the input h5 filepath")
    parser.add_argument("output_h5", type=pathlib.Path, help="the output h5 filepath")
    parser.add_argument(
        "--zeros", dest="zeros", action="store_true", help="replace data with zeros"
    )

    args = parser.parse_args()
    rewrite(args.input_h5, args.output_h5, zeros=args.zeros)
