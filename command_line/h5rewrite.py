import argparse
import logging
import pathlib
import sys

from dlstbx.swmr.h5rewrite import rewrite


def run():
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
        "--delay", type=float, help="time delay (in seconds) between writing each image"
    )
    parser.add_argument("-v", "--verbose", dest="verbose", action="store_true")

    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO, format="%(message)s"
    )
    if args.output_h5.exists():
        sys.exit(f"Refusing to overwrite existing file {args.output_h5}")
    rewrite(
        args.input_h5,
        args.output_h5,
        zeros=args.zeros,
        image_range=args.range,
        delay=args.delay,
    )


if __name__ == "__main__":
    run()
