import h5py
import os
import threading
import time

from dlstbx.cli.h5rewrite import cli
from dlstbx.swmr import h5watcher


def test_h5rewrite(dials_data, tmpdir):
    master_h5 = dials_data("vmxi_thaumatin") / "image_15799_master.h5"
    out_h5 = tmpdir / "out_master.h5"
    args = (str(master_h5), str(out_h5), "--range", "0", "20", "--delay=0.1")
    x = threading.Thread(target=cli, kwargs=dict(args=args))
    x.start()
    time.sleep(1)
    assert out_h5.check(file=1)
    with h5py.File(out_h5, "r", swmr=True) as f:
        d = f["/entry/data/data"]
        h5watcher.vds_info(os.path.split(out_h5)[0], f, d)
    x.join()
