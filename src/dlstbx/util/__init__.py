import datetime
import errno
import os
import re
import stat
import string
from collections import ChainMap


class ChainMapWithReplacement(ChainMap):
    def __init__(self, *maps, substitutions=None) -> None:
        super().__init__(*maps)
        self._substitutions = substitutions

    def __getitem__(self, k):
        v = super().__getitem__(k)
        if self._substitutions and "$" in v:
            template = string.Template(v)
            return template.substitute(**self._substitutions)
        return v


def _create_tmp_folder(tmp_folder):
    try:
        os.makedirs(tmp_folder)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise
    try:
        os.chmod(
            tmp_folder,
            stat.S_IRUSR
            + stat.S_IWUSR
            + stat.S_IXUSR
            + stat.S_IRGRP
            + stat.S_IWGRP
            + stat.S_IXGRP
            + stat.S_IROTH
            + stat.S_IWOTH
            + stat.S_IXOTH,
        )
    except OSError as exception:
        if exception.errno != errno.EPERM:
            raise


def dls_tmp_folder():
    tmp_folder = "/dls/tmp/dlstbx"
    _create_tmp_folder(tmp_folder)
    return tmp_folder


def dls_tmp_folder_date():
    tmp_folder = os.path.join(
        dls_tmp_folder(), datetime.date.today().strftime("%Y-%m-%d")
    )
    _create_tmp_folder(tmp_folder)
    return tmp_folder


_proc_getnumber = re.compile(r":\s+([0-9]+)\s")


def get_process_uss(pid=None):
    """Get the unique set size of a process in bytes.
    The unique set size is the amount of memory that would be freed if that
    process was terminated.
    Note that this will only work on linux and takes about 10ms.
    """
    if not pid:
        pid = os.getpid()  # Don't cache this. Multiprocessing would copy value.
    with open("/proc/%s/smaps" % str(pid)) as fh:
        return 1024 * sum(
            int(_proc_getnumber.search(x).group(1))
            for x in fh
            if x.startswith("Private")
        )


try:
    if not os.path.isdir("/proc"):
        get_process_uss = lambda pid=None: None  # noqa: F811
except OSError as exception:
    if exception.errno == 2:
        # /proc not available on this platform
        get_process_uss = lambda pid=None: None
    else:
        raise
