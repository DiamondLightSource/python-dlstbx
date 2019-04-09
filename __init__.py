from __future__ import absolute_import, division, print_function

import sys


def enable_graylog(live=True):
    """
    Set up graylog logging using the zocalo.
    In live mode direct logs to the Data Analysis stream in graylog.
    Otherwise send logs to the general bucket.
    """
    import zocalo

    if live:
        return zocalo.enable_graylog(host="graylog2.diamond.ac.uk", port=12208)
    else:
        return zocalo.enable_graylog()


class Buck:
    """A buck, which can be passed."""

    def __init__(self, name="Buck"):
        self._name = name

    def _debuck(self, frame):
        references = [var for var in frame if frame[var] == self]
        for ref in references:
            del frame[ref]

    def Pass(self):
        try:
            raise Exception()
        except Exception:
            self._debuck(sys.exc_info()[2].tb_frame.f_back.f_locals)
            print("...aand it's gone.")

    def __repr__(self):
        try:
            raise Exception()
        except Exception:
            self._debuck(sys.exc_info()[2].tb_frame.f_back.f_locals)
            return "<%s instance at %s...aand it's gone>" % (
                self._name,
                hex(id(self))[:-1],
            )
