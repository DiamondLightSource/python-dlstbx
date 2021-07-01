import contextlib
import time


class Profiler:
    """
    A helper class that can record summary statistics on time spent in
    code blocks. Example usage:

    profiler = _Profiler()
    with profiler.record():
        ...
    print(profiler.mean)
    print(profiler.max)
    """

    def __init__(self):
        self._timing_max = None
        self._timing_sum = None
        self._timing_count = None

    @contextlib.contextmanager
    def record(self):
        start = time.time()
        try:
            yield
        finally:
            runtime = time.time() - start
            if self._timing_count:
                self._timing_count += 1
                self._timing_sum += runtime
                if runtime > self._timing_max:
                    self._timing_max = runtime
            else:
                self._timing_count = 1
                self._timing_sum = runtime
                self._timing_max = runtime

    @property
    def max(self):
        return self._timing_max

    @property
    def mean(self):
        if self._timing_count:
            return self._timing_sum / self._timing_count
        else:
            return None
