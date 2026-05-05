from __future__ import annotations

import contextlib
import time
from typing import NamedTuple, Optional


class ProfileResult(NamedTuple):
    elapsed_ms: float
    mem_delta_mb: Optional[float]  # GPU memory allocated delta; None on CPU
    mem_reserved_mb: Optional[float]  # total GPU memory reserved; None on CPU


class CudaProfiler:
    """
    Context manager for timing CUDA (or CPU) operations accurately.

    CUDA kernels are asynchronous — time.time() measures CPU submission time,
    not GPU completion time. This class uses torch.cuda.Event timestamps on
    the GPU timeline, then synchronizes before reading elapsed time.

    Falls back to time.perf_counter() transparently when CUDA is unavailable.

    Example::

        profiler = CudaProfiler(device="cuda:0", track_memory=True)
        with profiler.record():
            output = model(inputs)
        print(profiler.results[-1].elapsed_ms)   # per-call
        print(profiler.median_ms)                # over all recorded calls
    """

    def __init__(self, device: str = "cuda:0", track_memory: bool = True):
        self._device = device
        self._track_memory = track_memory
        self._use_cuda = device.startswith("cuda")
        self._dev_idx: int = 0
        if self._use_cuda and ":" in device:
            self._dev_idx = int(device.split(":")[-1])
        self.results: list[ProfileResult] = []

    @contextlib.contextmanager
    def record(self):
        """Wrap a block and record its execution time (and optional GPU memory)."""
        try:
            import torch

            cuda_ok = self._use_cuda and torch.cuda.is_available()
        except ImportError:
            cuda_ok = False

        if cuda_ok:
            import torch

            mem_before = (
                torch.cuda.memory_allocated(self._dev_idx)
                if self._track_memory
                else None
            )
            start = torch.cuda.Event(enable_timing=True)
            end = torch.cuda.Event(enable_timing=True)
            start.record()
            try:
                yield
            finally:
                end.record()
                torch.cuda.synchronize(self._dev_idx)
                elapsed_ms = start.elapsed_time(end)
                if self._track_memory:
                    mem_after = torch.cuda.memory_allocated(self._dev_idx)
                    mem_reserved = torch.cuda.memory_reserved(self._dev_idx)
                    mem_delta_mb = (mem_after - mem_before) / 1e6
                    mem_reserved_mb = mem_reserved / 1e6
                else:
                    mem_delta_mb = mem_reserved_mb = None
                self.results.append(
                    ProfileResult(elapsed_ms, mem_delta_mb, mem_reserved_mb)
                )
        else:
            t = time.perf_counter()
            try:
                yield
            finally:
                elapsed_ms = (time.perf_counter() - t) * 1e3
                self.results.append(ProfileResult(elapsed_ms, None, None))

    @property
    def mean_ms(self) -> Optional[float]:
        if not self.results:
            return None
        return sum(r.elapsed_ms for r in self.results) / len(self.results)

    @property
    def median_ms(self) -> Optional[float]:
        if not self.results:
            return None
        vals = sorted(r.elapsed_ms for r in self.results)
        mid = len(vals) // 2
        return vals[mid] if len(vals) % 2 else (vals[mid - 1] + vals[mid]) / 2

    @property
    def max_ms(self) -> Optional[float]:
        if not self.results:
            return None
        return max(r.elapsed_ms for r in self.results)
