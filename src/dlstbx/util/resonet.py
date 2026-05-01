from __future__ import annotations

import logging

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.colors import LogNorm

from dlstbx.util.xray_centering import reshape_grid

logger = logging.getLogger(__name__)


def plot_detector_image(raw_image, output_path, title=None):
    """Save a false-colour log-scale plot of a raw detector image.

    Non-positive pixel values (masked/dead pixels) are excluded from the
    colour scale so they do not compress the dynamic range.
    """
    data = raw_image.astype(float)
    data[data <= 0] = np.nan
    vmin = np.nanmin(data[data > 0]) if np.any(data > 0) else 1.0

    fig, ax = plt.subplots(figsize=(8, 8))
    im = ax.imshow(
        data,
        norm=LogNorm(vmin=vmin),
        cmap="viridis",
        origin="lower",
        interpolation="none",
    )
    fig.colorbar(im, ax=ax, label="counts (log scale)")
    if title:
        ax.set_title(title)
    ax.set_xlabel("x (pixels)")
    ax.set_ylabel("y (pixels)")
    fig.tight_layout()
    fig.savefig(output_path, dpi=150)
    plt.close(fig)
    logger.debug("Saved detector image plot to %s", output_path)


def plot_resolution_grid(results, grid_info, kind, output_path):
    """Save a 2D heatmap of per-frame resolution (or multilattice) estimates.

    Frames are mapped onto the scan grid using grid_info geometry, including
    unsnaking of bidirectional scans and handling of horizontal/vertical orientation.
    Missing frames are shown as NaN (white gaps in the colormap).
    """
    value_key = "resolution" if kind == "reso" else "multilattice_probability"
    n_frames = grid_info.steps_x * grid_info.steps_y

    output_path = "/tmp" + output_path

    data = np.full(n_frames, np.nan)
    for item in results:
        idx = item["frame"]
        if 0 <= idx < n_frames:
            data[idx] = item[value_key]

    grid = reshape_grid(
        data,
        (grid_info.steps_x, grid_info.steps_y),
        grid_info.snaked,
        grid_info.orientation,
    ).T

    w = max(4, grid_info.steps_x / 4)
    h = max(4, grid_info.steps_y / 4)
    fig, ax = plt.subplots(figsize=(w, h))
    cmap = "viridis_r" if value_key == "resolution" else "viridis"
    im = ax.imshow(grid, cmap=cmap, interpolation="none", aspect="equal")
    label = (
        "Resolution (Å)" if value_key == "resolution" else "Multilattice probability"
    )
    fig.colorbar(im, ax=ax, label=label)
    ax.set_xlabel(f"x steps  (Δ={grid_info.dx_mm * 1000:.1f} µm)")
    ax.set_ylabel(f"y steps  (Δ={grid_info.dy_mm * 1000:.1f} µm)")
    ax.set_title(
        f"ResoNet {value_key.replace('_', ' ')} "
        f"({grid_info.steps_x}×{grid_info.steps_y})"
    )
    fig.tight_layout()
    fig.savefig(output_path, dpi=150)
    plt.close(fig)
    logger.info("Saved resolution grid plot to %s", output_path)
