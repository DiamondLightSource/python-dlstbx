from __future__ import annotations

import logging

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.colors import LogNorm

logger = logging.getLogger(__name__)


def plot_detector_image(raw_image, output_path, title=None):
    """Save a false-colour log-scale plot of a raw detector image.

    Non-positive pixel values (masked/dead pixels) are excluded from the
    colour scale so they do not compress the dynamic range.
    """
    data = raw_image.astype(np.int16)
    data[data <= 0] = -1
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
