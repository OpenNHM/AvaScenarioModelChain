# tifDiff.py
"""
tiffDiff.py

Compares two raster files by counting the number of pixels > 1 (excluding NoData).
Prints the counts for each raster, and the absolute & percent difference.
"""

import rasterio
import numpy as np
import os
import sys


def compare_rasters(file1, file2):
    """Compare two rasters and report pixel counts >1."""
    files = [file1, file2]
    counts = []

    for f in files:
        with rasterio.open(f) as src:
            arr = src.read(1)
            nodata = src.nodata

        mask = (arr != nodata) & (arr > 1)
        count = np.sum(mask)
        print(f"{os.path.basename(f)}: pixel count >1 = {count}")
        counts.append(count)

    # Absolute and percent difference
    abs_diff = counts[1] - counts[0]
    perc_diff = 100 * abs_diff / counts[1] if counts[1] != 0 else float("nan")

    print("\n--- Comparison ---")
    print(f"Absolute difference (file2 - file1): {abs_diff}")
    print(f"Percent difference (relative to file2): {perc_diff:.2f}%")


if __name__ == "__main__":
    # If no args: run with your example files
    if len(sys.argv) == 1:
        file1 = "/media/christoph/Daten/Cairos/ModelChainProcess/data/Euregio/STirol/090_prepForFlowPy/BnCh2_subC500_100_5_sizeF500_praBond/pra030secE-2200-2400-5_bound.tif"
        file2 = "/media/christoph/Daten/Cairos/ModelChainProcess/data/Euregio/STirol/090_prepForFlowPy/BnCh2_subC500_100_5_sizeF500/pra030secE-2200-2400-5.tif"
    elif len(sys.argv) == 3:
        file1, file2 = sys.argv[1], sys.argv[2]
    else:
        print("Usage: python compare_rasters.py <file1.tif> <file2.tif>")
        sys.exit(1)

    compare_rasters(file1, file2)
