# ------------ Runner: AvaDirectory Results Statistics ------------------ #
#
# Purpose :
#     Standalone runner for computing descriptive statistics and quality
#     checks on one or multiple AvaDirectoryResults.parquet files. Each
#     dataset is registered with a human-readable region name. The runner
#     executes per-region statistics and a combined “ALL” summary, and
#     writes plots + a markdown report to the configured output directory.
#
# Run it: 
#     pixi run -e dev python runAvaDirResultsStats.py
#
# Inputs :
#     - 12_avaDirectory/<caseFolder>/avaDirectoryResults.parquet
#       (one or multiple locations / regions)
#
# Outputs :
#     - 90_stats/avaDirResultsStats_report.md
#     - 90_stats/hist_rel_praAreaSized_<region>.png
#     - 90_stats/hist_res_pem_<region>.png
#     - 90_stats/hist_rel_praAreaSized_ALL.png
#     - 90_stats/hist_res_pem_ALL.png
#
# Config :
#     - Region list + parquet paths defined inside this runner
#     - Output directory set here (can be overridden via CLI later)
#
# Consumes :
#     - Step 15 outputs (AvaDirectory Results)
#
# Provides :
#     - Statistical overview and quality diagnostics for:
#         • Model-chain validation
#         • Regional and cross-regional comparison
#         • Downstream reporting and interpretation
#
# Author :
#     Christoph Hesselbach
#
# Institution :
#     Austrian Research Centre for Forests (BFW)
#     Department of Natural Hazards | Snow and Avalanche Unit
#
# Date & Version :
#   2025-12 - 1.0
#
# ----------------------------------------------------------------------- #


import logging
from pathlib import Path

from com2AvaDirectory.avaDirResultsStats import runAvaDirResultsStats

log = logging.getLogger(__name__)


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s:%(name)s: %(message)s",
    )

    # -------------------------------------------------------------------------
    # CONFIG: Add as many regions as you want here
    # -------------------------------------------------------------------------
    inputs = [
        {
            "regionName": "NTirol",
            "resultsParquet": "/media/christoph/SSD 500 GB/Cairos/ModelChainResults/Euregio/NTirol/251023/alpha32_3_umax8_18_maxS5/12_avaDirectory/BnCh2_subC500_100_5_sizeF500/avaDirectoryResults.parquet",
        },
        # {
        #     "regionName": "AnotherRegion",
        #     "resultsParquet": "/path/to/another/avaDirectoryResults.parquet",
        # },
    ]

    outDir = Path("/media/christoph/SSD 500 GB/Cairos/ModelChainResults/Euregio/NTirol/251023/alpha32_3_umax8_18_maxS5/90_stats/")

    reportPath = runAvaDirResultsStats(
        inputs=inputs,
        outDir=outDir,
        reportName="avaDirResultsStats_report.md",
    )

    log.info("Done. Report: %s", reportPath)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
