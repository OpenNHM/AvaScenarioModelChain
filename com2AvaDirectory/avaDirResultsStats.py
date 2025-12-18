# ------------ Step XX: AvaDirectory Results Statistics ----------------- #
#
# Purpose :
#     Compute descriptive statistics and quality checks for one or multiple
#     AvaDirectoryResults datasets. The analysis covers per-region and
#     combined evaluations, including feature counts, rel/res ratios,
#     duplicate detection (exact + scenario-independent), scenario coverage,
#     and basic distribution statistics.
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
#     - Provided via runner (region name + parquet path per location)
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
#   2025-12 - 1.1
#
# ----------------------------------------------------------------------- #

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

try:
    import geopandas as gpd
    _HAS_GPD = True
except Exception:
    _HAS_GPD = False

import matplotlib.pyplot as plt

log = logging.getLogger(__name__)

plt.rcParams.update({
    "figure.dpi": 120,
    "savefig.dpi": 200,
})


# ------------------ Data model ------------------ #
@dataclass
class resultsInput:
    regionName: str
    resultsParquet: Path


# ------------------ Public API ------------------ #
def runAvaDirResultsStats(
    inputs: List[Dict],
    outDir: Path,
    reportName: str = "avaDirResultsStats_report.md",
) -> Path:
    """
    Main entry point.

    Parameters
    ----------
    inputs:
        list of dicts with keys:
          - regionName
          - resultsParquet
    outDir:
        output directory for plots + md report
    reportName:
        name of markdown report

    Returns
    -------
    Path to markdown report
    """
    outDir = Path(outDir)
    outDir.mkdir(parents=True, exist_ok=True)

    parsed = _parseInputs(inputs)
    if not parsed:
        raise ValueError("No valid inputs provided.")

    mdLines: List[str] = []
    mdLines.append("# AvaDirectoryResults Stats Report\n")
    mdLines.append(f"- Output folder: `{outDir}`\n")
    mdLines.append("\n---\n")
    mdLines.append("## Notes on duplicates\n")
    mdLines.append(
        "- **Exact duplicates**: identical attribute rows **including scenario parameters** "
        "(flow/ppm/pem/rSize/resultID if present). This catches accidental double-writes.\n"
        "- **Scenario-independent duplicates**: identical attribute rows **excluding scenario parameters**. "
        "This is what you want for *release geometry (REL)* which is typically identical across scenarios.\n"
        "- Plots use **scenario-independent dedup** for REL and **exact dedup** for RES (so pem stays meaningful).\n"
    )
    mdLines.append("\n---\n")

    allFrames: List[pd.DataFrame] = []

    for inp in parsed:
        log.info("Stats: loading %s (%s)", inp.regionName, inp.resultsParquet)
        df = _loadResults(inp.resultsParquet)

        df["__regionName"] = inp.regionName
        allFrames.append(df)

        regionKey = _safeKey(inp.regionName)

        stats = _computeStats(df, regionName=inp.regionName)
        _writeRegionSection(mdLines, stats, header=f"## Region: {inp.regionName}")

        _plotRelPraAreaSized(df, outDir, suffix=regionKey)
        _plotResPem(df, outDir, suffix=regionKey)

        mdLines.append("\n---\n")

    dfAll = pd.concat(allFrames, ignore_index=True, copy=False)
    statsAll = _computeStats(dfAll, regionName="ALL")
    _writeRegionSection(mdLines, statsAll, header="## Combined: ALL regions")

    _plotRelPraAreaSized(dfAll, outDir, suffix="ALL")
    _plotResPem(dfAll, outDir, suffix="ALL")

    reportPath = outDir / reportName
    reportPath.write_text("\n".join(mdLines), encoding="utf-8")
    log.info("Stats: Markdown report written to %s", reportPath)
    return reportPath


# ------------------ Core stats ------------------ #
def _computeStats(df: pd.DataFrame, regionName: str) -> Dict:
    stats: Dict = {"regionName": regionName}

    _requireCols(df, ["praID", "modType"])

    nTotal = int(len(df))
    stats["totalRows"] = nTotal

    stats["modCounts"] = df["modType"].astype("string").value_counts(dropna=False).to_dict()

    relRows = int((df["modType"] == "rel").sum())
    resRows = int((df["modType"] == "res").sum())
    stats["relRows"] = relRows
    stats["resRows"] = resRows
    stats["relPct"] = _pct(relRows, nTotal)
    stats["resPct"] = _pct(resRows, nTotal)

    stats["uniquePraIdAll"] = int(df["praID"].nunique(dropna=True))
    stats["uniquePraIdRel"] = int(df.loc[df["modType"] == "rel", "praID"].nunique(dropna=True))
    stats["uniquePraIdRes"] = int(df.loc[df["modType"] == "res", "praID"].nunique(dropna=True))

    # duplicate keys
    relKeyScenarioInd = _dupSubsetColsScenarioIndependent(df, modType="rel")
    resKeyScenarioInd = _dupSubsetColsScenarioIndependent(df, modType="res")
    relKeyExact = _dupSubsetColsExact(df, modType="rel")
    resKeyExact = _dupSubsetColsExact(df, modType="res")

    stats["dupColsUsed"] = {
        "relScenarioIndependent": relKeyScenarioInd,
        "resScenarioIndependent": resKeyScenarioInd,
        "relExact": relKeyExact,
        "resExact": resKeyExact,
    }

    stats["relDupScenarioIndependent"] = _computeDuplicates(df, "rel", relKeyScenarioInd)
    stats["resDupScenarioIndependent"] = _computeDuplicates(df, "res", resKeyScenarioInd)
    stats["relDupExact"] = _computeDuplicates(df, "rel", relKeyExact)
    stats["resDupExact"] = _computeDuplicates(df, "res", resKeyExact)

    # missingness (small, useful)
    keyCols = [c for c in ["praID", "resultID", "modType", "praAreaSized", "pem", "ppm", "flow", "sector"] if c in df.columns]
    stats["naInfo"] = {c: int(df[c].isna().sum()) for c in keyCols}

    # scenario coverage (per praID)
    stats["scenarioCoverage"] = _scenarioCoverageStats(df)

    # numeric summaries (raw + scenario-independent unique-REL + exact-unique-RES)
    stats["numSummaryRaw"] = _basicNumericSummary(df)

    relUniq = _dedupScenarioIndependent(df[df["modType"] == "rel"], modType="rel")
    resUniqExact = _dedupExact(df[df["modType"] == "res"], modType="res")
    stats["numSummaryRelScenarioIndependent"] = _basicNumericSummary(relUniq) if len(relUniq) else {}
    stats["numSummaryResExact"] = _basicNumericSummary(resUniqExact) if len(resUniqExact) else {}

    return stats


def _computeDuplicates(df: pd.DataFrame, modType: str, subsetCols: List[str]) -> Dict:
    out = {
        "modType": modType,
        "rows": 0,
        "dupRows": 0,
        "dupGroups": 0,
        "dupRowsPct": "0.0%",
        "exampleTopGroups": [],
    }

    if "modType" not in df.columns:
        return out

    d = df[df["modType"] == modType]
    out["rows"] = int(len(d))
    if d.empty or not subsetCols:
        return out

    dupMask = d.duplicated(subset=subsetCols, keep=False)
    dupRows = int(dupMask.sum())
    out["dupRows"] = dupRows
    out["dupRowsPct"] = _pct(dupRows, out["rows"])

    if dupRows > 0:
        grp = d.loc[dupMask].groupby(subsetCols, dropna=False).size().sort_values(ascending=False)
        out["dupGroups"] = int(len(grp))
        top = grp.head(10).reset_index()
        top.columns = subsetCols + ["count"]
        out["exampleTopGroups"] = top.to_dict(orient="records")

    return out


def _scenarioCoverageStats(df: pd.DataFrame) -> Dict:
    """
    How many scenarios per praID?
    Uses available scenario columns (resultID/flow/ppm/pem/rSize).
    """
    out: Dict = {
        "scenarioColsUsed": [],
        "nUniqueScenarios": None,
        "scenariosPerPraId": {},
        "flowCounts": {},
        "pemCountsRaw": {},
    }

    if "praID" not in df.columns:
        return out

    scenarioCols = [c for c in ["resultID", "flow", "ppm", "pem", "rSize"] if c in df.columns]
    out["scenarioColsUsed"] = scenarioCols

    # simple flow + pem distributions on RAW rows (so you see what is in the file)
    if "flow" in df.columns:
        out["flowCounts"] = df["flow"].astype("string").value_counts(dropna=False).to_dict()
    if "pem" in df.columns:
        sPem = pd.to_numeric(df["pem"], errors="coerce").dropna()
        out["pemCountsRaw"] = sPem.value_counts().sort_index().astype(int).to_dict()

    if not scenarioCols:
        return out

    # unique scenario combinations
    scenKey = df[scenarioCols].astype("string").agg("|".join, axis=1)
    out["nUniqueScenarios"] = int(scenKey.nunique(dropna=True))

    # scenarios per praID (unique scenario combos per praID)
    tmp = pd.DataFrame({"praID": df["praID"], "__scen": scenKey})
    perPra = tmp.groupby("praID", dropna=True)["__scen"].nunique()

    out["scenariosPerPraId"] = {
        "min": int(perPra.min()) if len(perPra) else 0,
        "p05": float(perPra.quantile(0.05)) if len(perPra) else 0.0,
        "median": float(perPra.median()) if len(perPra) else 0.0,
        "mean": float(perPra.mean()) if len(perPra) else 0.0,
        "p95": float(perPra.quantile(0.95)) if len(perPra) else 0.0,
        "max": int(perPra.max()) if len(perPra) else 0,
    }

    # show most common scenario-counts (e.g. many praIDs have 6 or 7 scenarios)
    vc = perPra.value_counts().sort_index()
    out["scenariosPerPraId"]["countTableTop"] = vc.tail(12).astype(int).to_dict()  # last 12 bins

    return out


def _basicNumericSummary(df: pd.DataFrame) -> Dict[str, Dict]:
    cols = [c for c in ["praAreaM", "praAreaSized", "praAreaVol", "pem", "ppm", "rSize"] if c in df.columns]
    out: Dict[str, Dict] = {}

    for c in cols:
        s = pd.to_numeric(df[c], errors="coerce")
        if s.notna().sum() == 0:
            continue
        out[c] = {
            "min": float(s.min()),
            "p05": float(s.quantile(0.05)),
            "median": float(s.median()),
            "mean": float(s.mean()),
            "p95": float(s.quantile(0.95)),
            "max": float(s.max()),
        }
    return out


# ------------------ Plots ------------------ #
def _plotRelPraAreaSized(df: pd.DataFrame, outDir: Path, suffix: str) -> None:
    """
    REL plot:
      - scenario-independent dedup (so same release geometry across scenarios counts once)
      - bar chart for discrete classes (2..5)
    """
    if "modType" not in df.columns or "praAreaSized" not in df.columns:
        log.info("Plot rel/praAreaSized: required columns missing → skipping (%s)", suffix)
        return

    rel = df[df["modType"] == "rel"].copy()
    if rel.empty:
        log.info("Plot rel/praAreaSized: no rel rows → skipping (%s)", suffix)
        return

    relUniq = _dedupScenarioIndependent(rel, modType="rel")
    s = pd.to_numeric(relUniq["praAreaSized"], errors="coerce").dropna()
    if s.empty:
        log.info("Plot rel/praAreaSized: no numeric data → skipping (%s)", suffix)
        return

    vc = s.value_counts().sort_index()
    classes = vc.index.astype(int).tolist()
    counts = vc.values.astype(int).tolist()

    title = f"rel: praAreaSized (scenario-independent unique) [{suffix}]"
    outPath = Path(outDir) / f"hist_rel_praAreaSized_{suffix}.png"
    _plotDiscreteBars(classes, counts, title, "praAreaSized", outPath)


def _plotResPem(df: pd.DataFrame, outDir: Path, suffix: str) -> None:
    """
    RES plot:
      - exact dedup ONLY (prevents accidental duplicates, keeps pem distribution meaningful)
      - bar chart for discrete classes (2..5)
    """
    if "modType" not in df.columns or "pem" not in df.columns:
        log.info("Plot res/pem: required columns missing → skipping (%s)", suffix)
        return

    res = df[df["modType"] == "res"].copy()
    if res.empty:
        log.info("Plot res/pem: no res rows → skipping (%s)", suffix)
        return

    resUniq = _dedupExact(res, modType="res")
    s = pd.to_numeric(resUniq["pem"], errors="coerce").dropna()
    if s.empty:
        log.info("Plot res/pem: no numeric data → skipping (%s)", suffix)
        return

    vc = s.value_counts().sort_index()
    classes = vc.index.astype(int).tolist()
    counts = vc.values.astype(int).tolist()

    title = f"res: pem (exact-unique rows) [{suffix}]"
    outPath = Path(outDir) / f"hist_res_pem_{suffix}.png"
    _plotDiscreteBars(classes, counts, title, "pem", outPath)


def _plotDiscreteBars(classes: List[int], counts: List[int], title: str, xlabel: str, outPath: Path) -> None:
    fig, ax = plt.subplots(figsize=(11, 6))

    # Use bar chart with stable spacing even if only one class exists
    x = np.array(classes, dtype=float)
    y = np.array(counts, dtype=float)

    # "wider bars" but not ridiculous
    width = 0.85
    ax.bar(x, y, width=width)

    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel("count")

    ax.set_xticks(x)
    ax.set_xticklabels([str(int(v)) for v in x])

    # avoid the "one bar fills the whole plot" feeling
    xmin = float(np.min(x) - 0.75)
    xmax = float(np.max(x) + 0.75)
    ax.set_xlim(xmin, xmax)

    ax.grid(axis="y", alpha=0.25)

    # class counts box on the right (no overlap)
    txt = "class counts:\n" + "\n".join(f"{c}: {n}" for c, n in zip(classes, counts))
    ax.text(
        1.02, 0.02, txt,
        transform=ax.transAxes,
        va="bottom",
        ha="left",
        fontsize=10,
        family="monospace",
        bbox=dict(boxstyle="round", facecolor="white", alpha=0.95),
        clip_on=False,
    )

    fig.tight_layout(rect=[0, 0, 0.82, 1])
    fig.savefig(outPath)
    plt.close(fig)
    log.info("Plot written: %s", outPath)


# ------------------ Markdown report helpers ------------------ #
def _writeRegionSection(mdLines: List[str], stats: Dict, header: str) -> None:
    mdLines.append(header)
    mdLines.append("")
    mdLines.append(f"- total rows (raw): **{stats.get('totalRows', 0)}**")
    mdLines.append(f"- rel rows (raw): **{stats.get('relRows', 0)}** ({stats.get('relPct', '0.0%')})")
    mdLines.append(f"- res rows (raw): **{stats.get('resRows', 0)}** ({stats.get('resPct', '0.0%')})")
    mdLines.append(f"- unique praID: **{stats.get('uniquePraIdAll', 0)}**")
    mdLines.append("")

    mdLines.append("### modType counts (raw)")
    mdLines.append("")
    mdLines.append(_toMdTableFromDict(stats.get("modCounts", {}), keyName="modType", valName="count"))
    mdLines.append("")

    mdLines.append("### scenario coverage")
    mdLines.append("")
    mdLines.append(_scenarioMd(stats.get("scenarioCoverage", {})))
    mdLines.append("")

    mdLines.append("### missing values (selected columns)")
    mdLines.append("")
    mdLines.append(_toMdTableFromDict(stats.get("naInfo", {}), keyName="column", valName="naCount"))
    mdLines.append("")

    mdLines.append("### duplicates (scenario-independent)")
    mdLines.append("")
    mdLines.append("REL (scenario-independent key):")
    mdLines.append("")
    mdLines.append(_dupMd(stats.get("relDupScenarioIndependent", {})))
    mdLines.append("")
    mdLines.append("RES (scenario-independent key):")
    mdLines.append("")
    mdLines.append(_dupMd(stats.get("resDupScenarioIndependent", {})))
    mdLines.append("")

    mdLines.append("### duplicates (exact)")
    mdLines.append("")
    mdLines.append("REL (exact key):")
    mdLines.append("")
    mdLines.append(_dupMd(stats.get("relDupExact", {})))
    mdLines.append("")
    mdLines.append("RES (exact key):")
    mdLines.append("")
    mdLines.append(_dupMd(stats.get("resDupExact", {})))
    mdLines.append("")

    mdLines.append("### numeric summary (raw rows)")
    mdLines.append("")
    mdLines.append(_numSummaryMd(stats.get("numSummaryRaw", {})))
    mdLines.append("")

    mdLines.append("### numeric summary (REL scenario-independent unique)")
    mdLines.append("")
    mdLines.append(_numSummaryMd(stats.get("numSummaryRelScenarioIndependent", {})))
    mdLines.append("")

    mdLines.append("### numeric summary (RES exact-unique rows)")
    mdLines.append("")
    mdLines.append(_numSummaryMd(stats.get("numSummaryResExact", {})))
    mdLines.append("")


def _scenarioMd(d: Dict) -> str:
    if not d:
        return "_(no scenario info available)_"

    lines: List[str] = []
    lines.append(f"- scenario cols used: **{', '.join(d.get('scenarioColsUsed') or []) or '<none>'}**")

    nUniq = d.get("nUniqueScenarios", None)
    if nUniq is not None:
        lines.append(f"- unique scenario combinations (raw rows): **{nUniq}**")

    sp = d.get("scenariosPerPraId", {}) or {}
    if sp:
        lines.append("")
        lines.append("Scenarios per praID (unique scenario combos):")
        lines.append(f"- min / median / p95 / max: **{sp.get('min', 0)} / {sp.get('median', 0):.0f} / {sp.get('p95', 0):.0f} / {sp.get('max', 0)}**")
        lines.append(f"- mean: **{sp.get('mean', 0):.2f}**")

        top = sp.get("countTableTop", {}) or {}
        if top:
            lines.append("")
            lines.append("Most common (high-end) scenario-counts per praID:")
            lines.append("")
            df = pd.DataFrame([{"scenariosPerPraID": k, "praIDcount": v} for k, v in top.items()])
            lines.append(df.to_markdown(index=False))

    flowCounts = d.get("flowCounts", {}) or {}
    if flowCounts:
        lines.append("")
        lines.append("Flow counts (raw rows):")
        lines.append("")
        lines.append(_toMdTableFromDict(flowCounts, keyName="flow", valName="count"))

    pemCounts = d.get("pemCountsRaw", {}) or {}
    if pemCounts:
        lines.append("")
        lines.append("pem counts (raw rows):")
        lines.append("")
        lines.append(_toMdTableFromDict(pemCounts, keyName="pem", valName="count"))

    return "\n".join(lines)


def _dupMd(d: Dict) -> str:
    rows = int(d.get("rows", 0) or 0)
    dupRows = int(d.get("dupRows", 0) or 0)
    dupGroups = int(d.get("dupGroups", 0) or 0)
    dupPct = d.get("dupRowsPct", "0.0%")

    lines: List[str] = []
    lines.append(f"- rows: **{rows}**")
    lines.append(f"- duplicate rows: **{dupRows}** ({dupPct})")
    lines.append(f"- duplicate groups: **{dupGroups}**")

    top = d.get("exampleTopGroups") or []
    if top:
        dfTop = pd.DataFrame(top)
        lines.append("")
        lines.append("Top duplicate groups (first 10):")
        lines.append("")
        lines.append(dfTop.to_markdown(index=False))

    return "\n".join(lines)


def _numSummaryMd(d: Dict[str, Dict]) -> str:
    if not d:
        return "_(no numeric summary available)_"
    df = pd.DataFrame(d).T.reset_index().rename(columns={"index": "column"})
    return df.to_markdown(index=False)


def _toMdTableFromDict(d: Dict, keyName: str, valName: str) -> str:
    if not d:
        return "_(none)_"
    df = pd.DataFrame([{keyName: k, valName: v} for k, v in d.items()])
    return df.to_markdown(index=False)


# ------------------ Dedup helpers ------------------ #
def _dedupScenarioIndependent(d: pd.DataFrame, modType: str) -> pd.DataFrame:
    cols = _dupSubsetColsScenarioIndependent(d, modType=modType)
    if not cols:
        return d
    return d.drop_duplicates(subset=cols, keep="first")


def _dedupExact(d: pd.DataFrame, modType: str) -> pd.DataFrame:
    cols = _dupSubsetColsExact(d, modType=modType)
    if not cols:
        return d
    return d.drop_duplicates(subset=cols, keep="first")


def _dupSubsetColsScenarioIndependent(df: pd.DataFrame, modType: str) -> List[str]:
    """
    Scenario-independent geometry identity:
    - excludes scenario params (flow/ppm/pem/rSize/resultID)
    - keeps “static” PRA/region/elev-band attributes
    """
    base = [
        "praID", "modType",
        "LKGebiet", "LKGebietID", "LKRegion", "LWDGebietID",
        "praAreaM", "praAreaSized", "praAreaVol",
        "praElevBand", "praElevBandRule",
        "praElevMax", "praElevMean", "praElevMin",
        "subC", "sector", "elevMin", "elevMax",
    ]
    cols = [c for c in base if c in df.columns]
    return cols


def _dupSubsetColsExact(df: pd.DataFrame, modType: str) -> List[str]:
    """
    Exact row identity:
    - includes scenario parameters (if present)
    - catches accidental double writes
    """
    base = _dupSubsetColsScenarioIndependent(df, modType=modType)
    scen = [c for c in ["resultID", "flow", "ppm", "pem", "rSize"] if c in df.columns]
    cols = base + scen
    # ensure unique + stable order
    seen = set()
    out = []
    for c in cols:
        if c not in seen:
            out.append(c)
            seen.add(c)
    return out


# ------------------ Utilities ------------------ #
def _loadResults(path: Path) -> pd.DataFrame:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(path)

    if _HAS_GPD:
        gdf = gpd.read_parquet(path)
        return pd.DataFrame(gdf.drop(columns="geometry", errors="ignore"))

    df = pd.read_parquet(path)
    # if there is a geometry-like column, drop it for stats
    for gc in ["geometry", "geom", "wkb_geometry"]:
        if gc in df.columns:
            df = df.drop(columns=[gc], errors="ignore")
            break
    return df


def _parseInputs(inputs: List[Dict]) -> List[resultsInput]:
    out: List[resultsInput] = []
    for i, item in enumerate(inputs):
        if not isinstance(item, dict):
            log.warning("Stats: skipping input %d (not a dict)", i)
            continue
        regionName = str(item.get("regionName", "")).strip()
        resultsParquet = item.get("resultsParquet", None)
        if not regionName or not resultsParquet:
            log.warning("Stats: skipping input %d (missing regionName/resultsParquet)", i)
            continue
        out.append(resultsInput(regionName=regionName, resultsParquet=Path(resultsParquet)))
    return out


def _pct(x: int, total: int) -> str:
    if total <= 0:
        return "0.0%"
    return f"{(100.0 * float(x) / float(total)):.1f}%"


def _safeKey(s: str) -> str:
    s = "".join(c for c in str(s) if c.isalnum() or c in "-_").strip()
    return s or "region"


def _requireCols(df: pd.DataFrame, cols: List[str]) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
