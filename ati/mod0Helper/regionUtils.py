"""Shared processing-boundary and static-region helpers."""

import logging
import pathlib

import geopandas as gpd

from ati.mod0Helper.demOutlineToGeojson import createDemOutlineGeojson


log = logging.getLogger(__name__)


def _inputPath(inputDir, configuredPath):
    """Resolve a configured input path against the workflow input directory."""
    path = pathlib.Path(configuredPath).expanduser()
    return path if path.is_absolute() else pathlib.Path(inputDir) / path


def _ensureRegionsSection(cfg):
    """Return the region section, creating it when necessary."""
    if not cfg.has_section("REGIONS"):
        cfg.add_section("REGIONS")
    return cfg["REGIONS"]


def getBoundaryName(cfg):
    """Return the configured processing-boundary path.

    Parameters
    ----------
    cfg : configparser.ConfigParser
        Model-chain configuration.

    Returns
    -------
    str
        New region setting or the legacy ``MAIN.BOUNDARY`` value.
    """
    if cfg.has_section("REGIONS"):
        boundaryName = cfg["REGIONS"].get("boundaryFile", "").strip()
        if boundaryName:
            return boundaryName
    return cfg["MAIN"].get("BOUNDARY", "").strip()


def resolveBoundary(cfg, workFlowDir):
    """Resolve or generate the workflow processing boundary.

    Parameters
    ----------
    cfg : configparser.ConfigParser
        Model-chain configuration. The effective boundary is recorded in both
        the new and legacy configuration locations.
    workFlowDir : dict
        Workflow directory mapping.

    Returns
    -------
    pathlib.Path
        Existing custom boundary or generated DEM outline.

    Raises
    ------
    FileNotFoundError
        If a configured boundary or the DEM does not exist.
    """
    inputDir = pathlib.Path(workFlowDir["inputDir"])
    boundaryName = getBoundaryName(cfg)

    if boundaryName:
        boundaryPath = _inputPath(inputDir, boundaryName)
        if not boundaryPath.is_file():
            raise FileNotFoundError(f"Boundary file not found: {boundaryPath}")
        log.info("Step 00: Using configured processing boundary: %s", boundaryPath)
    else:
        demName = cfg["MAIN"].get("DEM", "").strip()
        demPath = _inputPath(inputDir, demName)
        if not demPath.is_file():
            raise FileNotFoundError(f"DEM not found for boundary generation: {demPath}")
        boundaryPath = demPath.with_name(f"{demPath.stem}_outline.geojson")
        createDemOutlineGeojson(demPath, boundaryPath, threshold=None)
        log.info("Step 00: Generated processing boundary from valid DEM cells: %s", boundaryPath)

    boundary = gpd.read_file(boundaryPath)
    if boundary.empty:
        raise ValueError(f"Processing boundary is empty: {boundaryPath}")
    if boundary.crs is None:
        raise ValueError(f"Processing boundary has no CRS: {boundaryPath}")

    effectiveName = boundaryPath.name
    if boundaryPath.parent.resolve() != inputDir.resolve():
        effectiveName = str(boundaryPath)
    regions = _ensureRegionsSection(cfg)
    regions["boundaryFile"] = effectiveName
    cfg["MAIN"]["BOUNDARY"] = effectiveName
    return boundaryPath


def getMaskRegionSettings(cfg):
    """Return the optional PRA-selection mask settings.

    Parameters
    ----------
    cfg : configparser.ConfigParser
        Model-chain configuration.

    Returns
    -------
    tuple
        Boolean activation flag and configured mask path. Legacy configuration
        keys are used when the new section is unavailable.
    """
    legacyEnabled = cfg["praSELECTION"].getboolean("maskCommRegion", fallback=False)
    legacyPath = cfg["MAIN"].get("COMMISSIONREGION", "").strip()
    enabled = False
    path = ""

    if cfg.has_section("REGIONS"):
        enabled = cfg["REGIONS"].getboolean("applyRegionMask", fallback=False)
        path = cfg["REGIONS"].get("regionMaskFile", "").strip()
    if not enabled and not path and legacyEnabled:
        enabled = True
        path = legacyPath
        log.warning(
            "Deprecated maskCommRegion/COMMISSIONREGION settings used; "
            "prefer [REGIONS] applyRegionMask/regionMaskFile."
        )
    return enabled, path


def getAdminRegionSettings(cfg):
    """Return static administrative-region settings.

    Parameters
    ----------
    cfg : configparser.ConfigParser
        Model-chain configuration.

    Returns
    -------
    tuple
        Activation flag and polygon path. Legacy configurations enable
        assignment when ``MAIN.COMMISSIONS`` is set.
    """
    legacyPath = cfg["MAIN"].get("COMMISSIONS", "").strip()

    if cfg.has_section("REGIONS"):
        regions = cfg["REGIONS"]
        enabled = regions.getboolean("assignAdminMetadata", fallback=False)
        path = regions.get("adminMetadataFile", fallback=legacyPath).strip()
        if not enabled and not path and legacyPath:
            enabled = True
            path = legacyPath
            log.warning(
                "Deprecated MAIN.COMMISSIONS setting used; prefer "
                "[REGIONS] assignAdminMetadata/adminMetadataFile."
            )
    else:
        enabled = bool(legacyPath)
        path = legacyPath

    return enabled, path


def loadAdminRegions(cfg, inputDir, targetCrs):
    """Load optional administrative polygons and their available attributes.

    Parameters
    ----------
    cfg : configparser.ConfigParser
        Model-chain configuration.
    inputDir : str or pathlib.Path
        Workflow input directory.
    targetCrs : pyproj.CRS
        CRS used by the PRA geometries.

    Returns
    -------
    geopandas.GeoDataFrame or None
        Administrative polygons, or ``None`` when assignment is disabled.

    Raises
    ------
    FileNotFoundError
        If assignment is enabled but its polygon file is unavailable.
    ValueError
        If CRS information is missing.
    """
    enabled, configuredPath = getAdminRegionSettings(cfg)
    if not enabled:
        return None
    if not configuredPath:
        raise FileNotFoundError(
            "assignAdminMetadata=True but adminMetadataFile is empty."
        )

    path = _inputPath(inputDir, configuredPath)
    if not path.is_file():
        raise FileNotFoundError(f"Administrative region file not found: {path}")

    regions = gpd.read_file(path)
    if regions.crs is None:
        raise ValueError(f"Administrative region file has no CRS: {path}")
    if regions.crs != targetCrs:
        regions = regions.to_crs(targetCrs)
    metadataColumns = [column for column in regions.columns if column != regions.geometry.name]
    log.info("Administrative metadata columns: %s", metadataColumns)
    return regions


def assignAdminMetadata(praGdf, adminRegions):
    """Assign static administrative attributes to PRA geometries.

    Parameters
    ----------
    praGdf : geopandas.GeoDataFrame
        PRA geometries to enrich.
    adminRegions : geopandas.GeoDataFrame or None
        Administrative polygons and the attributes to transfer.

    Returns
    -------
    geopandas.GeoDataFrame
        PRA data with all available administrative attributes.
    """
    pra = praGdf.copy()
    if adminRegions is None:
        return pra

    metadataColumns = [
        column for column in adminRegions.columns if column != adminRegions.geometry.name
    ]
    joined = gpd.sjoin(pra, adminRegions, how="left", predicate="intersects")
    results = {column: [None] * len(pra) for column in metadataColumns}

    for index, group in joined.groupby(joined.index):
        geometry = pra.loc[index].geometry
        if geometry is None or geometry.is_empty:
            continue
        candidateIds = group["index_right"].dropna().astype(int).unique()
        candidates = adminRegions.loc[candidateIds]
        if candidates.empty:
            continue
        overlapAreas = candidates.geometry.intersection(geometry).area
        best = candidates.loc[overlapAreas.idxmax()]
        position = pra.index.get_loc(index)
        for column in metadataColumns:
            results[column][position] = best[column]

    for column in metadataColumns:
        pra[column] = results[column]
    return pra


def validateConfiguredRegionInputs(cfg, workFlowDir):
    """Validate optional selection-mask and administrative input files.

    Parameters
    ----------
    cfg : configparser.ConfigParser
        Model-chain configuration.
    workFlowDir : dict
        Workflow directory mapping.

    Raises
    ------
    FileNotFoundError
        If an enabled optional input is unavailable.
    ValueError
        If a configured spatial dataset is empty or lacks CRS information.
    """
    inputDir = pathlib.Path(workFlowDir["inputDir"])
    maskEnabled, maskPath = getMaskRegionSettings(cfg)
    if maskEnabled:
        if not maskPath:
            raise FileNotFoundError("applyRegionMask=True but regionMaskFile is empty.")
        resolvedMask = _inputPath(inputDir, maskPath)
        if not resolvedMask.is_file():
            raise FileNotFoundError(f"Region mask file not found: {resolvedMask}")
        mask = gpd.read_file(resolvedMask)
        if mask.empty:
            raise ValueError(f"Region mask file is empty: {resolvedMask}")
        if mask.crs is None:
            raise ValueError(f"Region mask file has no CRS: {resolvedMask}")

    adminEnabled, adminPath = getAdminRegionSettings(cfg)
    if adminEnabled:
        if not adminPath:
            raise FileNotFoundError(
                "assignAdminMetadata=True but adminMetadataFile is empty."
            )
        resolvedAdmin = _inputPath(inputDir, adminPath)
        if not resolvedAdmin.is_file():
            raise FileNotFoundError(f"Administrative region file not found: {resolvedAdmin}")
        admin = gpd.read_file(resolvedAdmin, rows=1)
        if admin.empty:
            raise ValueError(f"Administrative region file is empty: {resolvedAdmin}")
        if admin.crs is None:
            raise ValueError(f"Administrative region file has no CRS: {resolvedAdmin}")
        metadataColumns = [column for column in admin.columns if column != admin.geometry.name]
        log.info("Administrative metadata columns: %s", metadataColumns)
