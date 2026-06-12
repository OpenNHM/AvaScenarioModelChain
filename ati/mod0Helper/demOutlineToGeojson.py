#!/usr/bin/env python3
"""Create a GeoJSON boundary from the valid DEM footprint where values are > 1."""

# python python ati/mod0Helper/demOutlineToGeojson.py /path/to/dem.tif


from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Any

import numpy as np
import rasterio
from rasterio.features import shapes
from shapely.geometry import MultiPolygon, Polygon, mapping, shape
from shapely.ops import unary_union
from shapely.validation import make_valid


log = logging.getLogger(__name__)


# --- Geometry helpers --- #

def ensureMultiPolygon(geometry: Any) -> MultiPolygon:
    """Return polygonal geometry as a MultiPolygon."""
    geometry = make_valid(geometry)

    if isinstance(geometry, Polygon):
        return MultiPolygon([geometry])

    if isinstance(geometry, MultiPolygon):
        return geometry

    if geometry.geom_type == "GeometryCollection":
        polygons = []
        for part in geometry.geoms:
            if isinstance(part, Polygon):
                polygons.append(part)
            elif isinstance(part, MultiPolygon):
                polygons.extend(part.geoms)

        if polygons:
            return MultiPolygon(polygons)

    raise ValueError(
        f"DEM mask produced unsupported geometry type: {geometry.geom_type}"
    )


def deriveDemOutline(
    demPath: Path,
    threshold: float = 1.0,
) -> tuple[MultiPolygon, int]:
    """Polygonize and dissolve all DEM cells with values greater than threshold."""
    with rasterio.open(demPath) as src:
        demData = src.read(1, masked=True)
        demValues = np.asarray(demData.data)

        validMask = (
            ~np.ma.getmaskarray(demData)
            & np.isfinite(demValues)
            & (demValues > threshold)
        )

        if not np.any(validMask):
            raise ValueError(
                f"No DEM cells with values > {threshold} found in {demPath}"
            )

        polygonParts = [
            shape(geometry)
            for geometry, value in shapes(
                validMask.astype(np.uint8),
                mask=validMask,
                transform=src.transform,
            )
            if int(value) == 1
        ]

        if not polygonParts:
            raise ValueError(f"Could not polygonize DEM outline for {demPath}")

        dissolved = unary_union(polygonParts)
        multiPolygon = ensureMultiPolygon(dissolved)

        epsgCode = src.crs.to_epsg() if src.crs is not None else None
        if epsgCode is None:
            raise ValueError(
                "Input DEM CRS has no resolvable EPSG code."
            )

    return multiPolygon, epsgCode


# --- GeoJSON writer --- #

def createDemOutlineGeojson(
    demPath: str | Path,
    outputPath: str | Path | None = None,
    threshold: float = 1.0,
) -> Path:
    """Create the dissolved DEM outline GeoJSON with fixed CAIROS attributes."""
    demPath = Path(demPath).expanduser().resolve()
    if not demPath.is_file():
        raise FileNotFoundError(f"DEM not found: {demPath}")

    if outputPath is None:
        outputPath = demPath.with_name(f"{demPath.stem}_outline.geojson")
    else:
        outputPath = Path(outputPath).expanduser().resolve()

    outputPath.parent.mkdir(parents=True, exist_ok=True)

    geometry, epsgCode = deriveDemOutline(demPath, threshold=threshold)

    geojson = {
        "type": "FeatureCollection",
        "name": demPath.stem,
        "crs": {
            "type": "name",
            "properties": {
                "name": f"urn:ogc:def:crs:EPSG::{epsgCode}"
            },
        },
        "features": [
            {
                "type": "Feature",
                "properties": {
                    "LKGebiet": "1",
                    "LKGebietID": "1",
                    "LKRegion": "1",
                    "LWDGebietID": "1",
                },
                "geometry": mapping(geometry),
            }
        ],
    }

    with outputPath.open("w", encoding="utf-8") as geojsonFile:
        json.dump(
            geojson,
            geojsonFile,
            ensure_ascii=False,
            indent=2,
            allow_nan=False,
        )

    log.info(
        "DEM outline written to: %s (threshold > %s, EPSG:%s)",
        outputPath,
        threshold,
        epsgCode,
    )
    return outputPath


# --- Command line --- #

def parseArguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create a dissolved MultiPolygon GeoJSON from DEM cells "
            "whose values are greater than a threshold."
        )
    )
    parser.add_argument("demPath", type=Path, help="Input DEM raster")
    parser.add_argument(
        "-o",
        "--output",
        dest="outputPath",
        type=Path,
        default=None,
        help="Output GeoJSON path; default: <DEM stem>_outline.geojson",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=1.0,
        help="Keep DEM cells with values greater than this value; default: 1",
    )
    return parser.parse_args()


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s:%(name)s: %(message)s",
    )

    args = parseArguments()

    try:
        createDemOutlineGeojson(
            demPath=args.demPath,
            outputPath=args.outputPath,
            threshold=args.threshold,
        )
    except Exception:
        log.exception("DEM outline creation failed")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
