# Module `mod1Release`

## Overview

The `mod1Release` module provides tools to derive **potential release areas (PRAs)**, e.g. as basis for avalanche
simulations.

## `praDelineationVeitinger`

This algorithm delineates potential release areas based on slope, a wind-shelter index, and terrain roughness, all
derived from a digital elevation model (DEM). An optional forest mask can be supplied to exclude forested areas from the
delineation.

The method follows a **fuzzy-logic approach**: each raster cell
is assigned a continuous degree of membership, reflecting how likely it is to be a release area.

The algorithm was developed by [Veitinger et al. (2016)](https://nhess.copernicus.org/articles/16/2211/2016/) and
extended by [Sharp (2018)](https://doi.org/10.13140/RG.2.2.18673.94567.); the implementation here follows that approach
with minor modifications. The original code can be found in
this [repository](https://github.com/jocha81/Avalanche-release). For details on the underlying method, refer to the two
publications above.

### Input Files

The algorithm requires a digital elevation model and, optionally, a forest layer (raster). Inputs must follow this
folder structure:

```text
<avaDir>/
└── Inputs/
    ├── digital elevation model (*.tif or *.asc)
    └── RES/
        └── forest density (.tif or *.asc)
```

Configuration parameters can be adjusted in `(local_)praDelineationVeitingerCfg.ini`.

### Output Files

The resulting release area, together with intermediate results, is written to the `Outputs` folder:

```text
<avaDir>/
└── Outputs/
    └── PraDelineation/
        ├── pra.tif
        └── ...
```

 
---

## `praSegmentation`

-> Chris :)

---
Go back to [main documentation](../../README.md).
