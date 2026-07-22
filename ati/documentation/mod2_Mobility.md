# Module `mod2Mobility`

## Overview

The `mod2Mobility` module provides a computation tool that derives the **mobility parameters** required by avalanche
mobility simulation tools such as [AvaFrame::com4FlowPy](https://docs.avaframe.org/en/latest/moduleCom4FlowPy.html#).
These parameters are computed as a function of the release size (release area) and, optionally, of the local snow
climate.

## `compParams.py`

For each release area, the workflow implemented in `compParams.py` performs the following steps:

1. Compute the release thickness, either from the snow climate or as a constant value.
2. Compute the release volume.
3. Derive the avalanche size and, from it, the mobility parameters `alpha`, `umax` and `exp`.
4. Save the resulting parameter files for use by downstream modules.

The underlying relationships are based on the avalanche size classifications
of [EAWS](https://www.avalanches.org/standards/avalanche-size/), [CAA](https://avysavvy.avalanche.ca/en-ca/avalanche-sizes)
and [AAA](https://avalanche.org/avalanche-encyclopedia/avalanche/avalanche-problems/avalanche-size/).

Configuration parameters can be adjusted in `(local_)compParamsCfg.ini`.

---

## Derivation of the Mobility Parameters

The parameters are derived in four steps: **(1)** release thickness, **(2)** release volume, **(3)** avalanche size, and
**(4)** the mobility parameters `alpha`, `umax` and `exp`.

### 1. Release Thickness

The release thickness can either be set to a constant value (`constantPraThickness = True`), or computed from the snow
climate as a linear function of elevation `z`, using a reference thickness `D0` and a snow-depth gradient
`deltaD`:

```
thickness(z) = D0 + deltaD · z
```

`D0` (thickness at `z = 0`) and `deltaD` (change in thickness per unit elevation, e.g. `10 cm / 100 m` in an Alpine snow
climate) can both be adjusted in the configuration file.

### 2. Release Volume

A raster layer containing the release areas (in m²) is required (`Inputs/RelArea`). Given the release area `Arel`
and the thickness from Step 1, the release volume `Vrel` follows as:

```
Vrel = Arel · thickness
```

### 3. Avalanche Size

The avalanche size is a continuous quantity and expressed on the same scale as the EAWS/CAA/AAA size classes (from `1` -
`5`).

It is linked to the release volumes through the following empirical relationships:

```
Vrel = 5^(size - 2) · 1000
```

Combining this with the release volume from Step 2 (`Vrel = Arel · thickness`) gives the avalanche size directly as a
function of elevation and release area:

```
size = 2 + log5( Arel · thickness · 10⁻³ )
```

### 4. Mobility Parameters: `alpha`, `umax`, `exp`

With the avalanche size, the three Flow-Py mobility parameters are computed as follows.

#### runout angle `alpha`

`alpha` controls the stopping of the flow path: a smaller `alpha` allows the avalanche to travel farther.

```
alpha(size) = alphaSize2 - (size - 2) · deltaAlpha
```

where `alphaSize2` is `alpha` at `size = 2`, and `deltaAlpha` is the change in `alpha` per unit increase in size. Both
parameters can be adjusted via the corresponding entries in the configuration file.

#### maximum velocity limit `umax`

`umax` (or `umaxlim`) is the upper velocity limit a process can have.

```
umax(size) = uMaxSize2 + (size - 2) · deltaUMax
```

where `uMaxSize2` is `umax` at `size = 2`, and `deltaUMax` is the change in `umax` per unit increase in size. Both
parameters can be adjusted via the configuration file.

#### exponent `exp`

`exp` controls the lateral spread of the flow path: a larger `exp` produces a narrower flow.

```
exp(size) = expCoeff · expBase^size
```

with default values `expCoeff = 75` and `expBase = 0.64`. Both can be adjusted via the configuration file.

---

## Wet-Snow Avalanches

For **wet** avalanches, the parameters are derived from the dry-avalanche relationships above, using a shifted avalanche
size:

```
alpha_wet(size) = alpha_dry(size + sizeShiftAlpha)
umax_wet(size)  = umax_dry(size + sizeShiftUmax)
exp_wet(size)   = exp_dry(size + sizeShiftExp)
```

The shifts are configurable via `sizeShiftAlpha`, `sizeShiftUmax` and `sizeShiftExp`, with default values
`sizeShiftAlpha = 0.5`, `sizeShiftUmax = -0.75` and `sizeShiftExp = 0.5`.

Two additional rules apply:

- **Lower bound on `umax`:** any computed value below `5 m/s` is clamped to `5 m/s`.
- **Upper bound on size:** avalanche sizes larger than `4` are treated as `size = 4` when computing the mobility
  parameters (i.e. the parameters saturate at the size-4 values).

---

## Input Files

The workflow requires a digital elevation model and a raster containing the release area size (in m²). They need to be
provided in the following structure:

```text
<avaDir>/
└── Inputs/
    ├── DEM.tif or DEM.asc
    └── RELArea/
        └── releaseAreasM2.tif or *.asc
```

## Output Files

The parameterization files are saved within the following folder structure, so that a dynamic parameterization can be
run with
[AvaFrame::com4FlowPy](https://docs.avaframe.org/en/latest/moduleCom4FlowPy.html#iv-variable-parameters):

```text
<avaDir>/
└── Outputs/
    ├── ALPHA/
    │   └── alpha.tif
    ├── UMAX/
    │   └── umax.tif
    └── EXP/
        └── exp.tif
```

 
---

