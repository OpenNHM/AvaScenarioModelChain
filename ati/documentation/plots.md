# Creating Plots

## `plotParameterization`

The `runPlotParameterisation.py` workflow creates plots to visually check the `mod2Mobility` size/parameterization
settings (`compParamsCfg.ini`).

It plots `alpha`, `umax`, `exp` (and, optionally, the friction parameters `mu`/`xi` that are derived from `alpha`and
`umax`) against avalanche size, release volume and elevation, for a configurable release area and elevation range.

Settings are read from the `[PLOT]` section of `(local_)compParamsCfg.ini`:

### Output

Plots are saved as `.png` files to `<avaDir>/Outputs/plotParameterization` (or to a custom path passed via
`savePlotPath`):

- `sizeCrossCheck.png` — cross-check of size, volume, elevation and derived parameters
- `parameters_size.png`, `parameters_Vrel.png`, `parameters_elevation.png` — `alpha`/`umax`(/`exp`) vs. each variable
- `muxi.png` — friction parameters `mu`/`xi` vs. avalanche size

Example plots of the default configuration see in [mod2Mobility](mod2Mobility.md).

### Run

```bash
python workflows/runPlotParameterisation.py
```

---
Go back to [main documentation](../../README.md).