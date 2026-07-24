# Workflow: Dynamic Parameterization
[AvaFrame::com4FlowPy](https://docs.avaframe.org/en/latest/moduleCom4FlowPy.html)
## Overview

## Overview
 
The **dynamic parameterization workflow** (`workflows/runDynamicParameterisation.py`) combines the `mod1Release`
and `mod2Mobility` modules to compute the mobility parameters `alpha`, `umax` and `exp` for a set of automatically
delineated release areas.

It can be used to create input parameters for a simulation run, e.g. with [AvaFrame::com4FlowPy](https://docs.avaframe.org/en/latest/moduleCom4FlowPy.html).

The workflow is implemented in `dynamicParameterisationMain` (`workflows/runDynamicParameterisation.py`).
 
---
 
## Workflow Steps
 
### 1. PRA Delineation and Preparation
 
Uses [`mod1Release`](mod1Release.md). Identical to Step 1 of the
[autoATES model chain](workflow_autoAtesModelChain.md#1-pra-delineation-and-preparation):
 
1. **PRA delineation** — `praDelineationVeitinger.runPraDelineation` derives raw potential release areas from the
   DEM (and, optionally, a forest mask), writing results to `Outputs/PraDelineation`.
2. **PRA processing (polygonizing)** — `praProcessing.runPraProcessing` converts the raw PRA raster into polygons.
3. **Subcatchments** — `praSubCatchments.runSubcatchments` derives subcatchments used to structure the release
   areas.
4. **PRA segmentation** — `praSegmentation.runPraSegmentation` segments the polygons into individual, distinct
   release areas.
5. **PRA rasterization** — `praPrepForFlowPy.runPraPrepForFlowPy` rasterizes the segmented release areas back to
   grids, writing a release-ID raster (`*-5-praID.tif`) and a release-area raster (`*-5-praAreaM.tif`) to
   `Work/praPrepForFlowPy`.
The generated rasters are then copied into the `Inputs` folder structure expected by `mod2Mobility`:
 
| Generated file            | Copied to           |
|----------------------------|----------------------|
| `*-5-praID.tif`            | `Inputs/RELID`       |
| `*-5-praAreaM.tif`         | `Inputs/RELArea`     |
| binarized `*-5-praAreaM.tif` (values `> 0` set to `1`) | `Inputs/REL/pra_binary.tif` |
 
> **Note:** if a `.tif` file already exists in `Inputs/REL`, `Inputs/RELID` or `Inputs/RELArea` before the workflow
> runs, the corresponding copy step is skipped and the existing file is used instead. This allows a custom PRA
> raster to be substituted for the automatically delineated one.
 
### 2. Dynamic Mobility Parameterization
 
Uses [`mod2Mobility`](mod2Mobility.md).
 
`compParams.computeAndSaveParameters` computes the mobility parameters `alpha`, `umax` and `exp` for each release
area, based on release volume and, optionally, snow climate and a temperature-dependent wet/dry blend (see the
`mod2Mobility` documentation for the underlying equations). The resulting rasters are saved to `Inputs/ALPHA`,
`Inputs/UMAX` and `Inputs/EXP`.
 
---
 
## Configuration
 
The workflow reads two configuration files:
 
- **`atiCfg.ini`** (general project settings): defines `avalancheDirectory`, the path to the project folder
  containing the `Inputs`/`Outputs`/`Work` structure. Use a `local_atiCfg.ini` copy for your own setup (see the
  main [README](../../README.md)).
- **`runDynamicParameterisationCfg.ini`** (workflow settings): for each sub-module, an override section controls
  whether that module's own default configuration is used, or whether the parameter values defined in this
  workflow config file override it.
| Override section                                  | Applies to                                                     |
|-------------------------------------------------- |----------------------------------------------------------------|
| `mod1Release_praDelineationVeitinger_override`    | `praDelineationVeitinger` (PRA delineation)                    |
| `mod1Release_mod1Release_override`                | PRA processing, subcatchments, segmentation, rasterization     |
| `mod2Mobility_compParams_override`                | `compParams` (mobility parameterization)                       |
 
Each section has a `defaultConfig` flag: if `True`, the sub-module's own default configuration is used as the
base configuration, with the parameter values given in `runDynamicParameterisationCfg.ini` applied on top as
overrides. If `False`, and a `local_` configuration file for that sub-module is available, that local
configuration is used instead.
 
As with `atiCfg.ini`, create a `local_runDynamicParameterisationCfg.ini` copy to adjust these settings for your
own run.
 
---
 
## Input Files
 
At minimum, the workflow requires a DEM that is provided in the following structure:
 
```text
<avaDir>/
└── Inputs/
    ├── DEM.tif or DEM.asc
    └── RES/ or FOREST/
        └── forest density raster (*.tif or *.asc; optional)
```
 
Release-area rasters (`Inputs/REL`, `Inputs/RELID`, `Inputs/RELArea`) do **not** need to be provided: they are
generated automatically by Step 1, unless matching files are already present (see the note above).
 
## Output Files
 
```text
<avaDir>/
├── Inputs/
│   ├── REL/                        # binarized PRA raster (Step 1)
│   ├── RELID/                      # PRA ID raster (Step 1)
│   ├── RELArea/                    # PRA area raster (Step 1)
│   └── ALPHA/, UMAX/, EXP/         # mobility parameters (Step 2)
├── Outputs/
│   └── PraDelineation/             # raw PRA delineation (Step 1.1)
└── Work/
    └── praPrepForFlowPy/           # intermediate PRA rasterization files (Step 1.5)
```
 
---
 
## How to Run
 
```bash
cd [YOURDIR]/AvaScenarioModelChain
pixi shell
python workflows/runDynamicParameterisation.py
```
 
See the main [README](../../README.md) for full installation and setup instructions, including how to set up
`local_atiCfg.ini` and the `Inputs` folder.
 
---

Go back to [main documentation](../../README.md).