# Workflow: autoATES Model Chain

## Overview

The **autoATES model chain** (`workflows/runAutoAtesModelChain.py`) combines the `mod1Release`, `mod2Mobility` and
`mod3Map` modules with [AvaFrame::com4FlowPy](https://docs.avaframe.org/en/latest/moduleCom4FlowPy.html#) into a single
workflow that computes an **Avalanche Terrain Exposure Scale (ATES)** map from a digital elevation model (DEM) and,
optionally, a forest density layer:

1. Delineate, segment and prepare potential release areas (PRA).
2. Compute dynamic mobility parameters (`alpha`, `umax`, `exp`) for each release area based on its size.
3. Simulate avalanche runout with `AvaFrame::com4FlowPy`.
4. Classify the terrain into ATES classes with the `autoATES` classifier.

The workflow is implemented in `autoAtesModelChainMain` (`workflows/runAutoAtesModelChain.py`).

---

## Workflow Steps

### 1. PRA Delineation, Segmentation and Preparation

Uses [`mod1Release`](mod1Release.md).

1. **PRA delineation** — `praDelineationVeitinger.runPraDelineation` derives raw potential release areas from the DEM
   (and, optionally, a forest mask), writing results to `Outputs/PraDelineation`.
2. **PRA processing (polygonizing)** — `praProcessing.runPraProcessing` converts the raw PRA raster into polygons.
3. **Subcatchments** — `praSubCatchments.runSubcatchments` derives subcatchments used to structure the release areas.
4. **PRA segmentation** — `praSegmentation.runPraSegmentation` segments the polygons into individual, distinct release
   areas.
5. **PRA rasterization** — `praPrepForFlowPy.runPraPrepForFlowPy` rasterizes the segmented release areas back to grids
   required for `com4FlowPy`, writing a release-ID raster (`*-5-praID.tif`) and a release-area raster
   (`*-5-praAreaM.tif`) to `Work/praPrepForFlowPy`.

The generated rasters are then copied into the `Inputs` folder structure expected by the downstream modules:

| Generated file                                         | Copied to                   |
|--------------------------------------------------------|-----------------------------|
| `*-5-praID.tif`                                        | `Inputs/RELID`              |
| `*-5-praAreaM.tif`                                     | `Inputs/RELArea`            |
| binarized `*-5-praAreaM.tif` (values `> 0` set to `1`) | `Inputs/REL/pra_binary.tif` |

> **Note:** if a `.tif` file already exists in `Inputs/REL`, `Inputs/RELID` or `Inputs/RELArea` before the workflow
> runs, the corresponding copy step is skipped and the existing file is used instead. This allows a custom PRA
> raster to be substituted for the automatically delineated one.

### 2. Dynamic Mobility Parameterization

Uses [`mod2Mobility`](mod2Mobility.md).

`compParams.computeAndSaveParameters` computes the mobility parameters `alpha`, `umax` and `exp` for each release area,
based on release volume and, optionally, snow climate (see the `mod2Mobility` documentation for the underlying
equations).

### 3. Avalanche Simulation (`AvaFrame::com4FlowPy`)

`AvaFrame::com4FlowPy` is executed via `runCom4FlowPy.main`, using the release areas and mobility parameters from the
previous steps. The run is identified by a unique hash (`FlowpyHash`), which is used to locate its results in
`Outputs/com4FlowPy/peakFiles/res_<FlowpyHash>`, including the avalanche travel-angle raster (`fpTravelAngleMax`) and,
optionally, the forest-interaction raster (`forestInteraction`) consumed by the next step. Refer to the
[AvaFrame documentation](https://docs.avaframe.org/en/latest/moduleCom4FlowPy.html) for details on `com4FlowPy`
itself.

### 4. ATES Classification

Uses [`mod3Map`](mod3Map.md).

`autoATESClassifier.autoATESClassifierMain` combines the DEM, the PRA raster, the `com4FlowPy` travel-angle raster and,
optionally, forest density and forest-interaction rasters into the final ATES classification, written to
`Outputs/autoATES`.

---

## Configuration

The workflow reads two configuration files:

- **`atiCfg.ini`** (general project settings): defines `avalancheDirectory`, the path to the project folder containing
  the `Inputs`/`Outputs`/`Work` structure. Use a `local_atiCfg.ini` copy for your own setup (see the
  main [README](../../README.md)).
- **`runAutoAtesModelChainCfg.ini`** (workflow settings): for each sub-module, an override section controls whether that
  module's own default configuration is used, or whether the parameter values defined in this workflow config file
  override it:

| Override section                                | Applies to                                                                |
|--------------------------------------------------|----------------------------------------------------------------------------|
| `mod1Release_praDelineationVeitinger_override`    | `praDelineationVeitinger` (PRA delineation)                                |
| `mod1Release_mod1Release_override`                | PRA processing, subcatchments, segmentation, rasterization                 |
| `mod2Mobility_compParams_override`                | `compParams` (mobility parameterization)                                   |
| `com4FlowPy_com4FlowPy_override`                  | `AvaFrame::com4FlowPy`                                                      |
| `mod3Map_autoATESClassifier_override`             | `autoATESClassifier`                                                       |

  Each section has a `defaultConfig` flag: if `True`, the sub-module's own default configuration is used as the base
  configuration, with the parameter values given in `runAutoAtesModelChainCfg.ini` applied on top as overrides. If
  `False`, and a `local_` configuration file for that sub-module is available, that local configuration is used instead
  (more information
  see [here]([https://docs.avaframe.org/en/latest/complexUsage.html#override-configuration](https://docs.avaframe.org/en/latest/complexUsage.html#override-configuration))).

  As with `atiCfg.ini`, create a `local_runAutoAtesModelChainCfg.ini` copy to adjust these settings for your own run.

---

## Input Files

At minimum, the workflow requires a DEM. A forest raster (values between 0 and 1) is optional and, if provided, is used
both for PRA delineation (`mod1Release`), for the avalanche mobility simulation and for the ATES classification
(`mod3Map`). that is provided in the following structure:

```text
<avaDir>/
└── Inputs/
    ├── DEM.tif or DEM.asc
    └── RES/ or FOREST/
        └── (normalized) forest density raster (*.tif or *.asc; optional)
```

> **Hint:** if no forest layer is provided, make sure to set `forest = False` in the `com4FlowPy` section of the
> configuration file (`com4FlowPy_com4FlowPy_override` in `(local_)runAutoAtesModelChainCfg.ini`), otherwise
> `com4FlowPy` will expect a forest raster to be present.


Release-area rasters (`Inputs/REL`, `Inputs/RELID`, `Inputs/RELArea`) do **not** need to be provided: they are generated
automatically by Step 1, unless matching files are already present (see the note above).

## Output Files

```text
<avaDir>/
├── Inputs/
│   ├── REL/                        # binarized PRA raster (Step 1)
│   ├── RELID/                      # PRA ID raster (Step 1)
│   ├── RELArea/                    # PRA area raster (Step 1)
│   └── ALPHA/, UMAX/, EXP/         # mobility parameters (Step 2)
├── Outputs/
│   ├── PraDelineation/             # raw PRA delineation (Step 1.1)
│   ├── com4FlowPy/
│   │   └── peakFiles/
│   │       └── res_<FlowpyHash>/   # com4FlowPy simulation results (Step 3)
│   └── autoATES/                   # final ATES classification (Step 4)
└── Work/
    └── praPrepForFlowPy/           # intermediate PRA rasterization files (Step 1.5)
```

---

## How to Run

```bash
cd [YOURDIR]/AvaScenarioModelChain
pixi shell
python workflows/runAutoAtesModelChain.py
```

See the main [README](../../README.md) for full installation and setup instructions, including how to set up
`local_atiCfg.ini` and the `Inputs` folder.

---

## Notes

For details on the individual processing steps and their parameters, refer to the module documentation:
[`mod1Release`](mod1Release.md), [`mod2Mobility`](mod2Mobility.md), [`mod3Map`](mod3Map.md).

---

Go back to [main documentation](../../README.md).