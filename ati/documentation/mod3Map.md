# Module `mod3Map`

## Overview

The `mod3Map` module provides tools to map and classify terrain.

## `autoATESClassifier`

The `autoATESClassifier` classifies terrain into the four **Avalanche Terrain Exposure Scale (ATES)** classes,
`simple`, `challenging`, `complex` and `extreme`, based on slope angle, potential release areas (PRA), avalanche runout
(Flow-Py travel angle) and, optionally, forest density and forest interaction.

The autoATES v2.0 classifier was developed
by [Toft et al. (2023a)](https://doi.org/10.5194/nhess-2023-114); [Toft et al. (2023b)](https://github.com/AutoATES/AutoATES-v2.0/tree/main);
the implementation here follows that approach with minor modifications. For details on the underlying method, refer to
the two publications above.

### Modified Workflow

Compared to the publications mentioned above, we made the following changes (described in more detail in Hesselbach
(2023)[^1], [Huber et al. (2023)](https://arc.lib.montana.edu/snow-science/objects/ISSW2023_P2.48.pdf) and Spannring
(2024)[^2]):

- No overhead hazard is considered.
- Forest interaction: if a Flow-Py forest-interaction raster is available, it indicates how many forested cells an
  avalanche path has passed through. The autoATES classifier reclassifies cells, which are assigned to forest
  interaction value of `FORESTINT1` or higher, to a lower class. If a cell has a value higher than `FORESTINT2`, ATES
  class 3 is reclassified to ATES class 1.

### Input Files

The algorithm requires a digital elevation model, potential release areas (binary format), avalanche travel angle (in
°), and, optionally, a forest density layer and a forest-interaction layer. If `customPaths` is `True`, the paths to the
respective files are provided directly in the configuration file. If `customPaths` is `False`, the Inputs must follow
this folder
structure:

```text
<avaDir>/
└── Inputs/
    ├── digital elevation model (*.tif or *.asc)
    ├── REL
    │   └── PRA raster (*.tif or *.asc)
    └── RES/ or FOREST/
        └── forest density raster (*.tif or *.asc; optional)
└── Outputs/
    └── com4FlowPy/
        └── peakFiles/
            └── res_<flowpyHash>/
                ├── travel-angle raster, filename contains "fpTravelAngleMax" (*.tif)
                └── forest-interaction raster, filename contains "forestInteraction" (*.tif; optional)            
```

Configuration parameters can be adjusted in `(local_)autoATESClassifierCfg.ini`.

### Output Files

The resulting ATES layer, together with intermediate results, is written to the `Outputs/autoATES` folder:

```text
<avaDir>/
└── Outputs/
    └── autoATES/
        ├── ates_gen.tif
        └── ...
```

---

[^1]: Hesselbach, C., 2023: Adaptation and Application of an Automated Avalanche Terrain Classification in Austria.
Masters’ Thesis, Universität für Bodenkultur, Wien.

[^2]: Spannring., P., 2024: Comparison of two avalanche terrain classification approaches:
Avalanche Terrain Exposure Scale - Classified Avalanche Terrain. Masters’ Thesis, University of Innsbruck.

---

Go back to [main documentation](../../README.md).