# Module `mod3Map`

## Overview

The `mod3Map` module provides tools to map and classify terrain.

## `autoATESClassifier`

The `autoATESClassifier` classifies terrain into the four **Avalanche Terrain Exposure Scale (ATES)** classes —
`simple`, `challenging`, `complex` and `extreme` — based on slope angle, potential release areas (PRA), avalanche runout
(Flow-Py travel angle) and, optionally, forest density and forest interaction.

The autoATES v2.0 classifier was developed
by [Toft et al. (2023a)](https://doi.org/10.5194/nhess-2023-114); [Toft et al. (2023b)](https://github.com/AutoATES/AutoATES-v2.0/tree/main);
the implementation here follows that approach with minor modifications. For details on the underlying method, refer to
the two publications above.

### Adapted Workflow

Compared to the publications mentioned above, we made the following changes (described in more detail in Hesselbach
(2023), [Huber et al. (2023)](https://arc.lib.montana.edu/snow-science/objects/ISSW2023_P2.48.pdf) and Spannring
(2024)):

- No overhead hazard is considered.
- **Forest interaction**: if a Flow-Py forest-interaction raster is available, it indicates how many forested cells an
  avalanche path has passed through. Paths running through a moderate number of forested cells (`FORESTINT1`–
  `FORESTINT2`) are downgraded by one ATES class; paths running through more forested cells than
  `FORESTINT2` are downgraded to `simple` (class 1), except where already `extreme`.

### Input Files

The algorithm requires a digital elevation model, potential release areas (binary format), avalanche travel angle (in
°), and, optionally, a forest layer and layer of interaction between avalanche and forest. If `customPaths` is `True`,
the paths to the respective files are provided. If `customPaths` is `False`, the Inputs must follow this folder
structure:

```text
<avaDir>/
└── Inputs/
    ├── digital elevation model (*.tif or *.asc)
    ├── REL
        └── binary PRAs (.tif or *.asc)
    └── RES/
        └── forest density (.tif or *.asc; optionally)
└── Outputs/
    └── com4FlowPy/
        └── peakFiles/
            └── res_<flowpyHash>/
                └── travel angle file, the name contains "fpTravelAngleMax" (*.tif or *.asc)
                └── forest interaction file, the name contains "forestInteraction" (*.tif or *.asc; optionally)
            
```

Configuration parameters can be adjusted in `(local_)autoATESClassifierCfg.ini`.

### Output Files

The resulting ATES layer, together with intermediate results, is written to the `Outputs` folder:

```text
<avaDir>/
└── Outputs/
    └── autoATES/
        ├── ates_gen.tif
        └── ...
```

 
---