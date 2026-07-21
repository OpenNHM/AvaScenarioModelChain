## AvaScenariosModelChain or Avalanche Terrain Identification (2026-06 Update)


<p align="center">
  <img src="https://media.giphy.com/media/3Xzlefv57zcrVIPPRN/giphy.gif" 
       alt="Avalanche Scenario Model Chain" 
       width="300"/>
</p>

<h4 align="center">⚠️ Handle with care — work in progress</h4>

---
The ATI (Avalanche Terrain Identification) repository contains several modules and workflows
to identify avalanche terrain.
The main modules are:

- **praDelineation** and **praUtils**: modules to delineate and segment PRAs
- **mobilityUtils**: preparation and parameterization for avalanche mobility simulations
- **mapper**: postprocessing tools that help interpreting, mapping, representing simulation results
- **workflows**: suggestions / ideas to combine the individual modules

### General run (Linux)

#### Requirements

Install [git](https://github.com/git-guides/install-git) and [pixi](https://pixi.sh/latest/#installation).

#### Setup

Clone the AvaScenariosModelChain repository (in a directory of your choice: [YOURDIR]) and change into it::

```bash
  cd [YOURDIR]
  git clone https://github.com/OpenNHM/AvaScenarioModelChain.git
  cd AvaScenarioModelChain
```

#### Run

Follow these steps to run a workflow.

- change into your `AvaScenariosModelChain` directory (replace [YOURDIR]
  with your path from the installation steps):

```bash
cd [YOURDIR]/AvaScenariosModelChain/ati
```

- Activate the environment:

```bash
pixi shell
```

- run:

```bash
python workflows/runAutoAtesModelChain.py
```

This will perform an autoATES workflow including PRA delineation and segmentation, simulating
avalanche mobility using AvaFrame::com4FlowPy with dynamic alpha angle and max velocity limit parameterization
and an autoATES classifier.
ATES-results are saved to `data/avaTestBowl/Outputs/autoATES`.

In the workflows folder are various workflow examples.

### Initialize project

To create the folder where the input data lies and where the
output results will be saved, specify the full path to the folder
in the ``local_atiCfg.ini`` (which is a copy of
``atiCfg.ini`` that you need to create).

```bash
cd ati
cp atiCfg.ini local_atiCfg.ini
```

and edit ``local_atiCfg.ini`` with your favorite text editor and adjust the
variable ``avalancheDirectory``.

Then provide the respective input data in `[avalancheDirectory]/Inputs`.

You can also have a look at the default setting for
the module you want to use (for example ``runAutoAtesModelChainCfg.ini`` for the autoATES workflow).
If you want to use different settings, create a ``local_`` copy of the ``.ini``
file and modify the desired parameters.

#### Hint:

for the workflows/runAutoAtesModelChain.py workflow, you need to
clone [AvaFrame](https://github.com/OpenNHM/AvaFrame)
in `[YOURDIR]` and checkout the branch: `PS_FP_changeCfgRead`,
then activate the dev environment:

```bash
pixi shell --environment dev
```

