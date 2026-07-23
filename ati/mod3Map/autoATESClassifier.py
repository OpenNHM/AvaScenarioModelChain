# Initial authors of the script:
# Håvard B. Toft and John Sykes (Toft, H. B., Sykes, J. M., and Schauer, A.: AutoATES-v2.0, GitHub
# [code], https://github.com/AutoATES (last access: 19 Jan-
# uary 2024), 2024.)

# Modified by Christoph Hesselbach, Andreas Huber, Paula Spannring

import pathlib
import numpy as np
import rasterio, rasterio.mask
from osgeo import gdal
import os
import csv
import logging
from skimage import morphology
import scipy.ndimage

from ati.mod3Map import autoATESClassifier
import ati.mod0Helper.dataUtils as dataUtils

import avaframe.in3Utils.cfgUtils as cfgUtils
import avaframe.in1Data.getInput as getInput

log = logging.getLogger("avaframe.ati.autoATESClassifier")

def autoATESClassifierMain(cfg=None, avaDir=None, flowpyHash=None):
    if cfg is None:
        cfg = cfgUtils.getModuleConfig(autoATESClassifier)
    if cfg["PATHS"].getboolean("customPaths"):
        # paths
        # --- Where autoATES will save results:
        wd = cfg["PATHS"].get("wd")

        DEM = cfg["PATHS"].get("DEM")
        canopy = cfg["PATHS"].get("canopy")
        forest_int = cfg["PATHS"].get("forest_int")
        FP = cfg["PATHS"].get("FP")
        SZ = cfg["PATHS"].get("SZ")
    else:
        if avaDir is None or flowpyHash is None:
            message = "Either provide an avaDir and FlowPy simHash or the paths in the ini file and set customPaths to True"
            raise ValueError(message)
        avaDir = pathlib.Path(avaDir)
        wd = avaDir / "Outputs" / "autoATES"
        os.makedirs(wd, exist_ok=True)
        DEM = getInput.getDEMPath(avaDir)
        canopy, _, _ = getInput.getAndCheckInputFiles(
            avaDir / "Inputs", "RES", "Forest", fileExt=["asc", "tif"]
        )
        if not canopy:
            canopy, _, _ = getInput.getAndCheckInputFiles(
                avaDir / "Inputs", "FOREST", "Forest", fileExt=["asc", "tif"]
            )
        SZ, _, _ = getInput.getAndCheckInputFiles(
            avaDir / "Inputs", "REL", "PRAs", fileExt=["asc", "tif"]
        )
        flowPyOutDir = (
            avaDir / "Outputs" / "com4FlowPy" / "peakFiles" / f"res_{flowpyHash}"
        )
        FP = list(flowPyOutDir.glob("*fpTravelAngleMax*"))[0]
        forest_int = list(flowPyOutDir.glob("*forestInteraction*"))
        if len(forest_int) > 0:
            forest_int = forest_int[0]
        else:
            forest_int = ""

    # parameters
    WIN_SIZE = cfg["PARAMETERS"].getfloat("WIN_SIZE")
    SAT01 = cfg["PARAMETERS"].getfloat("SAT01")
    SAT12 = cfg["PARAMETERS"].getfloat("SAT12")
    SAT23 = cfg["PARAMETERS"].getfloat("SAT23")
    SAT34 = cfg["PARAMETERS"].getfloat("SAT34")
    AAT1 = cfg["PARAMETERS"].getfloat("AAT1")
    AAT2 = cfg["PARAMETERS"].getfloat("AAT2")
    AAT3 = cfg["PARAMETERS"].getfloat("AAT3")
    TREE1 = cfg["PARAMETERS"].getfloat("TREE1")
    TREE2 = cfg["PARAMETERS"].getfloat("TREE2")
    TREE3 = cfg["PARAMETERS"].getfloat("TREE3")
    FORESTINT1 = cfg["PARAMETERS"].getfloat("FORESTINT1")
    FORESTINT2 = cfg["PARAMETERS"].getfloat("FORESTINT2")
    ISL_SIZE = cfg["PARAMETERS"].getfloat("ISL_SIZE")
    CONNECTIVITY = cfg["PARAMETERS"].getfloat("CONNECTIVITY")

    labels = [
        "DEM",
        "canopy",
        "FP",
        "SAT01",
        "SAT12",
        "SAT23",
        "SAT34",
        "AAT1",
        "AAT2",
        "AAT3",
        "TREE1",
        "TREE2",
        "TREE3",
        "ISL_SIZE",
        "WIN_SIZE",
    ]
    csvRow = [
        DEM,
        canopy,
        FP,
        SAT01,
        SAT12,
        SAT23,
        SAT34,
        AAT1,
        AAT2,
        AAT3,
        TREE1,
        TREE2,
        TREE3,
        ISL_SIZE,
        WIN_SIZE,
    ]
    csvfile = os.path.join(wd, "inputpara.csv")
    with open(csvfile, "a") as fp:
        wr = csv.writer(fp, dialect="excel")
        wr.writerow(labels)
        wr.writerow(csvRow)

    slope, profile = calculate_slope(DEM, wd)
    slope = slope.astype("int16")
    # Update metadata
    profile.update({"driver": "GTiff", "nodata": -9999, "dtype": "int16"})

    slope_nd = np.where(slope < 0, 0, slope)

    # Optional function to calculat class 4 slope using a neighborhood function - controlled by WIN_SIZE input parameter
    # If WIN_SIZE is set to 1 this function does not do anything to the SAT34 threshold calculation
    slope_smooth = scipy.ndimage.uniform_filter(slope_nd, size=WIN_SIZE, mode="nearest")

    # Reclassify
    slope[np.where((0 < slope) & (slope <= SAT01))] = 0
    slope[np.where((SAT01 < slope) & (slope <= SAT12))] = 1
    slope[np.where((SAT12 < slope) & (slope <= SAT23))] = 2
    slope[np.where((SAT23 < slope) & (slope <= 100))] = 3
    slope[np.where((SAT34 < slope_smooth) & (slope_smooth <= 100))] = 4

    dataUtils.saveRaster(
        pathlib.Path(DEM),
        pathlib.Path(wd, "slope.tif"),
        slope,
        nodata=-9999,
    )

    dataUtils.saveRaster(
        pathlib.Path(DEM),
        pathlib.Path(wd, "slope_smooth.tif"),
        slope_smooth,
        nodata=-9999,
    )

    # --- Open Flow-Py data, reclassify by thresholds and combine class 1, 2, and 3 runout zones into one raster

    # --- AAT1
    (array, profile) = dataUtils.readRaster(pathlib.Path(FP), return_profile=True)

    flow_py18 = array.copy()
    # flow_py18[np.where((flow_py18 >= AAT1) & (flow_py18 < 90))] = 1
    flow_py18[np.where((flow_py18 >= 0) & (flow_py18 < 90))] = (
        1  # Changed to 0 from AAT1 because we are not using Non-Avalanche Terrain - class 0
    )

    # --- AAT2
    flow_py25 = array.copy()
    flow_py25[np.where((flow_py25 < AAT2))] = 0
    flow_py25[np.where((flow_py25 >= AAT2) & (flow_py25 < 90))] = 2

    # --- AAT3
    flow_py38 = array.copy()
    flow_py38[np.where((flow_py38 < AAT3))] = 0
    flow_py38[np.where((flow_py38 >= AAT3) & (flow_py38 < 90))] = 3

    flowpy = np.maximum(flow_py18, flow_py25)
    flowpy = np.maximum(flowpy, flow_py38)

    # Update metadata

    dataUtils.saveRaster(
        pathlib.Path(DEM),
        pathlib.Path(wd, "flowpy.tif"),
        flowpy,
        dtype="int16",
        nodata=-9999,
    )

    # --- Combine Tree coverage, slope class and cell count

    src1 = slope.copy()
    src2 = flowpy.copy()

    ates = np.maximum(src1, src2)

    dataUtils.saveRaster(
        pathlib.Path(DEM),
        pathlib.Path(wd, "merge_new.tif"),
        ates,
        dtype="int16",
        nodata=-9999,
    )

    # --- Add tree coverage criteria

    src1 = ates.copy()

    # --- Reclassify using the forest criteria
    if canopy is None:
        print("No forest layer is provided, ATES classifier does not consider forest!")
        forest = np.zeros_like(ates)
    else:
        (forest, _) = dataUtils.readRaster(pathlib.Path(canopy), return_profile=True)
        forestModification = ""

        if TREE1 > 1 or TREE2 > 1 or TREE3 > 1:
            # check if forest values are between 0 and 100 to be consistent with the thresholds
            if np.all(forest <= 1):
                forestModification = "*"
                print("forest layer is multiplied by 100 for autoATES classiifer!")
                forest *= 100
        elif TREE1 < 1 and TREE2 < 1 and TREE3 < 1:
            if np.any(forest > 1):
                forestModification = "/"
                print("forest layer is mormalized for autoATES classiifer!")
                forest = forest / 100

    forest_open = forest.copy()
    forest_open[forest_open > TREE1] = -1
    forest_open[(forest_open >= 0) & (forest_open <= TREE1)] = 10

    forest_sparse = forest.copy()
    forest_sparse[forest_sparse > TREE2] = -1
    forest_sparse[forest <= TREE1] = -1
    forest_sparse[(forest > TREE1) & (forest <= TREE2)] = 20

    forest_dense = forest.copy()
    forest_dense[forest_dense > TREE3] = -1
    forest_dense[forest_dense <= TREE2] = -1
    forest_dense[(forest_dense > TREE2) & (forest_dense <= TREE3)] = 30

    forest_vdense = forest.copy()
    forest_vdense[forest_vdense < TREE3] = -1
    forest_vdense[forest_vdense >= TREE3] = 40

    src2 = np.maximum(forest_open, forest_sparse)
    src2 = np.maximum(src2, forest_dense)
    src2 = np.maximum(src2, forest_vdense)

    # no reclassification of forest in areas where there is FlowPy runout
    if cfg["FLAGS"].getboolean("forestReclassficationInRunout") is False:
        src2[flowpy > 0] = 10

    dataUtils.saveRaster(
        pathlib.Path(DEM),
        pathlib.Path(wd, "forest_reclass.tif"),
        src2,
        dtype="int16",
        nodata=-9999,
    )

    # --- Add PRA criteria
    (src3, _) = dataUtils.readRaster(pathlib.Path(SZ), return_profile=True)

    src3[np.where(0 >= src3)] = 0
    src3[np.where(0 < src3)] = 100

    dataUtils.saveRaster(
        pathlib.Path(DEM),
        pathlib.Path(wd, "SZ_reclass.tif"),
        src3,
        dtype="int16",
        nodata=-9999,
    )

    array = np.sum([src1, src2, src3], axis=0)

    array[np.where(array == 0)] = 1
    array[np.where(array == 10)] = 0
    array[np.where(array == 11)] = 1
    array[np.where(array == 12)] = 2
    array[np.where(array == 13)] = 3
    array[np.where(array == 14)] = 4
    array[np.where(array == 20)] = 0
    array[np.where(array == 21)] = 1
    # Changed from 22 - 1 to 22 - 2 to modify how sparse forest can alter output ATES class
    array[np.where(array == 22)] = 1
    array[np.where(array == 23)] = 2
    array[np.where(array == 24)] = 3
    array[np.where(array == 30)] = 0
    array[np.where(array == 31)] = 1
    array[np.where(array == 32)] = 1
    array[np.where(array == 33)] = 1
    array[np.where(array == 34)] = 3
    array[np.where(array == 40)] = 0
    array[np.where(array == 41)] = 1
    array[np.where(array == 42)] = 1
    array[np.where(array == 43)] = 1
    array[np.where(array == 44)] = 2

    # Added this section to convert areas where PRA is identified to
    array[np.where(array == 110)] = 0
    array[np.where(array == 111)] = 1
    array[np.where(array == 112)] = 2
    array[np.where(array == 113)] = 3
    array[np.where(array == 114)] = 4
    array[np.where(array == 120)] = 0
    array[np.where(array == 121)] = 1
    # Changed from 22 - 1 to 22 - 2 to modify how sparse forest can alter output ATES class
    array[np.where(array == 122)] = 1
    array[np.where(array == 123)] = 2
    array[np.where(array == 124)] = 3
    array[np.where(array == 130)] = 0
    array[np.where(array == 131)] = 1
    array[np.where(array == 132)] = 1
    array[np.where(array == 133)] = 2
    array[np.where(array == 134)] = 3
    array[np.where(array == 140)] = 0
    array[np.where(array == 141)] = 1
    array[np.where(array == 142)] = 1
    array[np.where(array == 143)] = 2
    array[np.where(array == 144)] = 2
    array[np.where(array < 0)] = -9999

    array = array.astype("int16")

    # --- Remove class 4 pixels in forested terrain
    # Added by JS 20210714
    if canopy is None:
        print("No forest layer is provided, ATES classifier does not consider forest!")
        src4 = np.zeros_like(ates)
    else:
        (src4, _) = dataUtils.readRaster(pathlib.Path(canopy), return_profile=True)
        if forestModification == "*":
            src4 *= 100
        elif forestModification == "/":
            src4 = src4 / 100

    # Converting forest mask raster so non-forest is 0 and forest is 1000
    src4[np.where(0 == src4)] = 0
    src4[np.where(0 < src4)] = 1000

    # Adding current ATES class to forest mask so 1004 are forested extreme pixels
    array = np.sum([array, src4], axis=0)

    # Down grading forested extreme pixels to complex
    array[np.where(array == 0)] = 0
    array[np.where(array == 1)] = 1
    array[np.where(array == 2)] = 2
    array[np.where(array == 3)] = 3
    array[np.where(array == 4)] = 4
    array[np.where(array == 1000)] = 0
    array[np.where(array == 1001)] = 1
    array[np.where(array == 1002)] = 2
    array[np.where(array == 1003)] = 3
    array[np.where(array == 1004)] = 3

    # PS: Add forest intraction layer (Flow-Py)
    if forest_int != "":
        (src5, _) = dataUtils.readRaster(pathlib.Path(forest_int), return_profile=True)
    else:
        src5 = np.zeros_like(array)

    src5[np.where(FORESTINT2 < src5)] = -1
    src5[np.where(FORESTINT1 < src5)] = 10000
    src5[np.where(src5 == -1)] = 20000
    src5[np.where(FORESTINT1 > src5)] = 0
    src5[np.where(FORESTINT1 == src5)] = 0

    array = np.sum([array, src5], axis=0)

    # for forest interaction between 2 and 8 (path hitted 2-8 cells):
    # with resolution of 10 m: avalanche ran through forest for 20 - 80 m
    # -> reclassify by one ATES class
    array[np.where(array == 10000)] = 0
    array[np.where(array == 10001)] = 1
    array[np.where(array == 10002)] = 2
    array[np.where(array == 10003)] = 2
    array[np.where(array == 10004)] = 3

    # for forest interaction more than 8 (path hitted more than 8 cells):
    # with resolution of 10 m: avalanche ran through forest for more than 80 m
    # -> reclassify all classes to class 1 (except class 4)
    # -> goal with this threshold is to reclassify all cells in the valley bottom to class 1
    array[np.where(array == 20000)] = 0
    array[np.where(array == 20001)] = 1
    array[np.where(array == 20002)] = 1
    array[np.where(array == 20003)] = 1
    array[np.where(array == 20004)] = 2

    ########### GLACIER ######################## start
    """
    assuming the following values in 'glacier'
        * 1 ... glacier
        * 0 ... no glacier or
        * positive values ... glacier
        * values <=0      ... no glacier
    and also assuming 'glacier' has the correct format
    i.e. ncols and nrows fit
    """
    """
    arrGlacier = rasterio.open(glacier).read() #read glacier.tif to numpy array
    assert arrGlacier.shape == array.shape   #check if the raster sizes align

    arrGlacier=np.where(arrGlacier>0,1,0) #convert to 0,1 if format is different

    array = np.where(arrGlacier==1, np.maximum(array,2) ,array) #glaciated areas have minimum of level 'challenging'
    """
    ############################################# ende

    array = array.astype("int16")

    # --- Save raster to path
    dataUtils.saveRaster(
        pathlib.Path(DEM),
        pathlib.Path(wd, "merge_all.tif"),
        array,
        dtype="int16",
        nodata=-9999,
    )

    # --- Remove clusters of raster cells smaller than ISL_SIZE

    raster = gdal.Open(DEM)
    gt = raster.GetGeoTransform()
    pixelSizeX = gt[1]
    pixelSizeY = -gt[5]
    num_cells = np.around(ISL_SIZE / (pixelSizeX * pixelSizeY))
    # print(num_cells)
    # --- Open file
    src1 = array.copy()

    # --- Change values to prepare for morphology and rasterio.fill
    src1 = src1 + 1
    src1 = src1.reshape(1, src1.shape[0], src1.shape[1])

    # --- Same as region group in arcmap. Each cluster gets a value between 1 and num_labels (number of clusters)
    # 20210430 JS changed connectivity to 2
    lab, num_labels = morphology.label(src1, connectivity=CONNECTIVITY, return_num=True)

    rg = np.arange(1, num_labels + 1, 1)

    # --- Loop through all clusters and assign all clusters with less then ISL_SIZE to the value 0 (set null)
    for i in rg:
        occurrences = np.count_nonzero(lab == i)
        if occurrences < num_cells:
            lab[np.where(lab == i)] = 0

    # --- Save as dtype int16
    lab = lab.astype("int16")

    # search_dist = num_cells / 4
    # search_dist = num_cells
    lab = (lab != 0).astype(np.uint8) * 255
    mask = np.where(lab > 0, 1, 0)

    # --- This algorithm will interpolate values for all designated nodata pixels (marked by zeros) (nibble)
    data = rasterio.fill.fillnodata(src1, mask, smoothing_iterations=0)

    # --- Change values back to standardized way of plotting ATES (0, 1, 2, 3 and 4)
    data = data - 1

    data[np.where(data == 0)] = -9999

    data = data.astype("int16")

    # --- Save raster to path
    dataUtils.saveRaster(
        pathlib.Path(DEM),
        pathlib.Path(wd, "ates_gen.tif"),
        data,
        dtype="int16",
        nodata=-9999,
    )


def calculate_slope(DEM, wd):
    """
    Calculate slope angle
    """
    demRaster, _ = dataUtils.readRaster(pathlib.Path(DEM), return_profile=False)
    gdal.DEMProcessing(f"{wd}/slope.tif", DEM, "slope")
    (slope, profile) = dataUtils.readRaster(
        pathlib.Path(wd, "slope.tif"), return_profile=True
    )
    return slope, profile


if __name__ == "__main__":
    autoATESClassifierMain()
