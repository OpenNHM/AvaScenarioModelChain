import configparser

from ati.mod1Release.praAssignElevSize import loadElevationBands


def test_load_elevation_bands_uses_selection_range_when_config_is_empty():
    cfg = configparser.ConfigParser()
    cfg.read_dict(
        {
            "praASSIGNELEV": {
                "elevationBand1": "",
                "elevationBand2": "",
                "elevationBand3": "",
                "elevationBand4": "",
                "elevationBand5": "",
            },
            "praSELECTION": {"minElev": "350", "maxElev": "4000"},
        }
    )

    bands = loadElevationBands(cfg)

    assert bands == [("0350-4000", (350.0, 4000.0))]
