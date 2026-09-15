import pytest
from routes.conus_hydrology import (
    calculate_and_apply_gcm_diffs_to_maurer_climatology,
)


def _conus_data_dict():
    """One-day fixture in the CONUS shape (model -> scenario -> era)."""
    day = {"doy": 119, "water_year_index": 211}
    return {
        "Maurer": {
            "historical": {
                "1976-2005": [
                    dict(day, doy_min=296.9, doy_mean=708.793, doy_max=1373.0)
                ],
            },
        },
        "CCSM4": {
            "historical": {
                "1976-2005": [
                    dict(day, doy_min=286.1, doy_mean=966.628, doy_max=4546.0)
                ],
            },
            "rcp45": {
                "2016-2045": [
                    dict(day, doy_min=0.002, doy_mean=900.0, doy_max=4000.0)
                ],
            },
        },
    }


def test_maurer_adjustment_near_zero_projected_min_stays_bounded():
    from postprocessing import RATIO_CAP

    adjusted = calculate_and_apply_gcm_diffs_to_maurer_climatology(_conus_data_dict())
    entry = adjusted["CCSM4"]["rcp45"]["2016-2045"][0]
    # projected min ~0 against healthy historical min: clamped at 1/RATIO_CAP
    assert entry["doy_min"] >= 296.9 / RATIO_CAP - 0.001
    assert entry["doy_min"] <= 296.9


def test_maurer_adjustment_preserves_structure():
    adjusted = calculate_and_apply_gcm_diffs_to_maurer_climatology(_conus_data_dict())
    assert adjusted["Maurer"]["historical"]["1976-2005"][0]["doy_mean"] == 708.793
    assert "rcp45" in adjusted["CCSM4"]


def _assert_ordered(entry):
    assert entry["doy_min"] <= entry["doy_mean"] <= entry["doy_max"]


def test_maurer_adjustment_enforces_stat_ordering():
    """
    Production values from CONUS stream 50563 (dynamic / CCSM4 / rcp26,
    doy 214), which yielded adjusted doy_min 353.042 > doy_max 343.703
    because the GCM historical max held a flood day that does not recur.
    """
    day = {"doy": 214, "water_year_index": 306}
    data = {
        "Maurer": {
            "historical": {
                "1976-2005": [
                    dict(day, doy_min=296.9, doy_mean=708.793, doy_max=1373.0)
                ],
            },
        },
        "CCSM4": {
            "historical": {
                "1976-2005": [
                    dict(day, doy_min=286.1, doy_mean=966.628, doy_max=4546.0)
                ],
            },
            "rcp26": {
                "2016-2045": [
                    dict(day, doy_min=340.2, doy_mean=627.062, doy_max=1138.0)
                ],
            },
        },
    }
    adjusted = calculate_and_apply_gcm_diffs_to_maurer_climatology(data)
    _assert_ordered(adjusted["CCSM4"]["rcp26"]["2016-2045"][0])
