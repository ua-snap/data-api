import pytest
from postprocessing import RATIO_CAP
from routes.arctic_hydrology import (
    calculate_and_apply_gcm_diffs_to_blaskey_climatology,
)


def _arctic_data_dict():
    """
    Minimal one-day fixture mirroring production values at stream 81009008,
    doy 119 (the observed 602,159.949 cfs doy_min blowup).
    """
    day = {"doy": 119, "water_year_index": 211}
    return {
        "historical": {
            "1990-2021": [
                dict(day, doy_min=1.223, doy_mean=3152.662, doy_max=10496.285)
            ],
        },
        "C2LE2": {
            "1990-2021": [
                dict(day, doy_min=0.001, doy_mean=3964.098, doy_max=11532.644)
            ],
            "2034-2065": [
                dict(day, doy_min=492.363, doy_mean=3980.97, doy_max=9345.952)
            ],
        },
        # PGW model with no historical era: must be skipped, not crash
        "PGWh": {
            "2034-2065": [
                dict(day, doy_min=72.505, doy_mean=4257.796, doy_max=11770.358)
            ],
        },
    }


def test_blaskey_adjustment_min_stays_bounded():
    adjusted = calculate_and_apply_gcm_diffs_to_blaskey_climatology(
        _arctic_data_dict()
    )
    entry = adjusted["C2LE2"]["2034-2065"][0]
    assert entry["doy_min"] <= 1.223 * RATIO_CAP
    assert entry["doy_min"] < 602159.949


def test_blaskey_adjustment_ordinary_stats_close_to_plain_ratio():
    adjusted = calculate_and_apply_gcm_diffs_to_blaskey_climatology(
        _arctic_data_dict()
    )
    entry = adjusted["C2LE2"]["2034-2065"][0]
    plain_mean = 3152.662 * (3980.97 / 3964.098)
    assert entry["doy_mean"] == pytest.approx(plain_mean, rel=0.05)


def test_blaskey_adjustment_skips_pgw_and_keeps_historical():
    adjusted = calculate_and_apply_gcm_diffs_to_blaskey_climatology(
        _arctic_data_dict()
    )
    assert "PGWh" not in adjusted
    assert adjusted["historical"]["1990-2021"][0]["doy_min"] == 1.223
    assert "1990-2021" not in adjusted["C2LE2"]


def _assert_ordered(entry):
    assert entry["doy_min"] <= entry["doy_mean"] <= entry["doy_max"]


def test_blaskey_adjustment_enforces_stat_ordering():
    """
    Each stat is scaled by its own ratio, so a large projected minimum can
    overtake the adjusted mean. The clamp must restore ordering.
    """
    day = {"doy": 200, "water_year_index": 292}
    data = {
        "historical": {
            "1990-2021": [dict(day, doy_min=100.0, doy_mean=200.0, doy_max=1000.0)],
        },
        "MODEL": {
            "1990-2021": [dict(day, doy_min=100.0, doy_mean=200.0, doy_max=4000.0)],
            "2034-2065": [dict(day, doy_min=400.0, doy_mean=200.0, doy_max=1200.0)],
        },
    }
    adjusted = calculate_and_apply_gcm_diffs_to_blaskey_climatology(data)
    _assert_ordered(adjusted["MODEL"]["2034-2065"][0])
