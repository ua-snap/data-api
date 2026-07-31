import pytest
from postprocessing import (
    scale_aware_epsilon,
    stabilized_ratio,
    RATIO_CAP,
    RATIO_EPSILON_FLOOR,
    RATIO_EPSILON_FRACTION,
)
from routes.arctic_hydrology import (
    calculate_and_apply_gcm_diffs_to_blaskey_climatology,
)
from routes.conus_hydrology import (
    calculate_and_apply_gcm_diffs_to_maurer_climatology,
)


##############################
# 1. stabilized_ratio helper #
##############################


def test_stabilized_ratio_near_zero_baseline_does_not_explode():
    """
    Reproduces the production blowup at arctic stream 81009008, doy 119:
    historical min 0.001 cfs vs projected min 492.363 cfs produced a raw
    ratio of ~492,363x. With a scale-aware epsilon the factor must stay
    within the cap.
    """
    epsilon = scale_aware_epsilon([600.0] * 366)  # ~600 cfs mean flow stream
    ratio = stabilized_ratio(492.363, 0.001, epsilon)
    assert ratio <= RATIO_CAP


def test_stabilized_ratio_zero_denominator_no_error():
    epsilon = scale_aware_epsilon([1.0])
    assert stabilized_ratio(5.0, 0.0, epsilon) <= RATIO_CAP


def test_stabilized_ratio_both_near_zero_approaches_one():
    epsilon = scale_aware_epsilon([100.0])
    ratio = stabilized_ratio(0.001, 0.002, epsilon)
    assert 0.9 < ratio < 1.1


def test_stabilized_ratio_ordinary_values_unchanged():
    """Healthy baselines should give (nearly) the plain ratio."""
    epsilon = scale_aware_epsilon([100.0])
    ratio = stabilized_ratio(200.0, 100.0, epsilon)
    assert ratio == pytest.approx(2.0, rel=0.02)


def test_stabilized_ratio_clamps_low_end():
    epsilon = scale_aware_epsilon([100.0])
    assert stabilized_ratio(0.001, 5000.0, epsilon) == pytest.approx(1.0 / RATIO_CAP)


def test_scale_aware_epsilon_scales_with_flow():
    small = scale_aware_epsilon([1.0] * 10)
    large = scale_aware_epsilon([10000.0] * 10)
    assert small == pytest.approx(RATIO_EPSILON_FRACTION * 1.0)
    assert large == pytest.approx(RATIO_EPSILON_FRACTION * 10000.0)


def test_scale_aware_epsilon_floor():
    assert scale_aware_epsilon([]) == RATIO_EPSILON_FLOOR
    assert scale_aware_epsilon([0.0, 0.0]) == RATIO_EPSILON_FLOOR


###########################################
# 2. Arctic Blaskey climatology adjustment #
###########################################


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
    # blaskey min (1.223) can be scaled at most by RATIO_CAP
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


##########################################
# 3. CONUS Maurer climatology adjustment #
##########################################


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
    adjusted = calculate_and_apply_gcm_diffs_to_maurer_climatology(_conus_data_dict())
    entry = adjusted["CCSM4"]["rcp45"]["2016-2045"][0]
    # projected min ~0 against healthy historical min: clamped at 1/RATIO_CAP
    assert entry["doy_min"] >= 296.9 / RATIO_CAP - 0.001
    assert entry["doy_min"] <= 296.9


def test_maurer_adjustment_preserves_structure():
    adjusted = calculate_and_apply_gcm_diffs_to_maurer_climatology(_conus_data_dict())
    assert adjusted["Maurer"]["historical"]["1976-2005"][0]["doy_mean"] == 708.793
    assert "rcp45" in adjusted["CCSM4"]


####################################
# 4. min <= mean <= max invariant  #
####################################


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
