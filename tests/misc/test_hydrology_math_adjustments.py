import pytest
from postprocessing import (
    scale_aware_epsilon,
    stabilized_ratio,
    RATIO_CAP,
    RATIO_EPSILON_FLOOR,
    RATIO_EPSILON_FRACTION,
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
    epsilon = scale_aware_epsilon([600.0] * 366)
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
