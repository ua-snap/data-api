import numpy as np


def assert_json_allclose(actual, expected, path="root"):
    """Recursively assert actual == expected, comparing numbers with numpy.isclose."""
    if isinstance(expected, dict):
        assert isinstance(actual, dict), f"{path}: expected dict, got {type(actual)}"
        assert actual.keys() == expected.keys(), f"{path}: key mismatch"
        for key in expected:
            assert_json_allclose(actual[key], expected[key], f"{path}.{key}")
    elif isinstance(expected, list):
        assert isinstance(actual, list), f"{path}: expected list, got {type(actual)}"
        assert len(actual) == len(expected), f"{path}: length mismatch"
        for i, (a, e) in enumerate(zip(actual, expected)):
            assert_json_allclose(a, e, f"{path}[{i}]")
    elif isinstance(expected, bool) or expected is None:
        assert actual == expected, f"{path}: {actual!r} != {expected!r}"
    elif isinstance(expected, (int, float)):
        assert isinstance(actual, (int, float)), f"{path}: expected number, got {type(actual)}"
        magnitude = abs(expected)
        rtol, atol = (2, 0) if magnitude < 1 else (0.01, 0)
        assert np.isclose(actual, expected, rtol=rtol, atol=atol), f"{path}: {actual!r} != {expected!r}"
    else:
        assert actual == expected, f"{path}: {actual!r} != {expected!r}"
