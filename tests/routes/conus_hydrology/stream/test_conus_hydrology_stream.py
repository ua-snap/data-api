import csv
import io
import json


def test_conus_hydrology_stream_stats(client):
    """Tests /conus_hydrology/stats/<stream_id> for stream 50101 (Columbia River)."""
    response = client.get("/conus_hydrology/stats/50101")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/conus_hydrology/stream/json/conus_hydrology_stream_stats.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_conus_hydrology_stream_modeled_climatology(client):
    """Tests /conus_hydrology/modeled_climatology/<stream_id> for stream 50101 (Columbia River)."""
    response = client.get("/conus_hydrology/modeled_climatology/50101")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/conus_hydrology/stream/json/conus_hydrology_stream_modeled_climatology.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_conus_hydrology_stream_observed_climatology(client):
    """Tests /conus_hydrology/observed_climatology/<stream_id> for stream 50101; this segment has no associated USGS gage, so nodata is expected."""
    response = client.get("/conus_hydrology/observed_climatology/50101")
    assert response.status_code == 404


def test_conus_hydrology_stream_gage_info(client):
    """Tests /conus_hydrology/gage_info, a flat list endpoint that takes no stream ID."""
    response = client.get("/conus_hydrology/gage_info")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/conus_hydrology/stream/json/conus_hydrology_stream_gage_info.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_conus_hydrology_stream_hydroviz(client):
    """Tests /conus_hydrology/hydroviz/<stream_id> for stream 50101 (Columbia River)."""
    response = client.get("/conus_hydrology/hydroviz/50101")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/conus_hydrology/stream/json/conus_hydrology_stream_hydroviz.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_conus_hydrology_stream_stats_source(client):
    """Tests /conus_hydrology/stats/<stream_id>?source= for stream 50101 returns a parseable response."""
    response = client.get("/conus_hydrology/stats/50101?source=original_gcm")
    assert response.status_code == 200
    assert response.get_json() is not None


def test_conus_hydrology_stream_modeled_climatology_source(client):
    """Tests /conus_hydrology/modeled_climatology/<stream_id>?source= for stream 50101 returns a parseable response."""
    response = client.get("/conus_hydrology/modeled_climatology/50101?source=original_gcm")
    assert response.status_code == 200
    assert response.get_json() is not None


def test_conus_hydrology_stream_stats_csv(client):
    """Tests /conus_hydrology/stats/<stream_id>?format=csv for stream 50101 returns a parseable CSV."""
    response = client.get("/conus_hydrology/stats/50101?format=csv")
    assert response.status_code == 200
    rows = list(csv.reader(io.StringIO(response.get_data(as_text=True))))
    assert len(rows) > 0


def test_conus_hydrology_stream_modeled_climatology_csv(client):
    """Tests /conus_hydrology/modeled_climatology/<stream_id>?format=csv for stream 50101 returns a parseable CSV."""
    response = client.get("/conus_hydrology/modeled_climatology/50101?format=csv")
    assert response.status_code == 200
    rows = list(csv.reader(io.StringIO(response.get_data(as_text=True))))
    assert len(rows) > 0


# Note: /conus_hydrology/observed_climatology/50101 is NOT covered by a CSV smoke test here, since
# this stream segment has no associated USGS gage and already returns 404 without ?format=csv.
