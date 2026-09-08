import csv
import io
import json


def test_arctic_hydrology_stream_stats(client):
    """Tests /arctic_hydrology/stats/<stream_id> for stream 81014458."""
    response = client.get("/arctic_hydrology/stats/81014458")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/arctic_hydrology/stream/json/arctic_hydrology_stream_stats.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_arctic_hydrology_stream_modeled_climatology(client):
    """Tests /arctic_hydrology/modeled_climatology/<stream_id> for stream 81014458."""
    response = client.get("/arctic_hydrology/modeled_climatology/81014458")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/arctic_hydrology/stream/json/arctic_hydrology_stream_modeled_climatology.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_arctic_hydrology_stream_wt_stats(client):
    """Tests /arctic_hydrology/wt_stats/<stream_id> for stream 81014458."""
    response = client.get("/arctic_hydrology/wt_stats/81014458")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/arctic_hydrology/stream/json/arctic_hydrology_stream_wt_stats.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_arctic_hydrology_stream_wt_modeled_climatology(client):
    """Tests /arctic_hydrology/wt_modeled_climatology/<stream_id> for stream 81014458."""
    response = client.get("/arctic_hydrology/wt_modeled_climatology/81014458")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/arctic_hydrology/stream/json/arctic_hydrology_stream_wt_modeled_climatology.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_arctic_hydrology_stream_hydroviz(client):
    """Tests /arctic_hydrology/hydroviz/<stream_id> for stream 81014458."""
    response = client.get("/arctic_hydrology/hydroviz/81014458")
    assert response.status_code == 200
    actual_data = response.get_json()

    with open("tests/routes/arctic_hydrology/stream/json/arctic_hydrology_stream_hydroviz.json") as f:
        expected_data = json.load(f)

    assert actual_data == expected_data


def test_arctic_hydrology_stream_stats_source(client):
    """Tests /arctic_hydrology/stats/<stream_id>?source= for stream 81014458 returns a parseable response."""
    response = client.get("/arctic_hydrology/stats/81014458?source=original_gcm")
    assert response.status_code == 200
    assert response.get_json() is not None


def test_arctic_hydrology_stream_modeled_climatology_source(client):
    """Tests /arctic_hydrology/modeled_climatology/<stream_id>?source= for stream 81014458 returns a parseable response."""
    response = client.get("/arctic_hydrology/modeled_climatology/81014458?source=original_gcm")
    assert response.status_code == 200
    assert response.get_json() is not None


def test_arctic_hydrology_stream_wt_stats_source(client):
    """Tests /arctic_hydrology/wt_stats/<stream_id>?source= for stream 81014458 returns a parseable response."""
    response = client.get("/arctic_hydrology/wt_stats/81014458?source=original_gcm")
    assert response.status_code == 200
    assert response.get_json() is not None


def test_arctic_hydrology_stream_wt_modeled_climatology_source(client):
    """Tests /arctic_hydrology/wt_modeled_climatology/<stream_id>?source= for stream 81014458 returns a parseable response."""
    response = client.get("/arctic_hydrology/wt_modeled_climatology/81014458?source=original_gcm")
    assert response.status_code == 200
    assert response.get_json() is not None


def test_arctic_hydrology_stream_stats_csv(client):
    """Tests /arctic_hydrology/stats/<stream_id>?format=csv for stream 81014458 returns a parseable CSV."""
    response = client.get("/arctic_hydrology/stats/81014458?format=csv")
    assert response.status_code == 200
    rows = list(csv.reader(io.StringIO(response.get_data(as_text=True))))
    assert len(rows) > 0


def test_arctic_hydrology_stream_modeled_climatology_csv(client):
    """Tests /arctic_hydrology/modeled_climatology/<stream_id>?format=csv for stream 81014458 returns a parseable CSV."""
    response = client.get("/arctic_hydrology/modeled_climatology/81014458?format=csv")
    assert response.status_code == 200
    rows = list(csv.reader(io.StringIO(response.get_data(as_text=True))))
    assert len(rows) > 0


def test_arctic_hydrology_stream_wt_stats_csv(client):
    """Tests /arctic_hydrology/wt_stats/<stream_id>?format=csv for stream 81014458 returns a parseable CSV."""
    response = client.get("/arctic_hydrology/wt_stats/81014458?format=csv")
    assert response.status_code == 200
    rows = list(csv.reader(io.StringIO(response.get_data(as_text=True))))
    assert len(rows) > 0


def test_arctic_hydrology_stream_wt_modeled_climatology_csv(client):
    """Tests /arctic_hydrology/wt_modeled_climatology/<stream_id>?format=csv for stream 81014458 returns a parseable CSV."""
    response = client.get("/arctic_hydrology/wt_modeled_climatology/81014458?format=csv")
    assert response.status_code == 200
    rows = list(csv.reader(io.StringIO(response.get_data(as_text=True))))
    assert len(rows) > 0
