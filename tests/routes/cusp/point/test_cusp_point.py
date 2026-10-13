import csv
from datetime import datetime
import io
import json
from pathlib import Path

from flask.testing import FlaskClient

from tests.json_compare import assert_json_allclose


FIXTURE_DIRECTORY = Path(__file__).parent / "json"


def test_cusp_point_fairbanks(client: FlaskClient) -> None:
    """Compare Fairbanks observations with a captured live GeoJSON response."""
    response = client.get("/cusp/point/64.8378/-147.7164")
    assert response.status_code == 200
    actual_data = response.get_json()

    with (FIXTURE_DIRECTORY / "cusp_point_fairbanks.json").open(
        encoding="utf-8"
    ) as fixture_file:
        expected_data = json.load(fixture_file)

    # GeoServer's request timestamp changes even when observations are unchanged.
    datetime.fromisoformat(actual_data.pop("timeStamp"))
    datetime.fromisoformat(expected_data.pop("timeStamp"))
    assert_json_allclose(actual_data, expected_data)


def test_cusp_point_ocean(client: FlaskClient) -> None:
    """Return no data for the standard Bering Strait ocean point."""
    response = client.get("/cusp/point/66.95/-165")
    assert response.status_code == 404


def test_cusp_point_attu(client: FlaskClient) -> None:
    """Return no data near Attu's positive-longitude coordinates."""
    response = client.get("/cusp/point/52.8339/173.1794")
    assert response.status_code == 404


def test_cusp_point_dawson_city(client: FlaskClient) -> None:
    """Return no data near Dawson City, Yukon."""
    response = client.get("/cusp/point/64.0625/-139.431")
    assert response.status_code == 404


def test_cusp_point_csv(client: FlaskClient) -> None:
    """Return a parseable CSV export of the Fairbanks observations."""
    response = client.get("/cusp/point/64.8378/-147.7164?format=csv")
    assert response.status_code == 200
    rows = list(csv.reader(io.StringIO(response.get_data(as_text=True))))
    assert len(rows) > 0
