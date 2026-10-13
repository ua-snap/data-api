import csv
import io

from flask.testing import FlaskClient


def test_cusp_point_fairbanks(client: FlaskClient) -> None:
    """Return parseable JSON for Fairbanks observations."""
    response = client.get("/cusp/point/64.8378/-147.7164")
    assert response.status_code == 200
    assert response.get_json(silent=True) is not None


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
