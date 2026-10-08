"""Minimal helpers for querying SNAP Rasdaman via WCPS from the analysis scripts.

These intentionally avoid importing the Flask app so they can run standalone.
"""

import json
from urllib.parse import quote

import requests
from pyproj import Transformer

RAS_BASE_URL = "https://zeus.snap.uaf.edu/rasdaman/ows"

_to_3338 = Transformer.from_crs(4326, 3338, always_xy=True)


def to_3338(lat, lon):
    """Return (x, y) in EPSG:3338 for a WGS84 lat/lon."""
    return _to_3338.transform(lon, lat)


def wcps(query, timeout=120):
    """Run a WCPS query and return the decoded JSON (or raw text if not JSON)."""
    url = (
        f"{RAS_BASE_URL}?SERVICE=WCS&VERSION=2.0.1&REQUEST=ProcessCoverages"
        f"&query={quote(query)}"
    )
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    try:
        return json.loads(r.text)
    except json.JSONDecodeError:
        return r.text.strip()


def wcps_url(query):
    """Return the full GET URL for a WCPS query (handy for documenting in the report)."""
    return (
        f"{RAS_BASE_URL}?SERVICE=WCS&VERSION=2.0.1&REQUEST=ProcessCoverages"
        f"&query={quote(query)}"
    )
