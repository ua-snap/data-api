"""Pull the raw point time series behind each Arctic-EDS component for every test site.

Each coverage is sliced at the site with a single WCPS query (the same X/Y slicing the
API does), and the full model x scenario x time cube is cached to data/raw/<site>.json.
All delta-change math is done later in analyze.py; see wcps_server_side.py for the
server-side version.

Usage: python fetch_site_data.py
"""

import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from sites import SITES
from wcps import to_3338, wcps

OUT = Path(__file__).parent / "data" / "raw"
OUT.mkdir(parents=True, exist_ok=True)

# coverage id -> band subset expression (None means the coverage's only band)
COVERAGES = {
    # Temperature plate: CRU-TS 4.0 2km (month x year) and AR5 2km (model x scenario x month x year)
    "tas_2km_historical_wcs": ".tas",
    "tas_2km_projected_wcs": ".tas",
    # Precipitation plate: model x scenario x year annual totals
    "annual_precip_totals_mm": None,
    # Snowfall plate: model x scenario x decade
    "mean_annual_snowfall_mm": None,
    # Degree day plates: model x scenario x year (NCAR 12km / Daymet)
    "air_freezing_index_Fdays": None,
    "air_thawing_index_Fdays": None,
    "heating_degree_days_Fdays": None,
    # Wet days per year: model x year (WRF 20km)
    "wet_days_per_year": None,
}


def fetch_site(site):
    name, region, lat, lon = site
    x, y = to_3338(lat, lon)
    out = {"name": name, "region": region, "lat": lat, "lon": lon, "x": x, "y": y}
    for cov, band in COVERAGES.items():
        sel = f"$c{band or ''}"
        query = (
            f"for $c in ({cov}) "
            f'return encode({sel}[X({x}),Y({y})], "application/json")'
        )
        out[cov] = wcps(query)
    fp = OUT / f"{name.replace(' ', '_')}.json"
    fp.write_text(json.dumps(out))
    return name


if __name__ == "__main__":
    with ThreadPoolExecutor(max_workers=4) as pool:
        for name in pool.map(fetch_site, SITES):
            print("fetched", name)
