"""Check which EDS coverages hold GCM historical runs (the G_hist the delta change method needs).

For each coverage, query every GCM slice over the historical years at each test site and
count non-null values. Writes data/gcm_historical_availability.csv.

Usage: python check_gcm_historical.py
"""

from pathlib import Path

import numpy as np
import pandas as pd

from sites import SITES
from wcps import to_3338, wcps

DATA = Path(__file__).parent / "data"


def T(year):
    return f'"{year}-01-01T00:00:00.000Z"'


# coverage -> (component, GCM subset, historical subset, description of that subset)
CHECKS = {
    "tas_2km_projected_wcs": (
        "temperature",
        "model(0:2),scenario(0:2)",
        "year(0:94)",
        "only years 2006-2100 exist; the historical coverage (tas_2km_historical_wcs) has no model axis",
    ),
    "annual_precip_totals_mm": (
        "precipitation",
        "model(1:6),scenario(0:3)",
        f"year({T(1901)}:{T(2005)})",
        "GCM models, all scenarios, 1901-2005",
    ),
    "mean_annual_snowfall_mm": (
        "snowfall",
        "model(1:5),scenario(0:3)",
        "decade(0:9)",
        "GCM models, all scenarios, 1910-2009 decades",
    ),
    "wet_days_per_year": (
        "wet_days_per_year",
        "model(1:2)",
        f"year({T(1980)}:{T(2005)})",
        "GCM models, 1980-2005",
    ),
    "air_freezing_index_Fdays": (
        "freezing_index",
        "model(1:9),scenario(0:2)",
        "year(1980:2009)",
        "GCM models, all scenarios, 1980-2009",
    ),
    "air_thawing_index_Fdays": (
        "thawing_index",
        "model(1:9),scenario(0:2)",
        "year(1980:2009)",
        "GCM models, all scenarios, 1980-2009",
    ),
    "heating_degree_days_Fdays": (
        "heating_degree_days",
        "model(1:9),scenario(0:2)",
        "year(1980:2009)",
        "GCM models, all scenarios, 1980-2009",
    ),
}


def count_values(cov, gcm_sel, hist_sel, x, y, band=""):
    v = wcps(
        f"for $c in ({cov}) return encode($c{band}[{gcm_sel},{hist_sel},X({x}),Y({y})], "
        '"application/json")'
    )
    a = np.array(v, dtype=float)
    return int(np.sum(~np.isnan(a) & (a > -9000))), int(a.size)


if __name__ == "__main__":
    rows = []
    for cov, (comp, gcm_sel, hist_sel, desc) in CHECKS.items():
        band = ".tas" if cov.startswith("tas_2km") else ""
        found = total = 0
        for name, _, lat, lon in SITES:
            x, y = to_3338(lat, lon)
            if cov == "tas_2km_projected_wcs":
                # there are no pre-2006 years to check: record what the year axis holds
                n, t = 0, 0
            else:
                n, t = count_values(cov, gcm_sel, hist_sel, x, y, band)
            found += n
            total += t
        rows.append(
            dict(
                component=comp,
                coverage=cov,
                checked=desc,
                gcm_historical_values_found=found,
                values_checked=total,
                delta_change_possible=found > 0,
            )
        )
        print(f"{comp:20s} {cov:28s} found {found}/{total}")
    pd.DataFrame(rows).to_csv(DATA / "gcm_historical_availability.csv", index=False)
