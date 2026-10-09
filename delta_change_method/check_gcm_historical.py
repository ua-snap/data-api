"""Check, for every Arctic-EDS report component, whether Rasdaman holds the inputs the delta
change method needs: the GCM historical runs (G_hist) and an observed/reanalysis baseline (B).

For coverages with a model axis over the historical period, every GCM slice is queried at each
test site and non-null values are counted. Coverages whose time axis starts in the future are
recorded as structural absences (0 of 0). The baseline column and status are curated from the
coverage metadata, the API code and the source documentation (see REPORT.md, Part A).

Writes data/gcm_historical_availability.csv.

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


# Status classes used in the report
APPLIED = "applied in production"  # delta method applied when the data were made
COMPUTABLE = "computable now"  # B and G_hist both in Rasdaman
NEEDS_GHIST = "needs G_hist ingest"
NO_BASELINE = "no observed baseline"
NA = "not applicable"
UNVERIFIED = "unverified"  # derived from downscaled inputs whose method isn't documented

# component -> coverage, band, GCM subset, historical subset (None = structurally absent),
#              what was checked, observed baseline in Rasdaman, status
CHECKS = {
    "temperature": (
        "tas_2km_projected_wcs", ".tas", None, None,
        "projected coverage starts in 2006; tas_2km_historical_wcs has no model axis",
        "CRU-TS 4.0 2km 1901-2015", APPLIED,
    ),
    "precipitation": (
        "annual_precip_totals_mm", "", "model(1:6),scenario(0:3)", f"year({T(1901)}:{T(2005)})",
        "GCM models, all scenarios, 1901-2005",
        "CRU-TS 4.0 2km 1901-2015", APPLIED,
    ),
    "snowfall": (
        "mean_annual_snowfall_mm", "", "model(1:5),scenario(0:3)", "decade(0:9)",
        "GCM models, all scenarios, 1910-2009 decades",
        "CRU-TS 3.1 771m 1910-2009", UNVERIFIED,
    ),
    "freezing_index": (
        "air_freezing_index_Fdays", "", "model(1:9),scenario(0:2)", "year(1980:2009)",
        "GCM models, all scenarios, 1980-2009",
        "Daymet 1980-2009", COMPUTABLE,
    ),
    "thawing_index": (
        "air_thawing_index_Fdays", "", "model(1:9),scenario(0:2)", "year(1980:2009)",
        "GCM models, all scenarios, 1980-2009",
        "Daymet 1980-2009", COMPUTABLE,
    ),
    "heating_degree_days": (
        "heating_degree_days_Fdays", "", "model(1:9),scenario(0:2)", "year(1980:2009)",
        "GCM models, all scenarios, 1980-2009",
        "Daymet 1980-2009", COMPUTABLE,
    ),
    "wet_days_per_year": (
        "wet_days_per_year", "", "model(1:2)", f"year({T(1980)}:{T(2005)})",
        "GCM models, 1980-2005",
        "ERA-Interim WRF 1980-2009", NEEDS_GHIST,
    ),
    "hydrology": (
        "hydrology", ".runoff", "model(0:9),scenario(0:1),month(0:11)", "era(0:5)",
        "all 10 GCMs, both RCPs, all months, decades 1950-2009 (runoff band)",
        "none (no Daymet-driven VIC run)", NO_BASELINE,
    ),
    "permafrost": (
        "crrel_gipl_outputs_nc", "", None, None,
        "time axis starts in 2021 (GIPL 2021-2120 only)",
        "none", NO_BASELINE,
    ),
    "precip_frequency": (
        "dot_precip", "", None, None,
        "era axis holds 2020-2049, 2050-2079, 2080-2099 only",
        "none (NOAA Atlas 14 not in Rasdaman)", APPLIED,
    ),
    "elevation": (
        None, "", None, None, "static terrain layer", "n/a", NA,
    ),
}


def count_values(cov, band, gcm_sel, hist_sel, x, y):
    v = wcps(
        f"for $c in ({cov}) return encode($c{band}[{gcm_sel},{hist_sel},X({x}),Y({y})], "
        '"application/json")'
    )
    a = np.array(v, dtype=float)
    return int(np.sum(~np.isnan(a) & (a > -9000))), int(a.size)


if __name__ == "__main__":
    rows = []
    for comp, (cov, band, gcm_sel, hist_sel, desc, baseline, status) in CHECKS.items():
        found = total = 0
        if cov and gcm_sel:
            for name, _, lat, lon in SITES:
                x, y = to_3338(lat, lon)
                n, t = count_values(cov, band, gcm_sel, hist_sel, x, y)
                found += n
                total += t
        rows.append(
            dict(
                component=comp,
                coverage=cov or "",
                checked=desc,
                gcm_historical_values_found=found,
                values_checked=total,
                observed_baseline_in_rasdaman=baseline,
                status=status,
            )
        )
        print(f"{comp:20s} {cov or '-':28s} found {found}/{total}  baseline: {baseline:35s} -> {status}")
    pd.DataFrame(rows).to_csv(DATA / "gcm_historical_availability.csv", index=False)
