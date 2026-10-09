"""Compare the PRISM 1961-1990 2km climatology with the CRU-TS 2km 1961-1990 means at the test sites.

ar5_baseline.py originally stood in for PRISM 1961-1990 (the climatology the AR5 deltas
were added to) with the 1961-1990 mean of the CRU-TS 4.0 2km historical coverage, on the
assumption that the CRU product was delta-downscaled onto the same PRISM grids. This
checks that assumption against the actual PRISM files.

PRISM source: SNAP GeoNetwork record 0e8e42f7-6774-4d35-a7b3-4a82f8b48e00
("PRISM 1961-1990 Climatologies", 2km, EPSG:3338). Monthly GeoTIFFs named
<var>/<var>_..._akcan_prism_<MM>_1961_1990.tif.

Writes data/prism_vs_cru_1961_1990.csv.

Usage: python prism_check.py /path/to/prism
"""

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import rasterio as rio

from sites import SITES
from wcps import to_3338

HERE = Path(__file__).parent
RAW = HERE / "data" / "raw"
DATA = HERE / "data"
MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


# annual_precip_totals_mm was gdalwarped onto a slightly different grid at ingest
# (corner origin and pixel size from its DescribeCoverage). tas_2km_* share the PRISM grid.
PR_GRID = (-2268260.438997154, 2475553.746647252, 2000.088, 2000.79)


def precip_cell_center(x, y):
    """Center of the annual_precip_totals_mm cell containing (x, y): where the
    nearest-neighbour warp took that cell's value from."""
    x0, y0, dx, dy = PR_GRID
    i, j = (x - x0) // dx, (y0 - y) // dy
    return x0 + (i + 0.5) * dx, y0 - (j + 0.5) * dy


def site_points(var):
    pts = {}
    for name, _, lat, lon in SITES:
        x, y = to_3338(lat, lon)
        pts[name] = precip_cell_center(x, y) if var == "pr" else (x, y)
    return pts


def sample_prism(prism_dir, var, pattern):
    """Return {site: [12 monthly values]} from the PRISM cell matching the Rasdaman cell
    the API reads for that site."""
    pts = site_points(var)
    out = {name: [] for name in pts}
    for m in range(1, 13):
        fp = prism_dir / var / pattern.format(m=f"{m:02d}")
        with rio.open(fp) as src:
            arr = src.read(1, masked=True)
            for name, (x, y) in pts.items():
                row, col = src.index(x, y)
                v = arr[row, col]
                out[name].append(np.nan if np.ma.is_masked(v) else float(v))
    return out


def load_prism(prism_dir):
    """Monthly PRISM 1961-1990 tas (°C) and pr (mm) at each site."""
    tas = sample_prism(prism_dir, "tas", "tas_mean_C_akcan_prism_{m}_1961_1990.tif")
    pr = sample_prism(prism_dir, "pr", "pr_total_mm_akcan_prism_{m}_1961_1990.tif")
    return tas, pr


def main(prism_dir):
    tas, pr = load_prism(prism_dir)
    rows = []
    for name, region, *_ in SITES:
        site = json.loads((RAW / f"{name.replace(' ', '_')}.json").read_text())
        cru_tas = np.array(site["tas_2km_historical_wcs"], dtype=float)  # month x year 1901-2015
        cru_pr = np.array(site["annual_precip_totals_mm"], dtype=float)  # model x year x scen
        cru_tas_monthly = np.nanmean(cru_tas[:, 60:90], axis=1)
        for i, mon in enumerate(MONTHS):
            rows.append(dict(site=name, region=region, variable="tas", period=mon,
                             prism=tas[name][i], cru_2km_1961_1990=cru_tas_monthly[i]))
        rows.append(dict(site=name, region=region, variable="tas", period="Annual",
                         prism=np.mean(tas[name]), cru_2km_1961_1990=np.nanmean(cru_tas_monthly)))
        rows.append(dict(site=name, region=region, variable="pr", period="Annual",
                         prism=np.sum(pr[name]), cru_2km_1961_1990=np.nanmean(cru_pr[0, 60:90, 0])))
    df = pd.DataFrame(rows)
    df["diff"] = df.cru_2km_1961_1990 - df.prism
    df["pct_diff"] = 100 * df["diff"] / df.prism
    df.to_csv(DATA / "prism_vs_cru_1961_1990.csv", index=False)

    pd.set_option("display.width", 200)
    ann = df[df.period == "Annual"]
    print(ann.pivot(index="site", columns="variable", values=["prism", "cru_2km_1961_1990", "diff", "pct_diff"]).round(2))
    mon = df[(df.variable == "tas") & (df.period != "Annual")]
    print("\ntas monthly |CRU - PRISM| (°C): median %.3f, max %.3f" % (mon["diff"].abs().median(), mon["diff"].abs().max()))


if __name__ == "__main__":
    main(Path(sys.argv[1]))
