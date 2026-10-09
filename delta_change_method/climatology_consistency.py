"""Which candidate climatology do the AR5 2km projections share their fine-scale pattern with?

This compares spatial patterns, not values. Each AR5 value is climatology + GCM delta, and
the deltas were interpolated from ~2.5° GCM grids, so over a small block of 2km cells the
field AR5 - climatology (temperature) or AR5 / climatology (precipitation) is smooth *if*
the climatology is the one the deltas were added to. A different climatology leaves
terrain-scale detail in the residual.

Limit: the CRU-TS 2km anomalies are also coarse (0.5°), so CRU-TS 2km averaged over any
period carries the same terrain pattern. A smooth residual against it shows a shared base
climatology, not that the base is specifically 1961-1990.

For a 15 x 15 cell block around each site, this measures the "roughness" (standard deviation
after removing a best-fit plane) of the residual against two candidates:
  * the 1961-1990 mean of the CRU-TS 4.0 2km historical coverage in Rasdaman
  * the downloaded SNAP PRISM 1961-1990 2km GeoTIFFs

Writes data/climatology_consistency.csv.

Usage: python climatology_consistency.py /path/to/prism
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import rasterio as rio

from prism_check import PR_GRID
from sites import SITES
from wcps import to_3338, wcps

DATA = Path(__file__).parent / "data"
HALF = 7  # cells either side of the site -> 15 x 15 block


def T(year):
    return f'"{year}-01-01T00:00:00.000Z"'


def roughness(a):
    """Std of the residual after removing a best-fit plane."""
    ok = ~np.isnan(a)
    yy, xx = np.indices(a.shape)
    A = np.c_[xx[ok], yy[ok], np.ones(ok.sum())]
    coef, *_ = np.linalg.lstsq(A, a[ok], rcond=None)
    return float(np.std(a[ok] - A @ coef))


def read_prism(prism_dir, var, pattern, reduce):
    arrs = []
    for m in range(1, 13):
        with rio.open(prism_dir / var / pattern.format(m=f"{m:02d}")) as src:
            arrs.append(src.read(1, masked=True).filled(np.nan))
            transform = src.transform
    return reduce(np.array(arrs), axis=0), transform


def block(cov, sel, cx, cy, h):
    q = f'for $c in ({cov}) return encode($c{sel},X({cx - h}:{cx + h}),Y({cy - h}:{cy + h})], "application/json")'
    return np.array(wcps(q), dtype=float)


def temperature(prism, transform, x, y):
    # tas_2km_* share the PRISM grid; the JSON block's first spatial axis maps to PRISM rows
    r, c = rio.transform.rowcol(transform, x, y)
    cx, cy = transform * (c + 0.5, r + 0.5)
    h = HALF * 2000
    cru = np.nanmean(block("tas_2km_historical_wcs", ".tas[year(60:89)", cx, cy, h), axis=(0, 1))
    ar5 = np.nanmean(block("tas_2km_projected_wcs", ".tas[model(0),scenario(0),year(0:29)", cx, cy, h), axis=(0, 1))
    # allow a one-cell registration offset for the PRISM block, keep the smoothest
    candidates = [
        prism[r - HALF + di : r + HALF + 1 + di, c - HALF + dj : c + HALF + 1 + dj]
        for di in (-1, 0, 1)
        for dj in (-1, 0, 1)
    ]
    return roughness(ar5 - cru), min(roughness(ar5 - p) for p in candidates)


def precipitation(prism, transform, x, y):
    # annual_precip_totals_mm sits on a slightly different (warped) grid: sample PRISM at
    # its cell centers, trying each block orientation and keeping the best-registered one
    x0, y0, dx, dy = PR_GRID
    cx = x0 + ((x - x0) // dx + 0.5) * dx
    cy = y0 - ((y0 - y) // dy + 0.5) * dy
    h = HALF * dx
    cru = np.nanmean(block("annual_precip_totals_mm", f"[model(0),scenario(0),year({T(1961)}:{T(1990)})", cx, cy, h), axis=0)
    ar5 = np.nanmean(block("annual_precip_totals_mm", f"[model(1),scenario(3),year({T(2006)}:{T(2035)})", cx, cy, h), axis=0)
    best = None
    for transpose in (False, True):
        for flip in (False, True):
            p = np.full(cru.shape, np.nan)
            for a in range(cru.shape[0]):
                for b in range(cru.shape[1]):
                    ia, ib = (b, a) if transpose else (a, b)
                    px = cx - h + ib * dx
                    py = cy - h + ia * dy if flip else cy + h - ia * dy
                    rr, cc = rio.transform.rowcol(transform, px, py)
                    p[a, b] = prism[rr, cc]
            score = roughness(np.log(cru / p))
            if best is None or score < best[0]:
                best = (score, p)
    return roughness(np.log(ar5 / cru)), roughness(np.log(ar5 / best[1]))


def main(prism_dir):
    tas, tr = read_prism(prism_dir, "tas", "tas_mean_C_akcan_prism_{m}_1961_1990.tif", np.mean)
    pr, _ = read_prism(prism_dir, "pr", "pr_total_mm_akcan_prism_{m}_1961_1990.tif", np.sum)
    rows = []
    for name, region, lat, lon in SITES:
        x, y = to_3338(lat, lon)
        t_cru, t_prism = temperature(tas, tr, x, y)
        p_cru, p_prism = precipitation(pr, tr, x, y)
        rows.append(dict(site=name, region=region,
                         tas_rough_vs_cru_c=t_cru, tas_rough_vs_prism_c=t_prism,
                         pr_rough_vs_cru_log=p_cru, pr_rough_vs_prism_log=p_prism))
        print(f"{name:15s} tas {t_cru:.3f} vs {t_prism:.3f} °C | pr {p_cru:.4f} vs {p_prism:.4f}")
    pd.DataFrame(rows).to_csv(DATA / "climatology_consistency.csv", index=False)


if __name__ == "__main__":
    main(Path(sys.argv[1]))
