"""Statewide maps: the app's historical baseline (CRU-TS 1901-2015 mean) vs PRISM 1961-1990.

Inputs:
  * data/maps/temperature_cru_1901_2015.tif and data/maps/precipitation_cru_1901_2015.tif,
    from wcps_server_side.tas_baseline_map() / precip_baseline_map()
  * the SNAP PRISM 1961-1990 2km GeoTIFFs (GeoNetwork record 0e8e42f7-6774-4d35-a7b3-4a82f8b48e00)

Temperature: tas_2km_historical_wcs is on the PRISM grid, so after undoing the coverage's X/Y
transpose the cells line up one to one. Precipitation: annual_precip_totals_mm was warped onto a
slightly different grid at ingest, so the PRISM annual total is nearest-neighbour resampled onto
it. That resampling doesn't always pick the same source cell the ingest warp did, so expect
cell-scale speckle in steep terrain (see prism_check.py).

Writes data/maps/temperature_cru1901_2015_minus_prism.tif (°C, north-up) and
data/maps/precipitation_cru1901_2015_vs_prism_pct.tif (%, north-up).

Usage: python baseline_vs_prism_maps.py /path/to/prism
"""

import sys
from pathlib import Path

import numpy as np
import rasterio as rio
from rasterio.warp import Resampling, reproject

MAPS = Path(__file__).parent / "data" / "maps"


def read_prism(prism_dir, var, pattern, reduce):
    arrs = []
    for m in range(1, 13):
        with rio.open(prism_dir / var / pattern.format(m=f"{m:02d}")) as src:
            arrs.append(src.read(1, masked=True).filled(np.nan))
            profile = src.profile
    return reduce(np.array(arrs), axis=0).astype("float32"), profile


def write(fp, arr, transform, crs):
    with rio.open(
        fp, "w", driver="GTiff", height=arr.shape[0], width=arr.shape[1], count=1,
        dtype="float32", crs=crs, transform=transform, nodata=np.nan, compress="lzw",
    ) as dst:
        dst.write(arr.astype("float32"), 1)


def temperature(prism_dir):
    prism, pprof = read_prism(prism_dir, "tas", "tas_mean_C_akcan_prism_{m}_1961_1990.tif", np.mean)
    with rio.open(MAPS / "temperature_cru_1901_2015.tif") as src:
        cru = src.read(1).astype("float32").T  # undo the coverage's X/Y transpose -> rows are Y
        b = src.bounds
    cru[(cru < -9000) | ~np.isfinite(cru)] = np.nan
    # locate the CRU block inside the PRISM grid (same 2km cells)
    pt = pprof["transform"]
    row0 = round((pt.f - b.top) / -pt.e)
    col0 = round((b.left - pt.c) / pt.a)
    p = prism[row0 : row0 + cru.shape[0], col0 : col0 + cru.shape[1]]
    diff = cru - p
    transform = rio.transform.from_origin(b.left, b.top, pt.a, -pt.e)
    write(MAPS / "temperature_cru1901_2015_minus_prism.tif", diff, transform, pprof["crs"])
    return diff


def precipitation(prism_dir):
    prism, pprof = read_prism(prism_dir, "pr", "pr_total_mm_akcan_prism_{m}_1961_1990.tif", np.sum)
    with rio.open(MAPS / "precipitation_cru_1901_2015.tif") as src:
        cru = src.read(1).astype("float32")  # this coverage is not transposed
        transform, crs = src.transform, src.crs
    cru[(cru < -9000) | ~np.isfinite(cru)] = np.nan
    p = np.full(cru.shape, np.nan, dtype="float32")
    reproject(prism, p, src_transform=pprof["transform"], src_crs=pprof["crs"],
              dst_transform=transform, dst_crs=crs, src_nodata=np.nan, dst_nodata=np.nan,
              resampling=Resampling.nearest)
    pct = 100 * (cru / p - 1)
    write(MAPS / "precipitation_cru1901_2015_vs_prism_pct.tif", pct, transform, crs)
    return pct


if __name__ == "__main__":
    prism_dir = Path(sys.argv[1])
    t = temperature(prism_dir)
    print("temperature CRU1901-2015 − PRISM (°C) percentiles 1/50/99:", np.nanpercentile(t, [1, 50, 99]).round(2))
    pr = precipitation(prism_dir)
    print("precip CRU1901-2015 vs PRISM (%) percentiles 1/50/99:", np.nanpercentile(pr, [1, 50, 99]).round(1))
