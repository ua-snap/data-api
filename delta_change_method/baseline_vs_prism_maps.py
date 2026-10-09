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

Also does the same for the CRU-TS 2km 1961-1990 mean (the other candidate baseline):
  * temperature: data/maps/temperature_cru_1961_1990_mXX.tif (from wcps_server_side.tas_baseline_map)
  * precipitation: CRU-TS 1901-2015 mean / precipitation_baseline_ratio.tif

Writes, north-up:
  data/maps/temperature_cru1901_2015_minus_prism.tif   (°C)
  data/maps/temperature_cru1961_1990_minus_prism.tif   (°C)
  data/maps/precipitation_cru1901_2015_vs_prism_pct.tif (%)
  data/maps/precipitation_cru1961_1990_vs_prism_pct.tif (%)

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


def read_tas_cru(fp):
    with rio.open(fp) as src:
        a = src.read(1).astype("float32").T  # undo the coverage's X/Y transpose -> rows are Y
        b = src.bounds
    a[(a < -9000) | ~np.isfinite(a)] = np.nan
    return a, b


def temperature(prism_dir):
    prism, pprof = read_prism(prism_dir, "tas", "tas_mean_C_akcan_prism_{m}_1961_1990.tif", np.mean)
    cru_1901_2015, b = read_tas_cru(MAPS / "temperature_cru_1901_2015.tif")
    cru_1961_1990 = np.nanmean(
        [read_tas_cru(MAPS / f"temperature_cru_1961_1990_m{m:02d}.tif")[0] for m in range(1, 13)], axis=0
    )
    # locate the CRU block inside the PRISM grid (same 2km cells)
    pt = pprof["transform"]
    row0 = round((pt.f - b.top) / -pt.e)
    col0 = round((b.left - pt.c) / pt.a)
    p = prism[row0 : row0 + cru_1901_2015.shape[0], col0 : col0 + cru_1901_2015.shape[1]]
    transform = rio.transform.from_origin(b.left, b.top, pt.a, -pt.e)
    out = {}
    for label, cru in [("1901_2015", cru_1901_2015), ("1961_1990", cru_1961_1990)]:
        out[label] = cru - p
        write(MAPS / f"temperature_cru{label}_minus_prism.tif", out[label], transform, pprof["crs"])
    return out


def precipitation(prism_dir):
    prism, pprof = read_prism(prism_dir, "pr", "pr_total_mm_akcan_prism_{m}_1961_1990.tif", np.sum)
    with rio.open(MAPS / "precipitation_cru_1901_2015.tif") as src:
        cru_1901_2015 = src.read(1).astype("float32")  # this coverage is not transposed
        transform, crs = src.transform, src.crs
    with rio.open(MAPS / "precipitation_baseline_ratio.tif") as src:
        ratio = src.read(1).astype("float32")  # CRU 1901-2015 mean / CRU 1961-1990 mean
    for a in (cru_1901_2015, ratio):
        a[(a < -9000) | ~np.isfinite(a)] = np.nan
    cru_1961_1990 = cru_1901_2015 / ratio
    p = np.full(cru_1901_2015.shape, np.nan, dtype="float32")
    reproject(prism, p, src_transform=pprof["transform"], src_crs=pprof["crs"],
              dst_transform=transform, dst_crs=crs, src_nodata=np.nan, dst_nodata=np.nan,
              resampling=Resampling.nearest)
    out = {}
    for label, cru in [("1901_2015", cru_1901_2015), ("1961_1990", cru_1961_1990)]:
        out[label] = 100 * (cru / p - 1)
        write(MAPS / f"precipitation_cru{label}_vs_prism_pct.tif", out[label], transform, crs)
    return out


if __name__ == "__main__":
    prism_dir = Path(sys.argv[1])
    for var, unit, out in [("temperature", "°C", temperature(prism_dir)), ("precipitation", "%", precipitation(prism_dir))]:
        for label, arr in out.items():
            print(f"{var} CRU {label} vs PRISM ({unit}) percentiles 5/50/95:",
                  np.nanpercentile(arr, [5, 50, 95]).round(2), f"| |x| median {np.nanmedian(np.abs(arr)):.3f}")
