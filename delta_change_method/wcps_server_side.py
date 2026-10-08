"""Part B: can Rasdaman do the delta change math for us via WCPS?

1. Point queries: for every test site, compute the delta-method 2040-2069 mean entirely
   server-side (one WCPS request per component) and check it against analyze.py.
2. Map queries: compute statewide delta-method adjustment fields in a single request
   each and save them as GeoTIFFs for plotting.

Rasdaman quirks we hit, and the workarounds used here:
  * Combining avg() reductions over different-length subsets (e.g. 1901-2015 vs
    1961-1990) fails with "axes not compatible". Casting each aggregate to (double) fixes it.
  * condense over a `year` iterator mistranslates geo years into grid indices; explicit
    sums of the year slices work instead (sent via POST since the query gets long).
  * tas_2km_projected_wcs has an irregular scenario axis whose coefficients are 0 and 2,
    so RCP 8.5 is scenario(2) even though the encoding metadata labels it "1".
  * Map outputs from the NCAR 12km degree-day coverages come back with X/Y transposed
    (the 2km AR5 precip coverage does not), and -9999 nodata propagates.

Usage: python wcps_server_side.py   (run analyze.py first)
"""

import time
from pathlib import Path

import numpy as np
import pandas as pd
import requests

from sites import SITES
from wcps import RAS_BASE_URL, to_3338

HERE = Path(__file__).parent
DATA = HERE / "data"
MAPS = DATA / "maps"
MAPS.mkdir(parents=True, exist_ok=True)


def T(year):
    return f'"{year}-01-01T00:00:00.000Z"'


def run(query, timeout=600):
    """POST a WCPS query; return (response, seconds)."""
    t0 = time.time()
    r = requests.post(
        RAS_BASE_URL,
        data={
            "SERVICE": "WCS",
            "VERSION": "2.0.1",
            "REQUEST": "ProcessCoverages",
            "query": query,
        },
        timeout=timeout,
    )
    r.raise_for_status()
    return r, time.time() - t0


# --- point queries -----------------------------------------------------------------


def q_temperature(x, y):
    # B + (G_fut - G_hist), 5ModelAvg RCP 8.5, 2040-2069 (year index 34:63 from 2006)
    return (
        "for $h in (tas_2km_historical_wcs), $p in (tas_2km_projected_wcs) return encode("
        f"(double)avg($h.tas[year(0:114),X({x}),Y({y})])"
        f" + (double)avg($p.tas[model(0),scenario(2),year(34:63),X({x}),Y({y})])"
        f" - (double)avg($h.tas[year(60:89),X({x}),Y({y})]),"
        ' "application/json")'
    )


def q_precipitation(x, y, cap=3.0):
    # mean over 5 GCMs x 3 RCPs of  B * min(G_fut / G_hist, cap)
    B = f"(double)avg($c[model(0),scenario(0),year({T(1901)}:{T(2015)}),X({x}),Y({y})])"
    H = f"(double)avg($c[model(0),scenario(0),year({T(1961)}:{T(1990)}),X({x}),Y({y})])"
    F = f"(double)avg($c[model($m),scenario($s),year({T(2040)}:{T(2069)}),X({x}),Y({y})])"
    return (
        "for $c in (annual_precip_totals_mm) return encode("
        f"{B} * (condense + over $m model(2:6), $s scenario(1:3) using min({F} / {H}, {cap})) / 15.0,"
        ' "application/json")'
    )


def q_degree_days(cov, x, y):
    # mean over 9 GCMs x 2 RCPs of  B + (G_fut - G_hist)
    B = f"(double)avg($c[model(0),scenario(0),year(1980:2009),X({x}),Y({y})])"
    F = f"(double)avg($c[model($m),scenario($s),year(2040:2069),X({x}),Y({y})])"
    H = f"(double)avg($c[model($m),scenario($s),year(1980:2009),X({x}),Y({y})])"
    return (
        f"for $c in ({cov}) return encode("
        f"{B} + (condense + over $m model(1:9), $s scenario(1:2) using {F} - {H}) / 18.0,"
        ' "application/json")'
    )


POINT_QUERIES = {
    "temperature": q_temperature,
    "precipitation": q_precipitation,
    "freezing_index": lambda x, y: q_degree_days("air_freezing_index_Fdays", x, y),
    "thawing_index": lambda x, y: q_degree_days("air_thawing_index_Fdays", x, y),
    "heating_degree_days": lambda x, y: q_degree_days("heating_degree_days_Fdays", x, y),
}

# the analyze.py method each server-side result should match
PYTHON_METHOD = {
    "temperature": "delta",
    "precipitation": "delta_cap3x",
    "freezing_index": "delta",
    "thawing_index": "delta",
    "heating_degree_days": "delta",
}


def point_validation():
    long = pd.read_csv(DATA / "summary_long.csv")
    py = long[(long.era == "2040-2069") & (long.stat == "mean")]
    rows = []
    for name, _, lat, lon in SITES:
        x, y = to_3338(lat, lon)
        for comp, qfn in POINT_QUERIES.items():
            r, secs = run(qfn(x, y))
            server = float(r.text) if r.text.strip() != "null" else np.nan
            match = py[
                (py.site == name)
                & (py.component == comp)
                & (py.method == PYTHON_METHOD[comp])
            ]
            local = match.value.iloc[0] if len(match) else np.nan
            if server < -9000:  # nodata cell
                server = np.nan
            rows.append(
                dict(site=name, component=comp, wcps=server, python=local, seconds=secs)
            )
            print(f"{name:15s} {comp:20s} wcps={server:10.2f} python={local:10.2f} {secs:.2f}s")
    df = pd.DataFrame(rows)
    df["abs_diff"] = (df.wcps - df.python).abs()
    df.to_csv(DATA / "wcps_validation.csv", index=False)
    return df


# --- map queries -------------------------------------------------------------------


def ysum(m, s, years, axis_fmt=str):
    return "(" + " + ".join(
        f"$c[model({m}),scenario({s}),year({axis_fmt(yr)})]" for yr in years
    ) + ")"


def map_dd_adjustment(cov):
    """Delta-method adjustment for degree days: B - mean_ms(G_hist), statewide."""
    yrs = range(1980, 2010)
    B = f"{ysum(0, 0, yrs)} / 30.0"
    H = f"(condense + over $m model(1:9), $s scenario(1:2) using {ysum('$m', '$s', yrs)}) / 540.0"
    return f'for $c in ({cov}) return encode({B} - {H}, "image/tiff")'


def map_dd_baseline(cov):
    return f'for $c in ({cov}) return encode({ysum(0, 0, range(1980, 2010))} / 30.0, "image/tiff")'


def map_pr_factor():
    """Delta-method scaling for AR5 precip: B(1901-2015) / G_hist(1961-1990), statewide."""
    B = f"{ysum(0, 0, range(1901, 2016), T)} / 115.0"
    H = f"{ysum(0, 0, range(1961, 1991), T)} / 30.0"
    return f'for $c in (annual_precip_totals_mm) return encode(({B}) / ({H}), "image/tiff")'


MAP_QUERIES = {
    "freezing_index_adjustment": map_dd_adjustment("air_freezing_index_Fdays"),
    "freezing_index_baseline": map_dd_baseline("air_freezing_index_Fdays"),
    "thawing_index_adjustment": map_dd_adjustment("air_thawing_index_Fdays"),
    "thawing_index_baseline": map_dd_baseline("air_thawing_index_Fdays"),
    "precipitation_factor": map_pr_factor(),
}


def maps():
    timings = []
    for name, query in MAP_QUERIES.items():
        r, secs = run(query)
        (MAPS / f"{name}.tif").write_bytes(r.content)
        timings.append(dict(map=name, seconds=secs, bytes=len(r.content)))
        print(f"map {name}: {secs:.1f}s, {len(r.content) / 1e6:.1f} MB")
    pd.DataFrame(timings).to_csv(DATA / "wcps_map_timings.csv", index=False)


if __name__ == "__main__":
    df = point_validation()
    print("\nmax |wcps - python| by component:")
    print(df.groupby("component")[["abs_diff", "seconds"]].agg(["max", "mean"]).round(3))
    maps()
