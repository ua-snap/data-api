"""Part B: can Rasdaman do the delta change math for us via WCPS?

Only the degree-day coverages (NCAR 12km) contain GCM historical runs, so they are the only
EDS inputs on which the full method can run (see check_gcm_historical.py).

1. Point queries: for every test site, compute the delta-method 2040-2069 mean entirely
   server-side, one WCPS request per component, both additive and multiplicative with a
   3x cap, and check the results against a Python implementation on the same cubes.
2. Map queries: compute statewide delta-method adjustment fields in one request each and
   save them as GeoTIFFs for plotting.

Rasdaman quirks hit during this work, and the workarounds:
  * Combining avg() reductions over different-length subsets (seen on
    annual_precip_totals_mm with 1901-2015 vs 1961-1990) fails with "axes not compatible".
    Casting each aggregate to (double) fixes it, so every aggregate below is cast.
  * condense over a `year` iterator mistranslates geo years into grid indices; explicit
    sums of the year slices work instead (sent via POST since the query gets long).
  * tas_2km_projected_wcs has an irregular scenario axis whose coefficients are 0 and 2,
    so RCP 8.5 is scenario(2) even though the encoding metadata labels it "1".
  * Map outputs from the NCAR 12km coverages come back with X/Y transposed (the 2km AR5
    coverages do not), and -9999 nodata propagates.

Usage: python wcps_server_side.py   (run fetch_site_data.py and analyze.py first)
"""

import json
import time
from pathlib import Path

import numpy as np
import pandas as pd
import requests

from sites import SITES
from wcps import RAS_BASE_URL, to_3338

HERE = Path(__file__).parent
DATA = HERE / "data"
RAW = DATA / "raw"
MAPS = DATA / "maps"
MAPS.mkdir(parents=True, exist_ok=True)

COVERAGES = {
    "freezing_index": "air_freezing_index_Fdays",
    "thawing_index": "air_thawing_index_Fdays",
    "heating_degree_days": "heating_degree_days_Fdays",
}
CAP = 3.0


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


def _terms(x, y):
    B = f"(double)avg($c[model(0),scenario(0),year(1980:2009),X({x}),Y({y})])"
    F = f"(double)avg($c[model($m),scenario($s),year(2040:2069),X({x}),Y({y})])"
    H = f"(double)avg($c[model($m),scenario($s),year(1980:2009),X({x}),Y({y})])"
    return B, F, H


def q_additive(cov, x, y):
    """Mean over 9 GCMs x 2 RCPs of  B + (G_fut - G_hist)."""
    B, F, H = _terms(x, y)
    return (
        f"for $c in ({cov}) return encode("
        f"{B} + (condense + over $m model(1:9), $s scenario(1:2) using {F} - {H}) / 18.0,"
        ' "application/json")'
    )


def q_multiplicative(cov, x, y, cap=CAP):
    """Mean over 9 GCMs x 2 RCPs of  B * min(G_fut / G_hist, cap)."""
    B, F, H = _terms(x, y)
    return (
        f"for $c in ({cov}) return encode("
        f"{B} * (condense + over $m model(1:9), $s scenario(1:2) using min({F} / {H}, {cap})) / 18.0,"
        ' "application/json")'
    )


def python_reference(cube):
    """Same two formulas in numpy, on the cube cached by fetch_site_data.py."""
    dd = np.array(cube, dtype=float)
    B = np.nanmean(dd[0, 0, 30:60])
    F = np.nanmean(dd[1:, 1:, 90:120], axis=2)  # 2040-2069
    H = np.nanmean(dd[1:, 1:, 30:60], axis=2)  # 1980-2009
    return B + np.mean(F - H), B * np.mean(np.minimum(F / H, CAP))


def point_validation():
    rows = []
    for name, _, lat, lon in SITES:
        x, y = to_3338(lat, lon)
        site = json.loads((RAW / f"{name.replace(' ', '_')}.json").read_text())
        for comp, cov in COVERAGES.items():
            ref_add, ref_mult = python_reference(site[cov])
            for method, qfn, ref in [
                ("additive", q_additive, ref_add),
                (f"multiplicative_cap{CAP:g}x", q_multiplicative, ref_mult),
            ]:
                r, secs = run(qfn(cov, x, y))
                text = r.text.strip()
                server = np.nan if text == "null" else float(text)
                if server < -9000:  # nodata cell
                    server = np.nan
                rows.append(
                    dict(site=name, component=comp, method=method, wcps=server, python=ref, seconds=secs)
                )
                print(f"{name:15s} {comp:20s} {method:22s} wcps={server:10.2f} python={ref:10.2f} {secs:.2f}s")
    df = pd.DataFrame(rows)
    df["abs_diff"] = (df.wcps - df.python).abs()
    df.to_csv(DATA / "wcps_validation.csv", index=False)
    return df


def cap_check(site="Utqiagvik", caps=(1.5, 1.3)):
    """At 3x the cap never binds on era means, so also test caps that do bind."""
    _, _, lat, lon = next(s for s in SITES if s[0] == site)
    x, y = to_3338(lat, lon)
    cov = "air_thawing_index_Fdays"
    dd = np.array(json.loads((RAW / f"{site}.json").read_text())[cov], dtype=float)
    B = np.nanmean(dd[0, 0, 30:60])
    F = np.nanmean(dd[1:, 1:, 90:120], axis=2)
    H = np.nanmean(dd[1:, 1:, 30:60], axis=2)
    rows = []
    for cap in caps:
        r, _ = run(q_multiplicative(cov, x, y, cap))
        rows.append(
            dict(
                site=site,
                component="thawing_index",
                cap=cap,
                runs_capped=int((F / H > cap).sum()),
                runs=F.size,
                wcps=float(r.text),
                python=B * np.mean(np.minimum(F / H, cap)),
                uncapped=B * np.mean(F / H),
            )
        )
    df = pd.DataFrame(rows)
    df.to_csv(DATA / "wcps_cap_check.csv", index=False)
    print(df.round(2))


# --- map queries -------------------------------------------------------------------


def ysum(m, s, years):
    return "(" + " + ".join(f"$c[model({m}),scenario({s}),year({yr})]" for yr in years) + ")"


def map_adjustment(cov):
    """Additive delta-method adjustment, B - mean_ms(G_hist), statewide.

    Adding this to the current ensemble-mean future gives the delta-method ensemble mean.
    """
    yrs = range(1980, 2010)
    B = f"{ysum(0, 0, yrs)} / 30.0"
    H = f"(condense + over $m model(1:9), $s scenario(1:2) using {ysum('$m', '$s', yrs)}) / 540.0"
    return f'for $c in ({cov}) return encode({B} - {H}, "image/tiff")'


def map_baseline(cov):
    return f'for $c in ({cov}) return encode({ysum(0, 0, range(1980, 2010))} / 30.0, "image/tiff")'


def T(year):
    return f'"{year}-01-01T00:00:00.000Z"'


def map_precip_baseline_ratio():
    """Not a delta calculation: the ratio of the app's precip baseline (CRU-TS 1901-2015) to
    the 1961-1990 mean that stands in for the PRISM climatology the AR5 deltas were added to
    (see ar5_baseline.py). ~75 s for the 2km grid."""
    yrs = lambda a, b: "(" + " + ".join(
        f"$c[model(0),scenario(0),year({T(y)})]" for y in range(a, b + 1)
    ) + ")"
    return (
        "for $c in (annual_precip_totals_mm) return encode("
        f'({yrs(1901, 2015)} / 115.0) / ({yrs(1961, 1990)} / 30.0), "image/tiff")'
    )


MAP_QUERIES = {
    "freezing_index_adjustment": map_adjustment("air_freezing_index_Fdays"),
    "freezing_index_baseline": map_baseline("air_freezing_index_Fdays"),
    "thawing_index_adjustment": map_adjustment("air_thawing_index_Fdays"),
    "thawing_index_baseline": map_baseline("air_thawing_index_Fdays"),
    "heating_degree_days_adjustment": map_adjustment("heating_degree_days_Fdays"),
    "heating_degree_days_baseline": map_baseline("heating_degree_days_Fdays"),
    "precipitation_baseline_ratio": map_precip_baseline_ratio(),
}


def maps():
    for stale in MAPS.glob("precipitation_factor.tif"):
        stale.unlink()
    timings = []
    for name, query in MAP_QUERIES.items():
        r, secs = run(query)
        (MAPS / f"{name}.tif").write_bytes(r.content)
        timings.append(dict(map=name, seconds=secs, bytes=len(r.content)))
        print(f"map {name}: {secs:.1f}s, {len(r.content) / 1e6:.1f} MB")
    pd.DataFrame(timings).to_csv(DATA / "wcps_map_timings.csv", index=False)


if __name__ == "__main__":
    df = point_validation()
    print("\nmax |wcps - python| and mean seconds by component and method:")
    print(df.groupby(["component", "method"]).agg(max_abs_diff=("abs_diff", "max"), mean_s=("seconds", "mean")).round(4))
    cap_check()
    maps()
