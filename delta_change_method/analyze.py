"""Compare current Arctic-EDS summaries with delta-change-method summaries at the test sites.

Reads the cubes cached by fetch_site_data.py and, for each component, reproduces the
min/mean/max summary the /eds/all endpoint currently returns ("current"), then recomputes
the same summary with the delta change method applied to each future value ("delta"):

    additive        future = B + (G_future - G_hist)
    multiplicative  future = B * min(G_future / G_hist, cap)

where B is the historical baseline the app already shows and G_hist is the model's own
value over the reference period. How G_hist is obtained differs by dataset; see METHODS
below and REPORT.md.

Writes data/summary_long.csv, data/comparison.csv, data/ratios.csv.

Usage: python analyze.py
"""

import json
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).parent
RAW = HERE / "data" / "raw"
DATA = HERE / "data"

ERAS = {"2010-2039": (2010, 2039), "2040-2069": (2040, 2069), "2070-2099": (2070, 2099)}
CAPS = {"uncapped": np.inf, "cap3x": 3.0, "cap2x": 2.0, "cap1.5x": 1.5}
DEFAULT_CAP = "cap3x"

METHODS = {
    "temperature": "additive; B = CRU-TS 4.0 2km 1901-2015; G_hist = 1961-1990 downscaling reference",
    "precipitation": "multiplicative; B = CRU-TS 4.0 2km 1901-2015; G_hist = 1961-1990 downscaling reference",
    "snowfall": "multiplicative; B = CRU-TS 3.1 1910-2009; G_hist = 1970-1999 downscaling reference (approx.)",
    "freezing_index": "additive; B = Daymet 1980-2009; G_hist = each GCM 1980-2009",
    "thawing_index": "additive; B = Daymet 1980-2009; G_hist = each GCM 1980-2009",
    "heating_degree_days": "additive; B = Daymet 1980-2009; G_hist = each GCM 1980-2009",
    "wet_days_per_year": "multiplicative; B = ERA-Interim 1980-2009; G_hist inferred from 2006-2009 overlap (indicative only)",
}

UNITS = {
    "temperature": "°C",
    "precipitation": "mm",
    "snowfall": "mm SWE",
    "freezing_index": "°F·days",
    "thawing_index": "°F·days",
    "heating_degree_days": "°F·days",
    "wet_days_per_year": "days",
}


def arr(site, key):
    return np.array(site[key], dtype=float)


def mmm(values):
    v = np.asarray(values, dtype=float).ravel()
    v = v[~np.isnan(v)]
    if v.size == 0:
        return {"min": np.nan, "mean": np.nan, "max": np.nan}
    return {"min": v.min(), "mean": v.mean(), "max": v.max()}


def additive(B, fut, hist, floor=None):
    out = B + (fut - hist)
    if floor is not None:
        out = np.maximum(out, floor)
    return out


def multiplicative(B, fut, hist, cap):
    ratio = fut / hist
    return B * np.minimum(ratio, cap), ratio


def temperature(site):
    """tas_2km: CRU-TS 4.0 (month x year 1901-2015); AR5 (model x scen x month x year 2006-2100).

    The app's annual summary uses the 5ModelAvg / RCP 8.5 series, and that's what's
    reproduced here. SNAP's AR5 2km data were delta-downscaled onto a PRISM 1961-1990
    climatology, so a downscaled GCM's 1961-1990 mean equals the CRU-TS 2km 1961-1990
    mean at every pixel. That stands in for G_hist (GCM historical runs aren't in
    Rasdaman).
    """
    hist = arr(site, "tas_2km_historical_wcs")  # month, year
    proj = arr(site, "tas_2km_projected_wcs")  # model, scen, month, year
    hist_annual = np.nanmean(hist, axis=0)
    B = np.nanmean(hist_annual)
    H = np.nanmean(hist_annual[60:90])  # 1961-1990
    annual = np.nanmean(proj[0, 1], axis=0)  # 5ModelAvg rcp85, annual means 2006-2100
    rows = []
    for era, (a, b) in ERAS.items():
        cur = annual[a - 2006 : b - 2006 + 1]
        rows.append(("current", era, mmm(cur)))
        rows.append(("delta", era, mmm(additive(B, cur, H))))
    monthly_offset = np.nanmean(hist, axis=1) - np.nanmean(hist[:, 60:90], axis=1)
    return B, H, rows, {"monthly_offset": monthly_offset.tolist()}, None


def precipitation(site):
    """annual_precip_totals_mm: model (0 CRU-TS, 2-6 GCMs) x year 1901-2100 x scenario."""
    pr = arr(site, "annual_precip_totals_mm")
    B = np.nanmean(pr[0, :115, 0])
    H = np.nanmean(pr[0, 60:90, 0])  # 1961-1990 downscaling reference, see temperature()
    rows, ratios = [], []
    for era, (a, b) in ERAS.items():
        cur = pr[2:7, a - 1901 : b - 1901 + 1, 1:4]
        rows.append(("current", era, mmm(cur)))
        for cap_name, cap in CAPS.items():
            val, ratio = multiplicative(B, cur, H, cap)
            rows.append((f"delta_{cap_name}", era, mmm(val)))
        ratios.append(ratio.ravel())
    return B, H, rows, None, np.concatenate(ratios)


def snowfall(site):
    """mean_annual_snowfall_mm: model (0 CRU-TS, 1-5 GCMs) x scenario x decade (1910s-2090s).

    The app pools all GCM decades 2010-2099 into a single "projected" summary. The AR5 SFE
    was built on a PRISM 1971-2000 climatology, so CRU-TS decades 1970-1999 approximate
    G_hist. SFE is a nonlinear function of monthly tas and pr, so this is approximate.
    """
    s = arr(site, "mean_annual_snowfall_mm")
    B = np.nanmean(s[0, 0, :10])
    H = np.nanmean(s[0, 0, 6:9])
    cur = s[1:, 1:, 10:]
    rows = [("current", "2010-2099", mmm(cur))]
    for cap_name, cap in CAPS.items():
        val, ratio = multiplicative(B, cur, H, cap)
        rows.append((f"delta_{cap_name}", "2010-2099", mmm(val)))
    return B, H, rows, None, ratio.ravel()


def degree_days(site, cov):
    """NCAR 12km: model (0 Daymet, 1-9 GCMs) x scenario (0 hist, 1 rcp45, 2 rcp85) x year 1950-2099.

    GCM years before 2006 live in the RCP tracks (the 'historical' scenario is empty for
    GCMs), so G_hist is the 1980-2009 mean of each model/scenario track. That is a true
    delta-change calculation, nothing approximated.
    """
    dd = arr(site, cov)
    B = np.nanmean(dd[0, 0, 30:60])
    G = dd[1:, 1:, :]  # 9 models x 2 scenarios x 150 years
    Hms = np.nanmean(G[:, :, 30:60], axis=2, keepdims=True)
    rows, ratios = [], []
    for era, (a, b) in ERAS.items():
        cur = G[:, :, a - 1950 : b - 1950 + 1]
        rows.append(("current", era, mmm(cur)))
        rows.append(("delta", era, mmm(additive(B, cur, Hms, floor=0))))
        for cap_name, cap in CAPS.items():
            val, ratio = multiplicative(B, cur, Hms, cap)
            rows.append((f"delta_mult_{cap_name}", era, mmm(val)))
        ratios.append(ratio.ravel())
    extra = {"gcm_hist": Hms.ravel().tolist()}
    return B, float(np.nanmean(Hms)), rows, extra, np.concatenate(ratios)


def wet_days(site):
    """wet_days_per_year: model (0 ERA-Interim 1980-2009, 1 GFDL-CM3, 2 NCAR-CCSM4 2006-2100).

    The WRF GCM historical runs were never ingested, so G_hist can't be computed. As an
    *indicative* stand-in, assume each GCM's bias over 2006-2009 (the only years both
    ERA-Interim and the GCMs cover) holds over 1980-2009: G_hist = G_0609 * B / ERA_0609.
    Four years is far too short for a real bias estimate.
    """
    w = arr(site, "wet_days_per_year")
    B = np.nanmean(w[0, :30])
    era0609 = np.nanmean(w[0, 26:30])
    H = np.nanmean(w[1:3, 26:30], axis=1, keepdims=True) * B / era0609
    rows, ratios = [], []
    for era, (a, b) in ERAS.items():
        cur = w[1:3, a - 1980 : b - 1980 + 1]
        rows.append(("current", era, mmm(cur)))
        for cap_name, cap in CAPS.items():
            val, ratio = multiplicative(B, cur, H, cap)
            rows.append((f"delta_{cap_name}", era, mmm(np.minimum(val, 365))))
        ratios.append(ratio.ravel())
    return B, float(np.nanmean(H)), rows, None, np.concatenate(ratios)


COMPONENTS = {
    "temperature": temperature,
    "precipitation": precipitation,
    "snowfall": snowfall,
    "freezing_index": lambda s: degree_days(s, "air_freezing_index_Fdays"),
    "thawing_index": lambda s: degree_days(s, "air_thawing_index_Fdays"),
    "heating_degree_days": lambda s: degree_days(s, "heating_degree_days_Fdays"),
    "wet_days_per_year": wet_days,
}


def main():
    long_rows, ratio_rows, extras = [], [], {}
    for fp in sorted(RAW.glob("*.json")):
        site = json.loads(fp.read_text())
        for comp, fn in COMPONENTS.items():
            B, H, rows, extra, ratios = fn(site)
            if np.isnan(B):
                continue
            for method, era, stats in rows:
                for stat, value in stats.items():
                    long_rows.append(
                        dict(
                            site=site["name"],
                            region=site["region"],
                            lat=site["lat"],
                            lon=site["lon"],
                            component=comp,
                            units=UNITS[comp],
                            era=era,
                            method=method,
                            stat=stat,
                            value=value,
                            baseline=B,
                            gcm_hist=H,
                        )
                    )
            if ratios is not None:
                r = ratios[~np.isnan(ratios)]
                ratio_rows += [
                    dict(site=site["name"], component=comp, ratio=v) for v in r
                ]
            if extra:
                extras.setdefault(comp, {})[site["name"]] = extra

    long = pd.DataFrame(long_rows)
    long.to_csv(DATA / "summary_long.csv", index=False)
    pd.DataFrame(ratio_rows).to_csv(DATA / "ratios.csv", index=False)
    (DATA / "extras.json").write_text(json.dumps(extras))

    # side-by-side comparison of the displayed mean, using the default cap for
    # multiplicative variables and the additive method for degree days
    preferred = {c: "delta" for c in COMPONENTS}
    for c in ("precipitation", "snowfall", "wet_days_per_year"):
        preferred[c] = f"delta_{DEFAULT_CAP}"
    means = long[long.stat == "mean"]
    cur = means[means.method == "current"].set_index(["site", "component", "era"])
    dlt = pd.concat(
        [means[(means.component == c) & (means.method == m)] for c, m in preferred.items()]
    ).set_index(["site", "component", "era"])
    comp = cur[["region", "units", "baseline", "gcm_hist", "value"]].rename(
        columns={"value": "current"}
    )
    comp["delta"] = dlt["value"]
    comp["diff"] = comp["delta"] - comp["current"]
    comp["pct_diff"] = 100 * comp["diff"] / comp["current"].abs()
    # the change the app displays vs the baseline (Diff.vue), before and after
    comp["change_current"] = comp["current"] - comp["baseline"]
    comp["change_delta"] = comp["delta"] - comp["baseline"]
    comp["pct_change_current"] = 100 * comp["change_current"] / comp["baseline"].abs()
    comp["pct_change_delta"] = 100 * comp["change_delta"] / comp["baseline"].abs()
    comp = comp.reset_index()
    comp.to_csv(DATA / "comparison.csv", index=False)

    pd.set_option("display.width", 200)
    print(
        comp[comp.era.isin(["2040-2069", "2010-2099"])]
        .groupby("component")[["diff", "pct_diff", "change_current", "change_delta"]]
        .agg(["mean", "min", "max"])
        .round(1)
    )


if __name__ == "__main__":
    main()
