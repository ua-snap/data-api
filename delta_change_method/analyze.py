"""Compare current Arctic-EDS summaries with delta-change-method summaries at the test sites.

The delta change method needs each GCM's own historical run (G_hist):

    additive        future = B + (G_future - G_hist)
    multiplicative  future = B * min(G_future / G_hist, cap)

where B is the historical baseline the app already shows. The degree-day coverages (NCAR
12km) are the only EDS coverages that contain GCM historical runs and do not already carry the
delta method (see check_gcm_historical.py), so only those components are computed here.
Temperature and precipitation were delta-downscaled during production (see ar5_baseline.py);
wet days and snowfall have no G_hist in Rasdaman.

Reads the cubes cached by fetch_site_data.py. For each degree-day component, reproduces
the min/mean/max summary the /eds/all endpoint returns today ("current"), then recomputes
it with the delta change method applied to each future model-year.

Writes data/summary_long.csv, data/comparison.csv, data/ratios.csv, data/extras.json.

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

COVERAGES = {
    "freezing_index": "air_freezing_index_Fdays",
    "thawing_index": "air_thawing_index_Fdays",
    "heating_degree_days": "heating_degree_days_Fdays",
}


def mmm(values):
    v = np.asarray(values, dtype=float).ravel()
    v = v[~np.isnan(v)]
    if v.size == 0:
        return {"min": np.nan, "mean": np.nan, "max": np.nan}
    return {"min": v.min(), "mean": v.mean(), "max": v.max()}


def degree_days(site, cov):
    """NCAR 12km: model (0 Daymet, 1-9 GCMs) x scenario (0 hist, 1 rcp45, 2 rcp85) x year 1950-2099.

    GCM years before 2006 live in the RCP tracks (the 'historical' scenario index is empty
    for GCMs), so G_hist is the 1980-2009 mean of each model/scenario track, matching the
    Daymet 1980-2009 baseline period the app already uses.
    """
    dd = np.array(site[cov], dtype=float)
    B = np.nanmean(dd[0, 0, 30:60])
    G = dd[1:, 1:, :]  # 9 models x 2 scenarios x 150 years
    H = np.nanmean(G[:, :, 30:60], axis=2, keepdims=True)
    rows, ratios = [], []
    for era, (a, b) in ERAS.items():
        fut = G[:, :, a - 1950 : b - 1950 + 1]
        rows.append(("current", era, mmm(fut)))
        rows.append(("delta", era, mmm(np.maximum(B + (fut - H), 0))))
        ratio = fut / H
        for cap_name, cap in CAPS.items():
            rows.append((f"delta_mult_{cap_name}", era, mmm(B * np.minimum(ratio, cap))))
        ratios.append(ratio.ravel())
    return B, H.ravel(), rows, np.concatenate(ratios)


def main():
    long_rows, ratio_rows, gcm_hist = [], [], {}
    for fp in sorted(RAW.glob("*.json")):
        site = json.loads(fp.read_text())
        for comp, cov in COVERAGES.items():
            B, H, rows, ratios = degree_days(site, cov)
            if np.isnan(B):  # outside the NCAR 12km grid (Ketchikan)
                continue
            gcm_hist.setdefault(comp, {})[site["name"]] = H.tolist()
            for method, era, stats in rows:
                for stat, value in stats.items():
                    long_rows.append(
                        dict(
                            site=site["name"],
                            region=site["region"],
                            component=comp,
                            era=era,
                            method=method,
                            stat=stat,
                            value=value,
                            baseline=B,
                            gcm_hist_mean=np.nanmean(H),
                        )
                    )
            r = ratios[~np.isnan(ratios)]
            ratio_rows += [dict(site=site["name"], component=comp, ratio=v) for v in r]

    long = pd.DataFrame(long_rows)
    long.to_csv(DATA / "summary_long.csv", index=False)
    pd.DataFrame(ratio_rows).to_csv(DATA / "ratios.csv", index=False)
    (DATA / "extras.json").write_text(json.dumps({"gcm_hist": gcm_hist}))

    # displayed mean, current vs additive delta, plus the % change the app shows (Diff.vue)
    means = long[long.stat == "mean"]
    idx = ["site", "region", "component", "era"]
    comp = means[means.method == "current"].set_index(idx)[["baseline", "gcm_hist_mean", "value"]]
    comp = comp.rename(columns={"value": "current"})
    comp["delta"] = means[means.method == "delta"].set_index(idx)["value"]
    comp["diff"] = comp["delta"] - comp["current"]
    comp["pct_diff"] = 100 * comp["diff"] / comp["current"]
    comp["pct_change_current"] = 100 * (comp["current"] - comp["baseline"]) / comp["baseline"]
    comp["pct_change_delta"] = 100 * (comp["delta"] - comp["baseline"]) / comp["baseline"]
    comp["pp_shift"] = comp["pct_change_delta"] - comp["pct_change_current"]
    comp = comp.reset_index()
    comp.to_csv(DATA / "comparison.csv", index=False)

    pd.set_option("display.width", 200)
    print(
        comp[comp.era == "2040-2069"]
        .groupby("component")[["diff", "pct_diff", "pp_shift"]]
        .agg(["mean", "min", "max"])
        .round(1)
    )


if __name__ == "__main__":
    main()
