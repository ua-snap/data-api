"""Render the report figures from the analyze.py / wcps_server_side.py outputs.

Usage: python make_figures.py
"""

import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import rasterio as rio
from matplotlib.colors import LinearSegmentedColormap, TwoSlopeNorm

from sites import SITES
from wcps import to_3338

HERE = Path(__file__).parent
DATA = HERE / "data"
FIGS = HERE / "figures"
FIGS.mkdir(exist_ok=True)

# reference palette (dataviz skill): categorical slots 1-2, diverging blue<->red, gray mid
CURRENT = "#2a78d6"
DELTA = "#eb6834"
INK = "#0b0b0b"
INK2 = "#52514e"
MUTED = "#8a8985"
GRID = "#e4e3df"
SURFACE = "#fcfcfb"
DIVERGING = LinearSegmentedColormap.from_list(
    "bluered", ["#104281", "#3987e5", "#9ec5f4", "#f0efec", "#f4a9a8", "#e34948", "#9b1c1c"]
)
DIVERGING.set_bad("#ffffff")

plt.rcParams.update(
    {
        "figure.facecolor": SURFACE,
        "axes.facecolor": SURFACE,
        "savefig.facecolor": SURFACE,
        "font.size": 10,
        "font.family": "sans-serif",
        "axes.edgecolor": GRID,
        "axes.labelcolor": INK2,
        "axes.titlecolor": INK,
        "axes.titlesize": 11,
        "axes.titleweight": "semibold",
        "xtick.color": INK2,
        "ytick.color": INK2,
        "axes.grid": True,
        "grid.color": GRID,
        "grid.linewidth": 0.8,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "legend.frameon": False,
    }
)

SITE_ORDER = [s[0] for s in SITES]
REGION = {s[0]: s[1] for s in SITES}
LABELS = {
    "temperature": "Temperature (°C)",
    "precipitation": "Precipitation",
    "snowfall": "Snowfall (SFE)",
    "freezing_index": "Freezing index",
    "thawing_index": "Thawing index",
    "heating_degree_days": "Heating degree days",
    "wet_days_per_year": "Wet days per year*",
}


def headline_era(df):
    return df[df.era.isin(["2040-2069", "2010-2099"])]


def site_axis(ax, sites):
    """Sites top-to-bottom in SITES order, with region separators."""
    ax.set_yticks(range(len(sites)))
    ax.set_yticklabels(sites)
    ax.set_ylim(len(sites) - 0.5, -0.5)
    prev = None
    for i, s in enumerate(sites):
        if prev is not None and REGION[s] != REGION[prev]:
            ax.axhline(i - 0.5, color=MUTED, lw=0.6, ls=(0, (2, 2)))
        prev = s


def fig_dumbbells(comp):
    """Displayed change vs baseline, current vs delta, per site (mid-century)."""
    comps = [
        ("temperature", "change", "°C"),
        ("precipitation", "pct", "%"),
        ("snowfall", "pct", "%"),
        ("freezing_index", "pct", "%"),
        ("thawing_index", "pct", "%"),
        ("heating_degree_days", "pct", "%"),
        ("wet_days_per_year", "pct", "%"),
    ]
    df = headline_era(comp)
    fig, axes = plt.subplots(1, len(comps), figsize=(17, 7.6), sharey=True)
    for ax, (c, kind, unit) in zip(axes, comps):
        d = df[df.component == c].set_index("site").reindex(SITE_ORDER)
        cur = d.change_current if kind == "change" else d.pct_change_current
        dlt = d.change_delta if kind == "change" else d.pct_change_delta
        y = np.arange(len(SITE_ORDER))
        ax.hlines(y, cur, dlt, color=MUTED, lw=1.5, zorder=1)
        ax.scatter(cur, y, s=34, color=CURRENT, zorder=2, edgecolor=SURFACE, lw=1.2, label="Current (app today)")
        ax.scatter(dlt, y, s=34, color=DELTA, zorder=3, edgecolor=SURFACE, lw=1.2, label="Delta change method")
        ax.axvline(0, color=INK2, lw=0.8)
        ax.set_title(LABELS[c], loc="left")
        ax.set_xlabel(f"change vs baseline ({unit})")
        ax.grid(axis="y", visible=False)
        site_axis(ax, SITE_ORDER)
    axes[0].legend(loc="upper left", bbox_to_anchor=(0, 1.1), ncol=2, fontsize=10)
    fig.suptitle(
        "Projected change from the historical baseline, mid-century (2040–2069; snowfall 2010–2099)",
        x=0.01, ha="left", fontsize=13, fontweight="semibold", color=INK, y=1.04,
    )
    fig.text(0.01, -0.02, "* Wet days: GCM historical not in Rasdaman; delta inferred from a 4-year overlap (indicative only). Ketchikan is outside the degree-day grid.", color=INK2, fontsize=9)
    fig.tight_layout()
    fig.savefig(FIGS / "fig1_change_current_vs_delta.png", dpi=150, bbox_inches="tight")
    plt.close(fig)


def fig_pp_heatmap(comp):
    """Percentage-point shift in displayed % change (delta minus current), sites x components."""
    comps = ["precipitation", "snowfall", "freezing_index", "thawing_index", "heating_degree_days", "wet_days_per_year"]
    df = headline_era(comp)
    df = df.assign(pp=df.pct_change_delta - df.pct_change_current)
    m = df.pivot(index="site", columns="component", values="pp").reindex(index=SITE_ORDER, columns=comps)
    fig, ax = plt.subplots(figsize=(8.2, 8.6))
    lim = 20
    im = ax.imshow(m.values, cmap=DIVERGING, norm=TwoSlopeNorm(0, -lim, lim), aspect="auto")
    for i in range(m.shape[0]):
        for j in range(m.shape[1]):
            v = m.values[i, j]
            if np.isnan(v):
                ax.text(j, i, "n/a", ha="center", va="center", fontsize=8, color=MUTED)
            else:
                ax.text(j, i, f"{v:+.1f}", ha="center", va="center", fontsize=8,
                        color="white" if abs(v) > 0.6 * lim else INK)
    ax.set_xticks(range(len(comps)))
    ax.set_xticklabels([LABELS[c] for c in comps], rotation=30, ha="right")
    ax.grid(False)
    site_axis(ax, SITE_ORDER)
    for s in ax.spines.values():
        s.set_visible(False)
    cb = fig.colorbar(im, ax=ax, shrink=0.6, pad=0.02, extend="both")
    cb.set_label("percentage points (delta − current)")
    cb.outline.set_visible(False)
    ax.set_title("How much the displayed % change moves under the delta method\nmid-century (snowfall 2010–2099)", loc="left")
    fig.tight_layout()
    fig.savefig(FIGS / "fig2_pp_shift_heatmap.png", dpi=150, bbox_inches="tight")
    plt.close(fig)


def fig_temperature_monthly(extras):
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    m = np.array([extras["temperature"][s]["monthly_offset"] for s in SITE_ORDER])
    fig, ax = plt.subplots(figsize=(8.6, 8.2))
    lim = 1.5
    im = ax.imshow(m, cmap=DIVERGING, norm=TwoSlopeNorm(0, -lim, lim), aspect="auto")
    for i in range(m.shape[0]):
        for j in range(m.shape[1]):
            ax.text(j, i, f"{m[i, j]:+.1f}", ha="center", va="center", fontsize=7.5,
                    color="white" if abs(m[i, j]) > 0.65 * lim else INK)
    ax.set_xticks(range(12))
    ax.set_xticklabels(months)
    ax.grid(False)
    site_axis(ax, SITE_ORDER)
    for s in ax.spines.values():
        s.set_visible(False)
    cb = fig.colorbar(im, ax=ax, shrink=0.6, pad=0.02)
    cb.set_label("°C added to every projected value")
    cb.outline.set_visible(False)
    ax.set_title("Temperature: delta-method adjustment by month\nCRU-TS 1901–2015 mean minus 1961–1990 downscaling reference", loc="left")
    fig.tight_layout()
    fig.savefig(FIGS / "fig3_temperature_monthly_adjustment.png", dpi=150, bbox_inches="tight")
    plt.close(fig)


def fig_dd_bias(long, extras):
    """Each GCM's own 1980-2009 mean vs the Daymet baseline: what the delta method removes."""
    comps = ["freezing_index", "thawing_index", "heating_degree_days"]
    sites = [s for s in SITE_ORDER if s in extras["freezing_index"]]
    fig, axes = plt.subplots(1, 3, figsize=(13, 7.2), sharey=True)
    rng = np.random.default_rng(0)
    for ax, c in zip(axes, comps):
        base = long[long.component == c].groupby("site").baseline.first()
        for i, s in enumerate(sites):
            h = np.array(extras[c][s]["gcm_hist"])
            pct = 100 * (h - base[s]) / base[s]
            ax.scatter(pct, i + rng.uniform(-0.18, 0.18, pct.size), s=12, color=CURRENT, alpha=0.55, lw=0)
            ax.scatter(pct.mean(), i, s=60, marker="|", color=INK, lw=2)
        ax.axvline(0, color=INK2, lw=0.8)
        ax.set_title(LABELS[c], loc="left")
        ax.set_xlabel("GCM 1980–2009 mean vs Daymet (%)")
        ax.grid(axis="y", visible=False)
        site_axis(ax, sites)
    fig.suptitle("Degree days: model historical bias the delta method would remove\ndots = 9 GCMs × 2 RCP tracks; bar = ensemble mean",
                 x=0.01, ha="left", fontsize=12, fontweight="semibold", color=INK)
    fig.tight_layout()
    fig.savefig(FIGS / "fig4_degree_day_gcm_bias.png", dpi=150, bbox_inches="tight")
    plt.close(fig)


def fig_ratios():
    r = pd.read_csv(DATA / "ratios.csv")
    comps = ["precipitation", "snowfall", "wet_days_per_year", "thawing_index"]
    titles = {
        "thawing_index": "Thawing index (if treated multiplicatively)",
        "wet_days_per_year": "Wet days per year (indicative)",
    }
    fig, axes = plt.subplots(1, 4, figsize=(15, 3.6))
    for ax, c in zip(axes, comps):
        v = r[r.component == c].ratio
        ax.hist(v, bins=np.arange(0, max(3.4, v.max()) + 0.05, 0.05), color=CURRENT, edgecolor=SURFACE, lw=0.3)
        for cap, ls in [(1.5, ":"), (2, "--"), (3, "-")]:
            ax.axvline(cap, color=DELTA, lw=1.2, ls=ls)
            ax.text(cap, ax.get_ylim()[1] * 0.97, f" {cap:g}×\n {100 * (v > cap).mean():.2f}%", color=INK2, fontsize=8, va="top")
        ax.set_title(titles.get(c, LABELS[c]), loc="left", fontsize=10)
        ax.set_xlabel("annual G_future / G_hist")
        ax.set_xlim(0, 3.6)
        ax.grid(axis="x", visible=False)
    axes[0].set_ylabel("model-years (all sites)")
    fig.suptitle("Multiplicative deltas: how often would a cap bind?  (% of model-years above each cap)",
                 x=0.01, ha="left", fontsize=12, fontweight="semibold", color=INK)
    fig.tight_layout()
    fig.savefig(FIGS / "fig5_ratio_caps.png", dpi=150, bbox_inches="tight")
    plt.close(fig)


def read_map(name, transposed):
    with rio.open(DATA / "maps" / f"{name}.tif") as src:
        a = src.read(1).astype(float)
        b = src.bounds
    a[(a < -9000) | ~np.isfinite(a)] = np.nan
    # the degree-day (NCAR 12km) coverages come back with X/Y transposed (rows are X)
    if transposed:
        a = a.T
    return a, (b.left, b.right, b.bottom, b.top)


def fig_maps():
    fi_adj, ext12 = read_map("freezing_index_adjustment", True)
    fi_base, _ = read_map("freezing_index_baseline", True)
    ti_adj, _ = read_map("thawing_index_adjustment", True)
    ti_base, _ = read_map("thawing_index_baseline", True)
    pr, ext2 = read_map("precipitation_factor", False)
    with np.errstate(invalid="ignore", divide="ignore"):
        fi_pct = 100 * fi_adj / fi_base
        ti_pct = 100 * ti_adj / ti_base
        fi_pct[fi_base < 100] = np.nan  # ignore near-zero baselines
    pr_pct = 100 * (pr - 1)
    panels = [
        (fi_pct, ext12, "Freezing index\nadjustment, % of Daymet baseline", 15),
        (ti_pct, ext12, "Thawing index\nadjustment, % of Daymet baseline", 15),
        (pr_pct, ext2, "Precipitation\nscaling, % (1901–2015 vs 1961–1990)", 15),
    ]
    xs, ys = zip(*[to_3338(s[2], s[3]) for s in SITES])
    fig, axes = plt.subplots(1, 3, figsize=(16, 5.6))
    for ax, (arr, ext, title, lim) in zip(axes, panels):
        im = ax.imshow(arr, extent=ext, cmap=DIVERGING, norm=TwoSlopeNorm(0, -lim, lim), interpolation="nearest")
        ax.scatter(xs, ys, s=10, color=INK, lw=0)
        ax.set_xlim(-1.0e6, 1.55e6)
        ax.set_ylim(0.35e6, 2.45e6)
        ax.set_aspect("equal")
        ax.set_xticks([])
        ax.set_yticks([])
        ax.grid(False)
        for s in ax.spines.values():
            s.set_visible(False)
        ax.set_title(title, loc="left")
        cb = fig.colorbar(im, ax=ax, shrink=0.75, pad=0.01, extend="both")
        cb.outline.set_visible(False)
        cb.set_label("%")
    fig.suptitle("Statewide delta-method adjustments, each computed server-side in a single WCPS request (dots = test sites)",
                 x=0.01, ha="left", fontsize=12, fontweight="semibold", color=INK)
    fig.tight_layout()
    fig.savefig(FIGS / "fig6_wcps_statewide_maps.png", dpi=150, bbox_inches="tight")
    plt.close(fig)


def fig_mmm_example(long):
    """Min/mean/max table the app shows, current vs delta, for a few sites (freezing index)."""
    sites = ["Utqiagvik", "Fairbanks", "Bethel", "Anchorage", "Kodiak", "Juneau"]
    d = long[(long.component == "freezing_index") & (long.method.isin(["current", "delta"]))]
    fig, axes = plt.subplots(1, 3, figsize=(14, 4.2), sharey=True)
    for ax, era in zip(axes, ["2010-2039", "2040-2069", "2070-2099"]):
        for k, (method, color, off) in enumerate([("current", CURRENT, -0.13), ("delta", DELTA, 0.13)]):
            e = d[(d.era == era) & (d.method == method)].pivot(index="site", columns="stat", values="value").reindex(sites)
            y = np.arange(len(sites)) + off
            ax.hlines(y, e["min"], e["max"], color=color, lw=2.5, alpha=0.9, label=f"{'Current' if method == 'current' else 'Delta method'} min–max")
            ax.scatter(e["mean"], y, color=color, s=40, zorder=3, edgecolor=SURFACE, lw=1.2)
        base = d.groupby("site").baseline.first().reindex(sites)
        ax.scatter(base, np.arange(len(sites)), marker="|", s=160, color=INK, lw=1.5, label="Daymet 1980–2009 mean", zorder=4)
        ax.set_title(era, loc="left")
        ax.set_xlabel("°F·days")
        ax.set_yticks(range(len(sites)))
        ax.set_yticklabels(sites)
        ax.set_ylim(len(sites) - 0.5, -0.5)
        ax.grid(axis="y", visible=False)
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper left", bbox_to_anchor=(0.01, 0.92), ncol=3)
    fig.suptitle("Freezing index: the min / mean / max the app reports, current vs delta method (dots = mean)",
                 x=0.01, ha="left", fontsize=12, fontweight="semibold", color=INK, y=0.99)
    fig.tight_layout(rect=(0, 0, 1, 0.84))
    fig.savefig(FIGS / "fig7_freezing_index_mmm.png", dpi=150, bbox_inches="tight")
    plt.close(fig)


if __name__ == "__main__":
    comp = pd.read_csv(DATA / "comparison.csv")
    long = pd.read_csv(DATA / "summary_long.csv")
    extras = json.loads((DATA / "extras.json").read_text())
    fig_dumbbells(comp)
    fig_pp_heatmap(comp)
    fig_temperature_monthly(extras)
    fig_dd_bias(long, extras)
    fig_ratios()
    fig_maps()
    fig_mmm_example(long)
    print("figures written to", FIGS)
