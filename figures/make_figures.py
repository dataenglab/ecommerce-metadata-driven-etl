"""Generate Figures 6-9 of the thesis from the measured results in data/.

Usage:  python make_figures.py
Output: fig6_latency.png ... fig9_cost.png (300 dpi) in this folder.
Requires: pandas, matplotlib.
"""
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib import font_manager
from matplotlib.ticker import FuncFormatter

HERE = Path(__file__).resolve().parent
DATA = HERE / "data"

# Arial if installed, otherwise a metric-compatible fallback
for name in ("Arial", "Liberation Sans", "DejaVu Sans"):
    if any(f.name == name for f in font_manager.fontManager.ttflist):
        plt.rcParams["font.family"] = name
        break
plt.rcParams.update({
    "font.size": 9, "axes.edgecolor": "black", "axes.linewidth": 0.8,
    "axes.spines.top": False, "axes.spines.right": False,
    "savefig.dpi": 300, "savefig.bbox": "tight", "savefig.facecolor": "white",
})
DARK, MID, LIGHT = "#1F4E79", "#5B9BD5", "#D9F1F8"
EDGE = dict(edgecolor="black", linewidth=0.6)
SIZE = (6.3, 3.3)  # inches, about 16 cm wide, same as Fig. 1
thousands = FuncFormatter(lambda v, _: f"{v:,.0f}")


def label_bars(ax, bars, fmt="{:,.0f}"):
    for b in bars:
        ax.annotate(fmt.format(b.get_height()), (b.get_x() + b.get_width() / 2, b.get_height()),
                    xytext=(0, 2), textcoords="offset points", ha="center", va="bottom", fontsize=7.5)


def fig6():
    df = pd.read_csv(DATA / "fig6_latency.csv")
    fig, ax = plt.subplots(figsize=SIZE)
    err = [df.latency_mean_s - df.latency_min_s, df.latency_max_s - df.latency_mean_s]
    ax.errorbar(df.rows_processed, df.latency_mean_s, yerr=err, fmt="o", color=DARK,
                ecolor="black", elinewidth=0.8, capsize=4, markersize=6, zorder=3)
    for _, r in df.iterrows():
        ax.annotate(f"{r.tier}\n{r.latency_mean_s:,.1f} s", (r.rows_processed, r.latency_mean_s),
                    xytext=(8, -4), textcoords="offset points", fontsize=7.5, va="top")
    ax.set_xscale("log"); ax.set_yscale("log")
    ax.set_xlim(1.5e4, 8e6); ax.set_ylim(2, 2000)
    ax.xaxis.set_major_formatter(thousands); ax.yaxis.set_major_formatter(thousands)
    ax.set_xlabel("Rows processed per run (log scale)")
    ax.set_ylabel("End-to-end latency, s (log scale)")
    ax.grid(True, which="major", color="#DDDDDD", linewidth=0.6, zorder=0)
    fig.savefig(HERE / "fig6_latency.png"); plt.close(fig)


def grouped(ax, cats, series, colors, width):
    n = len(series)
    out = []
    for i, ((label, vals), c) in enumerate(zip(series, colors)):
        xs = [x + (i - (n - 1) / 2) * width for x in range(len(cats))]
        bars = ax.bar(xs, vals, width, label=label, color=c, zorder=3, **EDGE)
        out.append(bars)
    ax.set_xticks(range(len(cats))); ax.set_xticklabels(cats)
    ax.grid(True, axis="y", color="#DDDDDD", linewidth=0.6, zorder=0)
    return out


def fig7():
    df = pd.read_csv(DATA / "fig7_throughput.csv")
    fig, ax = plt.subplots(figsize=SIZE)
    bars = grouped(ax, df.tier, [("Order lines", df.orders_rows_s),
                                 ("Clickstream events", df.clickstream_rows_s),
                                 ("Payments", df.payments_rows_s)], [DARK, MID, LIGHT], 0.26)
    for b in bars: label_bars(ax, b)
    ax.yaxis.set_major_formatter(thousands)
    ax.set_ylabel("Throughput, rows/s (mean of 5 runs)")
    ax.set_xlabel("Volume tier")
    ax.set_ylim(0, 15000)
    ax.legend(frameon=False, ncol=3, loc="upper center", bbox_to_anchor=(0.5, 1.12))
    fig.savefig(HERE / "fig7_throughput.png"); plt.close(fig)


def fig8():
    df = pd.read_csv(DATA / "fig8_lines_changed.csv")
    fig, ax = plt.subplots(figsize=SIZE)
    bars = grouped(ax, df.scenario, [("YAML configuration", df.yaml_lines_changed),
                                     ("Python baseline, total", df.python_lines_changed),
                                     ("Python baseline, source-specific", df.python_source_specific_lines)],
                   [LIGHT, DARK, MID], 0.26)
    for b in bars: label_bars(ax, b)
    ax.set_ylabel("Lines added + removed")
    ax.set_xlabel("Change scenario")
    ax.set_ylim(0, 170)
    ax.legend(frameon=False, ncol=3, loc="upper center", bbox_to_anchor=(0.5, 1.12))
    fig.savefig(HERE / "fig8_lines_changed.png"); plt.close(fig)


def fig9():
    df = pd.read_csv(DATA / "fig9_cost.csv")
    fig, ax = plt.subplots(figsize=SIZE)
    labels = ["Profile " + p for p in df.profile]
    bars = grouped(ax, labels, [("Managed stack (Fivetran + Snowflake), USD", df.managed_stack_usd),
                                ("Artifact (virtual server), EUR", df.artefact_server_eur)],
                   [DARK, LIGHT], 0.34)
    label_bars(ax, bars[0], "USD {:,.0f}")
    label_bars(ax, bars[1], "EUR {:,.2f}")
    ax.set_yscale("log"); ax.set_ylim(5, 20000)
    ax.yaxis.set_major_formatter(thousands)
    ax.set_ylabel("Monthly cost, list prices excl. VAT\n(log scale)")
    ax.set_xlabel("Load profile")
    ax.legend(frameon=False, ncol=2, loc="upper center", bbox_to_anchor=(0.5, 1.12))
    fig.savefig(HERE / "fig9_cost.png"); plt.close(fig)


if __name__ == "__main__":
    for f in (fig6, fig7, fig8, fig9):
        f()
    print("Figures written to", HERE)
