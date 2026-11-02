#!/usr/bin/env -S uv run --with matplotlib --with pandas --with numpy
# /// script
# requires-python = ">=3.10"
# dependencies = ["matplotlib", "pandas", "numpy"]
# ///

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import pathlib
from matplotlib.patches import Patch

scores_path = pathlib.Path(__file__).parent / "scores.txt"
df = pd.read_csv(scores_path)

# ── colour / hatch scheme ──
family_colors = {
    "PLN-Revision": "#4C72B0",
    "PLN-NoisyOr":  "#55A868",
    "ProbLog":      "#C44E52",
}
strength_hatches = {"ST": "", "Conf": "//", "Exp": "xx"}

def split_method(m):
    if m == "ProbLog":
        return "ProbLog", ""
    parts = m.rsplit("-", 1)
    return parts[0], parts[1]

df[["Family", "Strength"]] = df["Method"].apply(lambda m: pd.Series(split_method(m)))

# ── order: group by family, then by strength within family ──
strength_order = ["ST", "Conf", "Exp", ""]
family_order = ["PLN-Revision", "PLN-NoisyOr", "ProbLog"]
order = []
for fam in family_order:
    for st in strength_order:
        name = f"{fam}-{st}" if st else fam
        if name in df["Method"].values:
            order.append(name)

summary = df.groupby("Method")["AUC"].agg(["mean", "std"]).loc[order]
problog_mean = summary.loc["ProbLog", "mean"]

# ── build display rows: headers (no bar) + data bars ──
# Each row is either a header or a bar
rows = []  # list of dicts
prev_fam = None
for method in summary.index:
    fam, st = split_method(method)
    if fam != prev_fam:
        rows.append({"type": "header", "family": fam})
        prev_fam = fam
    sub_label = {"ST": "Strength Only", "Conf": "Confidence Only",
                 "Exp": "ST × Conf", "": ""}[st]
    rows.append({"type": "bar", "method": method, "family": fam,
                 "strength": st, "label": sub_label})

y_coords = np.arange(len(rows))
bar_height = 0.55

# ── plot ──
fig, ax = plt.subplots(figsize=(12, 6.5))
fig.patch.set_facecolor("white")
ax.set_facecolor("#F7F7F7")

# draw bars only for bar rows
for i, row in enumerate(rows):
    if row["type"] != "bar":
        continue
    method = row["method"]
    fam = row["family"]
    st = row["strength"]
    color = family_colors[fam]
    hatch = strength_hatches.get(st, "")
    ax.barh(
        i, summary.loc[method, "mean"],
        xerr=summary.loc[method, "std"],
        height=bar_height, color=color, hatch=hatch,
        edgecolor="white", linewidth=1.2,
        capsize=3, error_kw={"elinewidth": 1.2, "capthick": 1.2, "color": "#333"},
        alpha=0.85,
    )

# ── overlay individual fold scores ──
rng = np.random.default_rng(42)
for i, row in enumerate(rows):
    if row["type"] != "bar":
        continue
    method = row["method"]
    fold_vals = df.loc[df["Method"] == method, "AUC"].values
    jitter = rng.uniform(-0.08, 0.08, size=len(fold_vals))
    ax.scatter(
        fold_vals, i + jitter,
        color="white", edgecolor="#333", s=28, zorder=5, linewidth=0.8,
    )

# ── ProbLog reference line ──
ax.axvline(problog_mean, color=family_colors["ProbLog"], linestyle="--",
           linewidth=1.4, alpha=0.5, zorder=0)

# ── value labels to the RIGHT, past all dots and error bars ──
for i, row in enumerate(rows):
    if row["type"] != "bar":
        continue
    method = row["method"]
    m = summary.loc[method, "mean"]
    s = summary.loc[method, "std"]
    fold_vals = df.loc[df["Method"] == method, "AUC"].values
    right_edge = max(m + s, fold_vals.max(), 0.55)
    ax.text(right_edge + 0.012, i, f"{m:.3f}", va="center", ha="left",
            fontsize=10.5, fontweight="bold", color="#333")

# ── y-axis labels ──
tick_positions = []
tick_labels = []
for i, row in enumerate(rows):
    tick_positions.append(i)
    if row["type"] == "header":
        display = {"PLN-Revision": "PLN Revision", "PLN-NoisyOr": "PLN NoisyOr",
                    "ProbLog": "ProbLog"}[row["family"]]
        tick_labels.append(display)
    else:
        tick_labels.append("    " + row["label"])  # indent sub-labels

ax.set_yticks(tick_positions)
ax.set_yticklabels(tick_labels, fontsize=11)
ax.tick_params(axis="y", pad=10)
ax.invert_yaxis()

# bold + color the header labels
for i, row in enumerate(rows):
    if row["type"] == "header":
        label = ax.get_yticklabels()[i]
        label.set_fontweight("bold")
        label.set_color(family_colors[row["family"]])
        label.set_fontsize(12)

# ── axes styling ──
ax.set_xlabel("AUC", fontsize=14, labelpad=8)
ax.set_title("Method Comparison – 5-Fold Cross-Validation AUC",
             fontsize=16, weight="bold", pad=14)
lo = min((summary["mean"] - summary["std"]).min(), df["AUC"].min()) - 0.03
hi = max((summary["mean"] + summary["std"]).max(), df["AUC"].max()) + 0.05
ax.set_xlim(lo, hi)
ax.xaxis.set_major_locator(plt.MultipleLocator(0.05))
ax.grid(axis="x", linestyle="--", alpha=0.4, color="#BBB")

for spine in ["top", "right"]:
    ax.spines[spine].set_visible(False)
ax.spines["left"].set_color("#CCC")
ax.spines["bottom"].set_color("#CCC")
ax.tick_params(axis="both", which="both", length=0)

# ── legend ──
legend_items = []
for st, hatch in strength_hatches.items():
    nice = {"ST": "Strength Only", "Conf": "Confidence Only", "Exp": "ST × Conf"}
    legend_items.append(Patch(facecolor="#999", edgecolor="white", hatch=hatch,
                              label=nice[st], alpha=0.85))
ax.legend(handles=legend_items, title="Pred", loc="upper right",
          fontsize=10, title_fontsize=10, frameon=True, fancybox=True, framealpha=0.9)

plt.tight_layout()

out = pathlib.Path(__file__).parent / "scores_comparison.png"
fig.savefig(out, dpi=200, facecolor="white", bbox_inches="tight")
print(f"Saved to {out}")
