#!/usr/bin/env python3
"""Plot trace benchmark results comparing UniCache, RepliCache, and Raft."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.ticker import MaxNLocator

GB = 1_000_000_000.0

MODES = ["Raft", "UniCache", "RepliCache"]
MODE_COLORS = {
    "Raft": "#2ca02c",
    "UniCache": "#1f77b4",
    "RepliCache": "#ff7f0e",
}


def detect_leader(df: pd.DataFrame) -> int:
    """The leader is the node with the highest sent_bytes."""
    return int(df.loc[df["sent_bytes"].idxmax(), "node_id"])


def role_labels_for_nodes(node_ids: List[int], leader_id: int) -> Dict[int, str]:
    followers = sorted([n for n in node_ids if n != leader_id])
    follower_names = ["Follower A", "Follower B", "Follower C", "Follower D"]
    mapping = {leader_id: "Leader"}
    for i, n in enumerate(followers):
        mapping[n] = follower_names[i] if i < len(follower_names) else f"Follower {i+1}"
    return mapping


def load_latest(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)

    required_cols = ["node_id", "sent_bytes"]
    for c in required_cols:
        if c not in df.columns:
            raise ValueError(f"{csv_path} missing required column: {c}")

    df["node_id"] = pd.to_numeric(df["node_id"], errors="coerce").astype("Int64")
    df["sent_bytes"] = pd.to_numeric(df["sent_bytes"], errors="coerce")
    df = df.dropna(subset=["node_id"]).copy()
    df["node_id"] = df["node_id"].astype(int)

    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        df = df.sort_values(by="timestamp", kind="mergesort").drop_duplicates(
            subset=["node_id"], keep="last"
        )

    return df[["node_id", "sent_bytes"]].reset_index(drop=True)


def assemble_table(
    mode_dfs: Dict[str, pd.DataFrame],
) -> Tuple[pd.DataFrame, List[str]]:
    node_ids = set()
    for df in mode_dfs.values():
        if not df.empty:
            node_ids.update(df["node_id"].unique().tolist())
    node_ids = sorted(node_ids)

    if not node_ids:
        return pd.DataFrame(), []

    # Detect actual leader per run
    leader_ids = {}
    mode_label_maps = {}
    for mode_name, df in mode_dfs.items():
        if not df.empty:
            leader_ids[mode_name] = detect_leader(df)
            mode_label_maps[mode_name] = role_labels_for_nodes(
                node_ids, leader_ids[mode_name]
            )

    cluster_label = "Cluster"
    followers_total_label = "Followers"

    node_order = [cluster_label, followers_total_label,
                  "Leader", "Follower A", "Follower B", "Follower C", "Follower D"]

    rows = []
    for mode_name, df in mode_dfs.items():
        if df.empty:
            continue

        lid = leader_ids[mode_name]
        label_map = mode_label_maps[mode_name]
        temp_df = df.set_index("node_id")

        for n in node_ids:
            sent = float(temp_df["sent_bytes"].get(n, 0.0))
            rows.append({
                "mode": mode_name,
                "node_id": n,
                "node_label": label_map[n],
                "sent_gb": sent / GB,
            })

        total_sent = df["sent_bytes"].sum()
        rows.append({
            "mode": mode_name,
            "node_id": -1,
            "node_label": cluster_label,
            "sent_gb": total_sent / GB,
        })

        followers_sent = df[df["node_id"] != lid]["sent_bytes"].sum()
        rows.append({
            "mode": mode_name,
            "node_id": -2,
            "node_label": followers_total_label,
            "sent_gb": followers_sent / GB,
        })

    tidy = pd.DataFrame(rows)
    if tidy.empty:
        return tidy, node_order

    present_modes = [m for m in MODES if m in tidy["mode"].unique()]
    tidy["mode"] = pd.Categorical(
        tidy["mode"], categories=present_modes, ordered=True
    )
    tidy["node_label"] = pd.Categorical(
        tidy["node_label"], categories=node_order, ordered=True
    )
    tidy = tidy.sort_values(["node_label", "mode"]).reset_index(drop=True)
    return tidy, node_order


def plot_trace(
    tidy: pd.DataFrame,
    node_order: List[str],
    outpath: Path,
    include_labels: List[str],
    ymax_gb: float | None,
    title: str,
) -> None:
    plot_order = [x for x in node_order if x in include_labels]
    if tidy.empty or not plot_order:
        print("No data to plot.")
        return

    present_modes = [m for m in MODES if m in tidy["mode"].unique()]
    num_modes = len(present_modes)
    width = 0.6
    group_width = num_modes * width

    fig, ax = plt.subplots(figsize=(10, 5))

    heights = {m: [] for m in present_modes}
    xticks = []
    xtick_labels = []

    base = 0.0
    for node_lab in plot_order:
        center = base
        for mode_name in present_modes:
            row = tidy[(tidy["mode"] == mode_name) & (tidy["node_label"] == node_lab)]
            val = row["sent_gb"].values[0] if not row.empty else 0.0
            heights[mode_name].append(val)

        xticks.append(center)
        xtick_labels.append(node_lab)
        base += group_width + 1.5

    for i, mode_name in enumerate(present_modes):
        offset = (i - (num_modes - 1) / 2) * width
        xs = [x + offset for x in xticks]
        ax.bar(
            xs, heights[mode_name], width=width,
            label=mode_name, color=MODE_COLORS[mode_name],
            edgecolor="black", linewidth=1.0,
        )

    ax.set_title(title, loc="left", fontweight="bold", fontsize=14)
    ax.set_ylabel("GB Sent", fontsize=12)
    ax.yaxis.set_major_locator(MaxNLocator(integer=True))
    ax.set_xticks(xticks)
    ax.set_xticklabels(xtick_labels, rotation=0, ha="center", fontsize=11)
    ax.grid(True, axis="y", linestyle="-", alpha=0.5)
    ax.set_axisbelow(True)

    if ymax_gb:
        ax.set_ylim(0, ymax_gb)
    else:
        ax.set_ylim(bottom=0)

    ax.legend(
        loc="lower right", bbox_to_anchor=(1, 1.05), ncol=num_modes,
        frameon=False, fontsize=12,
    )

    plt.tight_layout()
    fig.savefig(outpath, dpi=300)
    print(f"Wrote {outpath}")


def main() -> None:
    p = argparse.ArgumentParser(
        description="Plot trace benchmark results.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--leader-root", type=Path, default=Path("results_summary_trace_leader_encode"))
    p.add_argument("--any-root", type=Path, default=Path("results_summary_trace_any_node"))
    p.add_argument("--raft-root", type=Path, default=None)
    p.add_argument("--ymax-gb", type=float, default=None)
    p.add_argument("--out-prefix", type=str, default="trace_comparison")
    args = p.parse_args()

    mode_dfs: Dict[str, pd.DataFrame] = {}

    # Load UniCache (leader-encode)
    leader_csv = args.leader_root / "_summary" / "nft_bytes_per_node.csv"
    if leader_csv.exists():
        mode_dfs["UniCache"] = load_latest(leader_csv)
    else:
        print(f"WARNING: UniCache CSV not found: {leader_csv}", file=sys.stderr)

    # Load RepliCache (any-node)
    any_csv = args.any_root / "_summary" / "nft_bytes_per_node.csv"
    if any_csv.exists():
        mode_dfs["RepliCache"] = load_latest(any_csv)
    else:
        print(f"WARNING: RepliCache CSV not found: {any_csv}", file=sys.stderr)

    # Load Raft baseline (optional)
    if args.raft_root:
        raft_csv = args.raft_root / "_summary" / "nft_bytes_per_node.csv"
        if raft_csv.exists():
            mode_dfs["Raft"] = load_latest(raft_csv)
        else:
            print(f"WARNING: Raft CSV not found: {raft_csv}", file=sys.stderr)

    if not mode_dfs:
        print("ERROR: No data found.", file=sys.stderr)
        sys.exit(1)

    tidy, node_order = assemble_table(mode_dfs)

    plot_trace(
        tidy, node_order,
        Path(f"{args.out_prefix}_aggregated.png"),
        include_labels=["Cluster", "Leader", "Followers"],
        ymax_gb=args.ymax_gb,
        title="Trace Workload: Peer Traffic",
    )

    plot_trace(
        tidy, node_order,
        Path(f"{args.out_prefix}_followers.png"),
        include_labels=["Follower A", "Follower B", "Follower C", "Follower D"],
        ymax_gb=args.ymax_gb,
        title="Trace Workload: Follower Breakdown",
    )


if __name__ == "__main__":
    main()
