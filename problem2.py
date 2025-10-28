#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Problem 2: Cluster Usage Analysis (Final)
- Ensures application counts match official (181 / 194).
- Uses aws s3 ls (or local glob) to enumerate all application_* folders.
- Pads missing apps into timeline, but density plot only uses apps with timestamps.
Outputs (to ~/spark-cluster):
  problem2_timeline.csv
  problem2_cluster_summary.csv
  problem2_stats.txt
  problem2_bar_chart.png
  problem2_density_plot.png
"""

import argparse, os, re, subprocess
from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, when

# ---------------------------
# Spark helpers
# ---------------------------
def get_spark(master_url: str, app_name="Problem2_ClusterUsage"):
    return (
        SparkSession.builder
        .appName(app_name)
        .master(master_url)
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )

def parse_args():
    p = argparse.ArgumentParser(description="Problem 2: Cluster Usage Analysis")
    p.add_argument("master_url", nargs="?", help="Spark master URL, e.g. spark://<ip>:7077")
    p.add_argument("--net-id", required=False, help="Your Net ID, e.g. abc123")
    p.add_argument("--input-dir", required=False, help="Input path containing application_* folders (S3/HDFS/local)")
    p.add_argument("--skip-spark", action="store_true", help="Skip Spark; only regenerate plots from CSVs")
    return p.parse_args()

# ---------------------------
# List ALL application_* folders
# ---------------------------
_APP_RE = re.compile(r"application_(\d+)_(\d+)/?$")

def list_all_app_folders(input_dir: str):
    out = []
    if input_dir.startswith("s3a://") or input_dir.startswith("s3://"):
        s3_uri = input_dir.replace("s3a://", "s3://").rstrip("/") + "/"
        cmd = ["aws", "s3", "ls", s3_uri]
        res = subprocess.run(cmd, capture_output=True, text=True, check=True)
        for line in res.stdout.splitlines():
            name = line.split()[-1] if line.strip() else ""
            m = _APP_RE.search(name)
            if m:
                cluster_id, app_no = m.group(1), m.group(2)
                out.append((cluster_id, f"application_{cluster_id}_{app_no}", app_no))
    else:
        for p in Path(input_dir).glob("application_*"):
            m = _APP_RE.fullmatch(p.name)
            if m:
                cluster_id, app_no = m.group(1), m.group(2)
                out.append((cluster_id, f"application_{cluster_id}_{app_no}", app_no))
    seen, deduped = set(), []
    for t in out:
        if t[1] not in seen:
            seen.add(t[1])
            deduped.append(t)
    return deduped

# ---------------------------
# Visualization
# ---------------------------
def generate_visualizations(timeline_csv: Path, cluster_summary_csv: Path, output_dir: Path):
    timeline_pd = pd.read_csv(timeline_csv)
    cluster_summary = pd.read_csv(cluster_summary_csv)

    # Bar chart
    plt.figure(figsize=(10, 6))
    sns.barplot(
        x="cluster_id", y="num_applications",
        hue="cluster_id", data=cluster_summary,
        palette="Set2", legend=False
    )
    plt.title("Applications per Cluster", fontsize=14, weight="bold")
    plt.xticks(rotation=45, ha="right")
    for idx, row in cluster_summary.iterrows():
        plt.text(idx, row["num_applications"] + 0.5, str(row["num_applications"]), ha="center")
    plt.tight_layout()
    plt.savefig(output_dir / "problem2_bar_chart.png")
    plt.close()

    # Density plot (only real apps with timestamps)
    if not timeline_pd.empty and not cluster_summary.empty:
        valid_apps = timeline_pd.copy()
        valid_apps["has_time"] = valid_apps["start_time"].astype(str).str.len() > 0
        valid_counts = valid_apps[valid_apps["has_time"]].groupby("cluster_id")["application_id"].nunique()
        if not valid_counts.empty:
            largest_cluster = valid_counts.sort_values(ascending=False).index[0]
        else:
            largest_cluster = cluster_summary.sort_values("num_applications", ascending=False).iloc[0]["cluster_id"]

        subset = timeline_pd[timeline_pd["cluster_id"] == str(largest_cluster)].copy()
        subset["start_time"] = pd.to_datetime(subset["start_time"], errors="coerce")
        subset["end_time"]   = pd.to_datetime(subset["end_time"], errors="coerce")
        subset["duration"]   = (subset["end_time"] - subset["start_time"]).dt.total_seconds()
        dur = subset["duration"].dropna()
        dur = dur[dur > 0]

        plt.figure(figsize=(10, 6))
        if len(dur) > 0:
            log_dur = np.log10(dur)
            sns.histplot(log_dur, bins=30, color="skyblue", alpha=0.6, stat="count")
            if len(log_dur) > 1:
                sns.kdeplot(log_dur, color="red", lw=2)
            ticks = np.arange(np.floor(log_dur.min()), np.ceil(log_dur.max()) + 1)
            plt.xticks(ticks, [f"{int(10**t):,}" for t in ticks])
            plt.xlabel("Job Duration (seconds, log scale)")
        else:
            plt.text(0.5, 0.5, "No valid durations to plot", ha="center", va="center", fontsize=12)
            plt.xticks([]); plt.yticks([])

        plt.title(f"Duration distribution for Cluster {largest_cluster} (n={len(dur)})")
        plt.ylabel("Count")
        plt.tight_layout()
        plt.savefig(output_dir / "problem2_density_plot.png")
        plt.close()

# ---------------------------
# Main
# ---------------------------
def main():
    args = parse_args()
    output_dir = Path.home() / "spark-cluster"
    output_dir.mkdir(parents=True, exist_ok=True)

    timeline_csv = output_dir / "problem2_timeline.csv"
    cluster_summary_csv = output_dir / "problem2_cluster_summary.csv"
    stats_txt = output_dir / "problem2_stats.txt"

    if args.skip_spark:
        generate_visualizations(timeline_csv, cluster_summary_csv, output_dir)
        print("Visualizations regenerated from existing CSVs.")
        return

    if not args.master_url or not args.net_id or not args.input_dir:
        raise ValueError("Must provide master_url, --net-id, and --input-dir unless using --skip-spark.")

    all_apps = list_all_app_folders(args.input_dir)
    apps_df = pd.DataFrame(all_apps, columns=["cluster_id", "application_id", "app_number"])

    spark = get_spark(args.master_url)
    try:
        input_glob = os.path.join(args.input_dir.rstrip("/"), "application_*/*.log")
        df = spark.read.text(input_glob)

        df_app = (
            df.withColumn("application_id", F.regexp_extract("value", r"(application_\d+_\d+)", 1))
              .filter(F.col("application_id") != "")
        )
        df_app = df_app.withColumn("cluster_id", F.regexp_extract("application_id", r"application_(\d+)_\d+", 1))
        df_app = df_app.withColumn(
            "timestamp_str",
            F.regexp_extract("value",
                r"(^\d{2}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2}|^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})", 0)
        )
        parsed1 = F.to_timestamp(when(col("timestamp_str") != "", col("timestamp_str")), "yy/MM/dd HH:mm:ss")
        parsed2 = F.to_timestamp(when(col("timestamp_str") != "", col("timestamp_str")), "yyyy-MM-dd HH:mm:ss")
        df_app = df_app.withColumn("parsed_time", F.coalesce(parsed1, parsed2))

        df_times = df_app.groupBy("cluster_id", "application_id") \
                         .agg(F.min("parsed_time").alias("start_time"),
                              F.max("parsed_time").alias("end_time"))

        timeline_pd = df_times.toPandas()
        have = set(timeline_pd["application_id"]) if not timeline_pd.empty else set()

        missing_rows = []
        for cid, aid, anum in all_apps:
            if aid not in have:
                missing_rows.append({"cluster_id": cid,
                                     "application_id": aid,
                                     "start_time": "",
                                     "end_time": ""})
        if missing_rows:
            timeline_pd = pd.concat([timeline_pd, pd.DataFrame(missing_rows)], ignore_index=True)

        timeline_pd["start_time"] = pd.to_datetime(timeline_pd["start_time"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
        timeline_pd["end_time"]   = pd.to_datetime(timeline_pd["end_time"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
        timeline_pd = timeline_pd.fillna("")
        timeline_pd.to_csv(timeline_csv, index=False)

        counts_full = apps_df.groupby("cluster_id")["application_id"].nunique().reset_index()
        counts_full = counts_full.rename(columns={"application_id":"num_applications"})

        tcopy = timeline_pd.replace({"": pd.NaT})
        tcopy["start_time"] = pd.to_datetime(tcopy["start_time"], errors="coerce")
        tcopy["end_time"]   = pd.to_datetime(tcopy["end_time"], errors="coerce")
        mins = tcopy.groupby("cluster_id")["start_time"].min().rename("cluster_first_app")
        maxs = tcopy.groupby("cluster_id")["end_time"].max().rename("cluster_last_app")

        cluster_summary = counts_full.merge(mins, on="cluster_id", how="left").merge(maxs, on="cluster_id", how="left")
        cluster_summary["cluster_first_app"] = cluster_summary["cluster_first_app"].dt.strftime("%Y-%m-%d %H:%M:%S")
        cluster_summary["cluster_last_app"]  = cluster_summary["cluster_last_app"].dt.strftime("%Y-%m-%d %H:%M:%S")
        cluster_summary = cluster_summary.fillna("")
        cluster_summary.to_csv(cluster_summary_csv, index=False)

        total_clusters = cluster_summary.shape[0]
        total_apps = int(counts_full["num_applications"].sum())
        avg_apps = total_apps / total_clusters if total_clusters > 0 else 0.0

        stats_lines = [
            f"Net ID: {args.net_id}",
            f"Total unique clusters: {total_clusters}",
            f"Total applications: {total_apps}",
            f"Average applications per cluster: {avg_apps:.2f}",
            "",
            "Most heavily used clusters:",
        ]
        for _, row in cluster_summary.sort_values("num_applications", ascending=False).head(5).iterrows():
            stats_lines.append(f"  Cluster {row['cluster_id']}: {row['num_applications']} applications")

        with open(stats_txt, "w", encoding="utf-8") as f:
            f.write("\n".join(stats_lines))

        generate_visualizations(timeline_csv, cluster_summary_csv, output_dir)
        print("✅ Problem 2 complete. Results written to ~/spark-cluster/")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()