#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Problem 2: Cluster Usage Analysis
---------------------------------
Analyze Spark log data to extract cluster usage patterns.

Outputs (written to ~/spark-cluster/):
1) problem2_timeline.csv         : Time-series data for each application
2) problem2_cluster_summary.csv  : Aggregated cluster statistics
3) problem2_stats.txt            : Overall summary statistics
4) problem2_bar_chart.png        : Applications per cluster (bar chart)
5) problem2_density_plot.png     : Duration distribution for largest cluster

Usage:
# Full Spark processing (10–20 minutes)
uv run python problem2.py spark://$MASTER_PRIVATE_IP:7077 \
  --net-id YOUR-NET-ID \
  --input-dir s3a://<bucket>/data/

# Skip Spark and regenerate visualizations from existing CSVs (fast)
uv run python problem2.py --skip-spark
"""

import argparse
import os
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
    parser = argparse.ArgumentParser(description="Problem 2: Cluster Usage Analysis")
    parser.add_argument("master_url", nargs="?", help="Spark master URL, e.g. spark://<ip>:7077")
    parser.add_argument("--net-id", help="Your Net ID, e.g. abc123")
    parser.add_argument("--input-dir", help="Input path (S3, HDFS, or local) that contains application_* folders")
    parser.add_argument("--skip-spark", action="store_true", help="Skip Spark and regenerate visualizations from CSVs")
    return parser.parse_args()


# ---------------------------
# Visualization
# ---------------------------
def generate_visualizations(timeline_csv: Path, cluster_summary_csv: Path, output_dir: Path):
    timeline_pd = pd.read_csv(timeline_csv)
    cluster_summary = pd.read_csv(cluster_summary_csv)

    # ---- Bar chart: Applications per cluster
    plt.figure(figsize=(10, 6))
    sns.barplot(
        x="cluster_id",
        y="num_applications",
        hue="cluster_id",            # To allow palette usage legally, without showing legend
        data=cluster_summary,
        palette="Set2",
        legend=False
    )
    plt.title("Applications per Cluster", fontsize=14, weight="bold")
    plt.xticks(rotation=45, ha="right")
    for idx, row in cluster_summary.iterrows():
        plt.text(idx, row["num_applications"] + 0.5, str(row["num_applications"]), ha="center")
    plt.tight_layout()
    plt.savefig(output_dir / "problem2_bar_chart.png")
    plt.close()

    # ---- Density plot: largest cluster
    if not timeline_pd.empty:
        largest_cluster = (
            cluster_summary.sort_values("num_applications", ascending=False).iloc[0]["cluster_id"]
        )

        subset = timeline_pd[timeline_pd["cluster_id"] == largest_cluster].copy()
        subset["start_time"] = pd.to_datetime(subset["start_time"], errors="coerce")
        subset["end_time"]   = pd.to_datetime(subset["end_time"], errors="coerce")
        subset["duration"]   = (subset["end_time"] - subset["start_time"]).dt.total_seconds()

        dur = subset["duration"].dropna()
        dur = dur[dur > 0]

        plt.figure(figsize=(10, 6))

        if len(dur) > 0:
            # On linear axis: plot log10(duration) histogram + KDE
            log_dur = np.log10(dur)

            sns.histplot(log_dur, bins=30, color="skyblue", alpha=0.6, stat="count")
            if len(log_dur) > 1:
                sns.kdeplot(log_dur, color="red", lw=2)

            # Label x-axis ticks as seconds (10^n)
            ticks = np.arange(np.floor(log_dur.min()), np.ceil(log_dur.max()) + 1)
            plt.xticks(ticks, [f"{int(10**t):,}" for t in ticks])

            plt.xlabel("Job Duration (seconds, log scale)")
        else:
            # If no valid durations, show empty plot with message
            plt.text(0.5, 0.5, "No valid durations to plot", ha="center", va="center", fontsize=12)
            plt.xticks([])
            plt.yticks([])

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

    # Fast mode: regenerate plots only from existing CSVs
    if args.skip_spark:
        generate_visualizations(timeline_csv, cluster_summary_csv, output_dir)
        print("Visualizations regenerated from existing CSVs.")
        return

    if not args.master_url or not args.net_id or not args.input_dir:
        raise ValueError("Must provide master_url, --net-id, and --input-dir unless using --skip-spark.")

    spark = get_spark(args.master_url)

    try:
        # Read logs (supports S3/HDFS/local), grab all .log files under application_* folders
        input_glob = os.path.join(args.input_dir.rstrip("/"), "application_*/*.log")
        df = spark.read.text(input_glob)

        # Extract application_id / cluster_id
        df_app = (
            df.withColumn("application_id", F.regexp_extract("value", r"(application_\d+_\d+)", 1))
              .filter(F.col("application_id") != "")
        )
        df_app = df_app.withColumn("cluster_id", F.regexp_extract("application_id", r"application_(\d+)_\d+", 1))

        # Allow two timestamp formats: 17/03/29 10:04:41  or  2017-03-29 10:04:41
        df_app = df_app.withColumn(
            "timestamp_str",
            F.regexp_extract(
                "value",
                r"(^\d{2}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2}|^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})",
                0,
            ),
        )

        # Try parsing with two formats; keep whichever succeeds
        parsed1 = F.to_timestamp(when(col("timestamp_str") != "", col("timestamp_str")), "yy/MM/dd HH:mm:ss")
        parsed2 = F.to_timestamp(when(col("timestamp_str") != "", col("timestamp_str")), "yyyy-MM-dd HH:mm:ss")
        df_app = df_app.withColumn("parsed_time", F.coalesce(parsed1, parsed2))
        # Note: do not filter out rows without parsed_time, to avoid dropping applications

        # Compute start_time / end_time for each application (keep apps even if times are missing)
        df_times = (
            df_app.groupBy("cluster_id", "application_id")
            .agg(
                F.min("parsed_time").alias("start_time"),
                F.max("parsed_time").alias("end_time"),
            )
        )

        # Extract app_number
        df_times = df_times.withColumn("app_number", F.regexp_extract("application_id", r"_(\d+)$", 1))

        # Save timeline: applications with no times will have empty values
        df_times = df_times.select("cluster_id", "application_id", "app_number", "start_time", "end_time")
        timeline_pd = df_times.toPandas()
        # Standardize timestamp format (empty values remain blank)
        timeline_pd["start_time"] = pd.to_datetime(timeline_pd["start_time"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
        timeline_pd["end_time"]   = pd.to_datetime(timeline_pd["end_time"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
        timeline_pd = timeline_pd.fillna("")  # Keep empty times as blank
        timeline_pd.to_csv(timeline_csv, index=False)

        # Aggregate cluster summary: application count is based on unique application_id (ensures no loss)
        base_counts = (
            timeline_pd.groupby("cluster_id")["application_id"].nunique().rename("num_applications").reset_index()
        )
        # Time range (ignore empty strings)
        tcopy = timeline_pd.replace({"": pd.NaT})
        tcopy["start_time"] = pd.to_datetime(tcopy["start_time"], errors="coerce")
        tcopy["end_time"]   = pd.to_datetime(tcopy["end_time"], errors="coerce")
        mins = tcopy.groupby("cluster_id")["start_time"].min().rename("cluster_first_app")
        maxs = tcopy.groupby("cluster_id")["end_time"].max().rename("cluster_last_app")
        cluster_summary = base_counts.merge(mins, on="cluster_id", how="left").merge(maxs, on="cluster_id", how="left")
        # Format output
        cluster_summary["cluster_first_app"] = cluster_summary["cluster_first_app"].dt.strftime("%Y-%m-%d %H:%M:%S")
        cluster_summary["cluster_last_app"]  = cluster_summary["cluster_last_app"].dt.strftime("%Y-%m-%d %H:%M:%S")
        cluster_summary = cluster_summary.fillna("")
        cluster_summary.to_csv(cluster_summary_csv, index=False)

        # Stats text
        total_clusters = cluster_summary.shape[0]
        total_apps = int(base_counts["num_applications"].sum())
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

        # Generate plots
        generate_visualizations(timeline_csv, cluster_summary_csv, output_dir)

        print("✅ Problem 2 complete. Results written to ~/spark-cluster/")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()