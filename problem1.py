#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Problem 1: Log Level Distribution
---------------------------------
Reads log files from a given input path (local dir, HDFS, or S3 URI),
extracts log levels (INFO/WARN/ERROR/DEBUG),
and writes three outputs into ~/spark-cluster/:

1) problem1_counts.csv   : counts per log level
2) problem1_sample.csv   : random sample log entries with their level
3) problem1_summary.txt  : summary statistics

Run on the cluster as:
uv run python ~/problem1.py spark://$MASTER_PRIVATE_IP:7077 --net-id YOUR-NET-ID --input-dir s3a://<bucket>/data/
"""

import argparse
import os
from pathlib import Path
import pandas as pd
from pyspark.sql import SparkSession, functions as F

LEVELS = ["INFO", "WARN", "ERROR", "DEBUG"]

def get_spark(master_url: str, app_name="Problem1_Cluster"):
    return (
        SparkSession.builder
        .appName(app_name)
        .master(master_url)
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )

def parse_args():
    parser = argparse.ArgumentParser(description="Problem 1: Log Level Distribution")
    parser.add_argument("master_url", help="Spark master URL, e.g. spark://<ip>:7077")
    parser.add_argument("--net-id", required=True, help="Your Net ID, e.g. abc123")
    parser.add_argument("--input-dir", required=True, help="Input path (local dir, HDFS, or S3 URI)")
    parser.add_argument("--sample-size", type=int, default=10, help="Number of random samples (default: 10)")
    parser.add_argument("--seed", type=int, default=42, help="Random seed (default: 42)")
    return parser.parse_args()

def main():
    args = parse_args()
    spark = get_spark(args.master_url)

    try:
        # Input logs (S3, local, or HDFS)
        input_dir = os.path.join(args.input_dir, "*")

        # Output directory on master node
        output_dir = Path.home() / "spark-cluster"
        output_dir.mkdir(parents=True, exist_ok=True)

        # 1) Read raw logs
        df = spark.read.text(input_dir)
        total_lines = df.count()

        # 2) Extract log levels
        pattern = r"\b(INFO|WARN|ERROR|DEBUG)\b"
        df_with_level = df.withColumn("log_level", F.regexp_extract("value", pattern, 1))
        df_levels = df_with_level.filter(F.col("log_level") != "")

        total_with_levels = df_levels.count()
        unique_levels = df_levels.select("log_level").distinct().count()

        # 3) Counts per level
        counts = (
            df_levels.groupBy("log_level").count()
            .toPandas()
            .set_index("log_level")["count"].to_dict()
        )
        counts_out = pd.DataFrame({
            "log_level": LEVELS,
            "count": [counts.get(lvl, 0) for lvl in LEVELS]
        })
        counts_out.to_csv(output_dir / "problem1_counts.csv", index=False)

        # 4) Random sample
        sample_pd = (
            df_levels.select(F.col("value").alias("log_entry"), "log_level")
            .orderBy(F.rand(args.seed))
            .limit(args.sample_size)
            .toPandas()
        )
        sample_pd.to_csv(output_dir / "problem1_sample.csv", index=False)

        # 5) Summary
        summary_lines = [
            f"Net ID: {args.net_id}",
            f"Total log lines processed: {total_lines:,}",
            f"Total lines with log levels: {total_with_levels:,}",
            f"Unique log levels found: {unique_levels}",
            "",
            "Log level distribution:"
        ]
        for lvl in LEVELS:
            cnt = counts.get(lvl, 0)
            pct = 100.0 * cnt / total_with_levels if total_with_levels else 0
            summary_lines.append(f"  {lvl:<5}: {cnt:>12,} ({pct:6.2f}%)")

        with open(output_dir / "problem1_summary.txt", "w", encoding="utf-8") as f:
            f.write("\n".join(summary_lines))

        print("Done. Results written to ~/spark-cluster/")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()