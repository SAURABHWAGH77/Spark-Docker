"""Concept 5: compare output files before and after coalesce().

Run:
    python 05_output_files_coalesce.py
Optional output location:
    python 05_output_files_coalesce.py --output-dir ./partitioning_output
"""
import argparse
from pathlib import Path
from pyspark.sql import SparkSession, functions as F

parser = argparse.ArgumentParser()
parser.add_argument("--output-dir", default="./partitioning_output")
args = parser.parse_args()

root = Path(args.output_dir).resolve()
many_path = root / "many_partitions"
few_path = root / "coalesced"

spark = (
    SparkSession.builder.appName("05 - Output Files")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

try:
    df = spark.range(0, 50_000).withColumn(
        "category", (F.col("id") % 20).cast("int")
    ).repartition(8)

    df.write.mode("overwrite").parquet(str(many_path))
    df.coalesce(2).write.mode("overwrite").parquet(str(few_path))

    def part_files(folder):
        return [p for p in folder.rglob("part-*") if p.is_file()]

    print("Output directory:", root)
    print("Files from 8 partitions:", len(part_files(many_path)))
    print("Files after coalesce(2):", len(part_files(few_path)))
    print("\\nInspect the directories and compare file sizes.")
    print("Note: partitionBy(), empty partitions, and writer behavior can affect file counts.")
finally:
    spark.stop()
