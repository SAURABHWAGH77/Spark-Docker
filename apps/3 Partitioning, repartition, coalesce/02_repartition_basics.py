"""Concept 2: repartition() and shuffle.

Run:
    python 02_repartition_basics.py
    python 02_repartition_basics.py --partitions 12
"""
import argparse
from pyspark.sql import SparkSession, functions as F

# --- CLI: target number of partitions (default 12) ---
parser = argparse.ArgumentParser()
parser.add_argument("--partitions", type=int, default=12)
args = parser.parse_args()
if args.partitions < 1:
    parser.error("--partitions must be at least 1")

# --- Start Spark session ---
spark = (
    SparkSession.builder.appName("02 - Repartition")
    .getOrCreate()
)

# Show only warnings/errors to keep output clean
spark.sparkContext.setLogLevel("WARN")

try:
    # Test data: 100,000 rows; customer_id cycles 0-99 (100 customers x 1,000 rows)
    df = spark.range(0, 100_000).withColumn(
        "customer_id", (F.col("id") % 100).cast("int")
    )

    # Partition count Spark picked by default
    print("Before repartition:", df.rdd.getNumPartitions())

    # repartition(n): lazy, wide transformation -> full shuffle, round-robin into n partitions
    repartitioned = df.repartition(args.partitions)
    print("After repartition:", repartitioned.rdd.getNumPartitions())

    # Plan shows "Exchange RoundRobinPartitioning(n)" = the shuffle
    print("\nExecution plan (look for Exchange / shuffle):")
    repartitioned.explain("formatted")

    # count() is an action -> actually runs the job (check Spark UI at localhost:4040)
    print("\nAction to trigger execution:", repartitioned.count())
    print("\nTry --partitions 4 and --partitions 24, then compare Spark UI stages.")
finally:
    # Always release Spark resources, even on error
    spark.stop()