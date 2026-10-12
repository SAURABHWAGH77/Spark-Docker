# =====================================================================
# JOB 07: Skew and Partition Counts
# TOPIC : Data skew (one key far more common than the others)
# GOAL  : Build skewed data, DETECT the skew by profiling key
#         frequencies, then look at how Spark plans a groupBy shuffle.
# KEY IDEA: Partition COUNT is not the problem. Uneven partition SIZE is.
# =====================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

# ---------------------------------------------------------------------
# 1. SESSION SETUP
# ---------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("07-Skew-And-Partition-Counts")
    # Number of partitions AFTER a shuffle (default is 200).
    # 8 keeps the plan and Spark UI small and easy to read.
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

# Show only warnings so our prints are not buried in INFO logs
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------
# 2. BUILD SKEWED DATA (10,000 rows, 100 distinct keys)
# ---------------------------------------------------------------------
# Key 0 is deliberately very common:
#   - 8,000 rows with join_key = 0            (80% of all data)
#   - 2,000 rows spread over keys 1..99       (about 20 rows per key)
rows = (
    [(0, i) for i in range(8000)]
    + [(i % 99 + 1, i) for i in range(2000)]
)

# Create a DataFrame from the local Python list
df = spark.createDataFrame(rows, ["join_key", "record_id"])

# ---------------------------------------------------------------------
# 3. DETECT SKEW: frequency profile of the keys
# ---------------------------------------------------------------------
# This is the standard first check when a join/aggregation is slow.
# Expect: key 0 -> 8000, then keys with ~20 rows each.
print("TOP KEYS BY FREQUENCY")
df.groupBy("join_key").agg(
    count("*").alias("row_count")
).orderBy(
    col("row_count").desc()      # biggest keys first
).show(10)

# ---------------------------------------------------------------------
# 4. INPUT PARTITIONS (before any shuffle)
# ---------------------------------------------------------------------
# How many partitions the data was split into at load time.
# Depends on your machine's cores (spark.default.parallelism).
# This is NOT the same as the 8 shuffle partitions configured above.
print("INPUT PARTITIONS:", df.rdd.getNumPartitions())

# ---------------------------------------------------------------------
# 5. PHYSICAL PLAN of the aggregation (nothing runs yet, plan only)
# ---------------------------------------------------------------------
# Read bottom-up. Expect:
#   HashAggregate (final)            <- merges partial counts
#   +- Exchange hashpartitioning(join_key, 8)
#                                    <- SHUFFLE: rows routed by
#                                       hash(join_key) % 8, so ALL rows
#                                       of key 0 go to the SAME partition
#      +- HashAggregate (partial)    <- counts per key inside each input
#                                       partition BEFORE the shuffle
#         +- Scan ExistingRDD
# The partial aggregate shrinks key 0 to a few rows per input partition,
# so groupBy tolerates skew much better than a join does.
print("\nAGGREGATION PHYSICAL PLAN")
df.groupBy("join_key").count().explain("formatted")

# ---------------------------------------------------------------------
# 6. ACTION: run the aggregation and show results
# ---------------------------------------------------------------------
# show() triggers real execution; everything above was lazy.
print("\nAGGREGATED RESULTS")
df.groupBy("join_key").count().show(10)

# Release resources
spark.stop()