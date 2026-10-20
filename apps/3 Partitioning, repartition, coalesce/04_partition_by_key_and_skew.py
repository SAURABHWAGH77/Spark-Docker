"""Concept 4: hash partitioning by key and data skew.

Run:
    python 04_partition_by_key_and_skew.py
"""
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("04 - Key Partitioning and Skew")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

try:
    # Key 0 is intentionally much more frequent than the other keys.
    rows = (
        [(0, i) for i in range(60_000)]
        + [(1, i) for i in range(10_000)]
        + [(2, i) for i in range(10_000)]
        + [(3, i) for i in range(10_000)]
        + [(4, i) for i in range(10_000)]
    )
    df = spark.createDataFrame(rows, ["key", "value"])

    def inspect(frame, label):
        counts = frame.rdd.mapPartitions(lambda it: [sum(1 for _ in it)]).collect()
        print(f"\\n{label}")
        print("Partition count:", frame.rdd.getNumPartitions())
        print("Rows per partition:", counts)
        print("Total rows:", sum(counts))

    inspect(df, "Original DataFrame")
    inspect(df.repartition(8), "repartition(8), no key specified")
    by_key = df.repartition(8, "key")
    inspect(by_key, "repartition(8, 'key')")
    by_key.explain("formatted")
    print("\nTotal rows:", by_key.count())

    print("\nLesson: rows with the same key are grouped into the same hash partition.")
    print("A hot key can make one partition much larger; repartitioning by key does not remove skew.")
finally:
    input("Press Enter to exit...")  # keeps the UI alive at localhost:4040
    spark.stop()
