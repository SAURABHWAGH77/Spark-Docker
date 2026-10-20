"""Concept 3: coalesce() reduces partitions without a full shuffle by default.

Run:
    python 03_coalesce_basics.py
"""
from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder.appName("03 - Coalesce")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

try:
    df = spark.range(0, 100_000).withColumn(
        "group_id", (F.col("id") % 10).cast("int")
    ).repartition(16)

    reduced = df.coalesce(4)

    print("Before coalesce:", df.rdd.getNumPartitions())
    print("After coalesce:", reduced.rdd.getNumPartitions())

    def show_counts(frame, label):
        counts = frame.rdd.mapPartitions(lambda rows: [sum(1 for _ in rows)]).collect()
        print(f"\\n{label}: {counts}")
        print("Total rows:", sum(counts))

    show_counts(df, "Rows per partition before") 
    show_counts(reduced, "Rows per partition after")

    print("\nExecution plan (look for coalesce and absence of a new full shuffle):")
    reduced.explain("formatted")
    print("\nAction to trigger execution:", reduced.count())
finally:
    spark.stop()
