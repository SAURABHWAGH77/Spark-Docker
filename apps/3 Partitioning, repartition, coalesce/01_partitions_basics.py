
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("01-Partitions-Basics")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

try:
    ROWS = 1_000_000
    PARTITIONS = 16

    df = spark.range(
        0, ROWS, 1,
        numPartitions=PARTITIONS
    )

    print("\n===== PARTITION BASICS =====")
    print("Total rows:", df.count())
    print("Partition count:", df.rdd.getNumPartitions())

    partition_sizes = (
        df.rdd
        .mapPartitions(lambda rows: [sum(1 for _ in rows)])
        .collect()
    )

    print("Rows per partition:", partition_sizes)
    print("Minimum rows:", min(partition_sizes))
    print("Maximum rows:", max(partition_sizes))
    print("Total rows across partitions:", sum(partition_sizes))

finally:
    spark.stop()
