# TOPIC: Sort-Merge Join (the default strategy for two LARGE tables)
# LESSON: Force it with hint("merge"); plan shows shuffle + sort on both sides
from pyspark.sql import SparkSession
spark = (
    SparkSession.builder
    .appName("05-Sort-Merge-Join")
    # AQE OFF: plan stays static, so what explain() shows is what runs
    # (AQE could otherwise switch strategy at runtime)
    .config("spark.sql.adaptive.enabled", "false")
    # Number of partitions after a shuffle; 8 keeps the plan/UI small
    # (default is 200)
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

# Two comparable-size tables: 100k and 80k rows
# join_key 1..80000 overlaps, so 80,000 rows should match
left = spark.range(1, 100001).selectExpr("id AS join_key", "id AS left_value")
right = spark.range(1, 80001).selectExpr("id AS join_key", "id * 10 AS right_value")

# hint("merge") = tell Catalyst to use SortMergeJoin
# (aliases: "shuffle_merge", "mergejoin")
joined = left.hint("merge").join(right.hint("merge"), on="join_key", how="inner")

joined.explain("formatted")   # shows the plan only, no execution
print(joined.count())         # action: runs the job -> expect 80000