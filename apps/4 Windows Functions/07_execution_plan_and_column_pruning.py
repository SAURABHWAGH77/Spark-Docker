"""
07 - Inspect the plan and compare wide vs narrow inputs
Run: spark-submit 07_execution_plan_and_column_pruning.py
"""
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("WindowPlanDemo")
    .config("spark.sql.shuffle.partitions", "4")  # small demo only
    .getOrCreate()
)

data = [
    ("A", "t1", "2024-01-01", 100, "unused-1", "unused-2"),
    ("A", "t2", "2024-01-02", 200, "unused-3", "unused-4"),
    ("B", "t3", "2024-01-01", 50, "unused-5", "unused-6"),
]
df = spark.createDataFrame(
    data, ["user", "transaction_id", "event_date", "amount", "extra_a", "extra_b"]
)

w = Window.partitionBy("user").orderBy("event_date", "transaction_id")

# Compare plans. Spark may automatically prune unused columns depending on the query.
wide_result = df.withColumn("rn", F.row_number().over(w))
narrow_input = df.select("user", "transaction_id", "event_date", "amount")
narrow_result = narrow_input.withColumn("rn", F.row_number().over(w))

print("\n=== Wide result plan ===")
wide_result.explain("formatted")

print("\n=== Narrow result plan ===")
narrow_result.explain("formatted")

print("\nSpark UI is needed for actual shuffle bytes, task durations, and spill metrics.")
print("A plan shows operators; it does not prove runtime cost by itself.")

spark.stop()
