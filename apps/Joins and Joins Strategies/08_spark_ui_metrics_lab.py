from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

spark = (
    SparkSession.builder
    .appName("08-Spark-UI-Metrics-Lab")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

customers = spark.range(1, 5001).selectExpr(
    "id AS customer_id",
    "concat('C', id) AS name"
)

orders = spark.range(1, 300001).selectExpr(
    "id AS order_id",
    "(id % 5000) + 1 AS customer_id",
    "(id % 1000) AS amount"
)

print("\n--- EXPERIMENT A: MERGE JOIN ---")

merge_join = (
    orders.hint("merge")
    .join(customers.hint("merge"), "customer_id", "inner")
)

merge_join.explain("formatted")
print("Merge join count:", merge_join.count())

print("\n--- EXPERIMENT B: BROADCAST JOIN ---")

broadcast_join = orders.join(
    broadcast(customers),
    on="customer_id",
    how="inner"
)

broadcast_join.explain("formatted")
print("Broadcast join count:", broadcast_join.count())

print("\nCompare the physical plans and Spark UI metrics.")

spark.stop()
