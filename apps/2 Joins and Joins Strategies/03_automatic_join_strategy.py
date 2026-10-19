# ---------------------------------------------------------------
# TOPIC: Automatic join strategy selection in Spark (Catalyst)
# LESSON: Spark picks a BROADCAST HASH JOIN on its own when one
#         side is small, so the big table is never shuffled.
# ---------------------------------------------------------------

from pyspark.sql import SparkSession

# Entry point to Spark; getOrCreate() reuses a session if one exists
spark = SparkSession.builder.appName(
    "03-Automatic-Join-Strategy"
).getOrCreate()

# Reduce log noise so the explain() output is easy to read
spark.sparkContext.setLogLevel("WARN")

# SMALL side (dimension table): 1,000 rows
# -> far below autoBroadcastJoinThreshold (default 10 MB)
customers = spark.range(1, 1001).selectExpr(
    "id AS customer_id",
    "concat('customer_', id) AS name"
)

# BIG side (fact table): 200,000 rows
# customer_id cycles 1..1000, so every order matches a customer
orders = spark.range(1, 200001).selectExpr(
    "id AS order_id",
    "(id % 1000) + 1 AS customer_id",
    "(id % 500) AS amount"
)

# No broadcast() hint here. Spark compares table sizes to the
# threshold and decides the join strategy by itself.
joined = orders.join(
    customers,
    on="customer_id",
    how="inner"
)

# KEY STEP: read the plan. Expect:
#   BroadcastHashJoin ... BuildRight
#   +- BroadcastExchange   <- customers sent to every executor
# (A SortMergeJoin + 2 shuffles would mean no broadcast happened.)
print("PHYSICAL PLAN")
joined.explain("formatted")

# Action: triggers actual execution (everything above was lazy).
# Result should be 200,000 (every order finds its customer).
print("JOINED ROW COUNT")
print(joined.count())

# Release resources
spark.stop()