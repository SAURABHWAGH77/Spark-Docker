
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

spark = SparkSession.builder.appName(
    "04-Broadcast-Hash-Join"
).getOrCreate()

spark.sparkContext.setLogLevel("WARN")

customers = spark.range(1, 1001).selectExpr(
    "id AS customer_id",
    "concat('customer_', id) AS name"
)

orders = spark.range(1, 100001).selectExpr(
    "id AS order_id",
    "(id % 1000) + 1 AS customer_id",
    "(id % 200) AS amount"
)

# Explicitly broadcast the smaller customer DataFrame
joined = orders.join(
    broadcast(customers),
    on="customer_id",
    how="inner"
)

print("PHYSICAL PLAN")
joined.explain("formatted")

print("JOINED ROW COUNT")
print(joined.count())

spark.stop()
