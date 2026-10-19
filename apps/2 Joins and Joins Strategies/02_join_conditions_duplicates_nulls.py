from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName(
    "02-Join-Conditions-Duplicates-Nulls"
).getOrCreate()

spark.sparkContext.setLogLevel("WARN")

orders = [
    (1, 10, 100),
    (2, 10, 200),
    (3, 20, 300),
    (4, None, 400)
]

customers = [
    (10, "Asha"),
    (10, "Asha-duplicate"),
    (20, "Dev"),
    (None, "Unknown")
]

o = spark.createDataFrame(
    orders, ["order_id", "customer_id", "amount"]
).alias("o")

c = spark.createDataFrame(
    customers, ["customer_id", "name"]
).alias("c")

print("1. DUPLICATE KEYS")
result = o.join(
    c,
    col("o.customer_id") == col("c.customer_id"),
    "left"
)

result.select(
    "o.order_id",
    col("o.customer_id").alias("order_customer_id"),
    "o.amount",
    "c.name"
).show()

print("2. MULTIPLE JOIN CONDITIONS")
result = o.join(
    c,
    (col("o.customer_id") == col("c.customer_id"))
    & (col("o.amount") >= 200),
    "left"
)

result.select(
    "o.order_id",
    "o.amount",
    "c.name"
).show()

print("3. NORMAL EQUALITY: NULL DOES NOT MATCH NULL")
o.join(
    c,
    col("o.customer_id") == col("c.customer_id"),
    "inner"
).select(
    "o.order_id",
    col("o.customer_id").alias("order_customer_id"),
    "c.name"
).show()

print("4. NULL-SAFE EQUALITY")
o.join(
    c,
    col("o.customer_id").eqNullSafe(col("c.customer_id")),
    "inner"
).select(
    "o.order_id",
    col("o.customer_id").alias("order_customer_id"),
    "c.name"
).show()

spark.stop()
