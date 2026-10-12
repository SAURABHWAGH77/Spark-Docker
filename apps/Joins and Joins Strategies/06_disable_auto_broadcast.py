from pyspark.sql import SparkSession

spark = SparkSession.builder.appName(
    "06-Disable-Auto-Broadcast"
).getOrCreate()

spark.sparkContext.setLogLevel("WARN")

customers = spark.range(1, 501).selectExpr(
    "id AS customer_id"
)

orders = spark.range(1, 50001).selectExpr(
    "id AS order_id",
    "(id % 500) + 1 AS customer_id"
)

setting = "spark.sql.autoBroadcastJoinThreshold"
old_value = spark.conf.get(setting, "10485760")

try:
    print("\n--- PLAN: CURRENT SETTINGS ---")
    orders.join(
        customers, on="customer_id", how="inner"
    ).explain("formatted")

    # -1 disables automatic broadcast selection
    spark.conf.set(setting, "-1")

    print("\n--- PLAN: AUTOMATIC BROADCAST DISABLED ---")
    orders.join(
        customers, on="customer_id", how="inner"
    ).explain("formatted")

    print("\nJOINED ROW COUNT")
    print(
        orders.join(
            customers, on="customer_id", how="inner"
        ).count()
    )

finally:
    # Restore the previous setting for this Spark session
    spark.conf.set(setting, old_value)

spark.stop()
