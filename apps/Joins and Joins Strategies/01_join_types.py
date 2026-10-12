
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("01-Join-Types").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

orders = [
    (101, 1, 500),
    (102, 2, 700),
    (103, 1, 300),
    (104, 4, 900),
    (105, None, 250)
]

customers = [
    (1, "Rahul"),
    (2, "Amit"),
    (3, "Priya"),
    (None, "Unknown")
]

df_orders = spark.createDataFrame(
    orders, ["order_id", "customer_id", "amount"]
)

df_customers = spark.createDataFrame(
    customers, ["customer_id", "name"]
)

print("ORDERS")
df_orders.show()

print("CUSTOMERS")
df_customers.show()

join_types = [
    "inner",
    "left",
    "right",
    "full_outer", #Keeps all rows when there is a match in either the left or right table.
    "left_semi", #Keeps rows from the left table that have a match in the right table.
    "left_anti"  #Keeps rows from the left table that do not have a match in the right table.
]

for join_type in join_types:
    print(f"\n--- {join_type.upper()} JOIN ---")

    result = df_orders.join(
        df_customers,
        on="customer_id",
        how=join_type
    )

    result.show()

spark.stop()
