"""
06 - Keep the latest record per business key with row_number
Run: spark-submit 06_dedup_latest_record.py
"""
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("WindowDedupDemo").getOrCreate()

data = [
    ("customer-1", "event-1", "2024-01-01 09:00:00", "bronze"),
    ("customer-1", "event-2", "2024-01-02 09:00:00", "silver"),
    ("customer-1", "event-3", "2024-01-02 09:00:00", "gold"),
    ("customer-2", "event-4", "2024-01-01 10:00:00", "bronze"),
]
df = (
    spark.createDataFrame(data, ["customer_id", "event_id", "event_time", "tier"])
         .withColumn("event_time", F.to_timestamp("event_time")) # Converts the existing event_time column from string to timestamp.
)

# event_id is used as a stable tie-breaker for equal timestamps.
w = Window.partitionBy("customer_id").orderBy(
    F.col("event_time").desc(),
    F.col("event_id").desc()
)

latest = (
    df.withColumn("rn", F.row_number().over(w))
      .filter(F.col("rn") == 1)
      .drop("rn") # Remove the temporary row-number column as we no longer needed in final result
)

latest.show(truncate=False)
print("Define the business key and tie-break rule explicitly; do not rely on arbitrary ordering.")

spark.stop()
