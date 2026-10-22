"""
03 - lag and lead: compare neighboring events
Run: spark-submit 03_lag_lead_comparisons.py
"""
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("WindowLagLeadDemo").getOrCreate()

data = [
    ("A", "e1", "2024-01-01", 100),
    ("A", "e2", "2024-01-02", 130),
    ("A", "e3", "2024-01-03", 90),
    ("B", "e4", "2024-01-01", 50),
    ("B", "e5", "2024-01-02", 80),
]
df = spark.createDataFrame(data, ["user", "event_id", "event_date", "amount"])

w = Window.partitionBy("user").orderBy("event_date", "event_id")

result = (
    df.withColumn("previous_amount", F.lag("amount", 1).over(w))
      .withColumn("next_amount", F.lead("amount", 1).over(w))
      .withColumn("change_from_previous", F.col("amount") - F.col("previous_amount"))
      .orderBy("user", "event_date")
)

result.show(truncate=False)
print("The first row per user has no previous value, so lag returns null by default.")

spark.stop()
