"""
02 - Running totals and row-based frames
Run: spark-submit 02_running_totals_and_frames.py
"""
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("WindowFramesDemo").getOrCreate()

data = [
    ("A", "2024-01-01", 100),
    ("A", "2024-01-02", 200),
    ("A", "2024-01-03", 150),
    ("A", "2024-01-04", 50),
    ("B", "2024-01-01", 40),
    ("B", "2024-01-02", 60),
]
df = spark.createDataFrame(data, ["user", "date", "amount"])

# Always make ordering deterministic if rows can share the ordering value.
ordered = Window.partitionBy("user").orderBy("date")

running_frame = ordered.rowsBetween(
    Window.unboundedPreceding,  # Starts from the first row in the window partition.
    Window.currentRow # Ends at the current row.
)
last_3_rows_frame = ordered.rowsBetween(-2, Window.currentRow)

result = (
    df.withColumn("running_total", F.sum("amount").over(running_frame))
      .withColumn("rolling_3_row_sum", F.sum("amount").over(last_3_rows_frame))
      .withColumn("rolling_3_row_avg", F.avg("amount").over(last_3_rows_frame))
      .orderBy("user", "date")
)

result.show(truncate=False)
print("rowsBetween(-2, currentRow) means current row plus up to two preceding rows.")
print("It is a row-count frame, not necessarily a three-calendar-day frame.")

spark.stop()
