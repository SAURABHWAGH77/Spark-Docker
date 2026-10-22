"""
04 - first_value, last_value, nth_value and explicit frames
Run: spark-submit 04_first_last_nth_value.py
"""
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("WindowValueFunctionsDemo").getOrCreate()

data = [
    ("A", "2024-01-01", 10),
    ("A", "2024-01-02", 20),
    ("A", "2024-01-03", 30),
    ("B", "2024-01-01", 7),
    ("B", "2024-01-02", 9),
]
df = spark.createDataFrame(data, ["user", "event_date", "value"])

w_all = (
    Window.partitionBy("user")
          .orderBy("event_date")
          .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing) #Window.unboundedFollowing - Includes all rows from the current row to the last row in the window partition.
)
w_to_current = (
    Window.partitionBy("user")
          .orderBy("event_date")
          .rowsBetween(Window.unboundedPreceding, Window.currentRow) 
)

result = (
    df.withColumn("first_in_partition", F.first_value("value").over(w_all))
      .withColumn("last_in_partition", F.last_value("value").over(w_all))
      .withColumn("running_last_value", F.last_value("value").over(w_to_current))
      .orderBy("user", "event_date")
)

result.show(truncate=False)
print("Explicit frames avoid confusion about what last_value can see.")
print("Function availability can vary by PySpark version; check your installed Spark docs.")

spark.stop()
