"""
05 - Distribution/ranking functions: ntile, percent_rank, cume_dist
Run: spark-submit 05_ntile_percent_rank_cume_dist.py
"""
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("WindowDistributionDemo").getOrCreate()

data = [
    ("A", "p1", 10),
    ("A", "p2", 20),
    ("A", "p3", 20),
    ("A", "p4", 40),
    ("A", "p5", 50),
]
df = spark.createDataFrame(data, ["category", "product", "score"])
w = Window.partitionBy("category").orderBy("score")

result = (
    df.withColumn("quartile_bucket", F.ntile(4).over(w))
      .withColumn("percent_rank", F.percent_rank().over(w))
      .withColumn("cumulative_distribution", F.cume_dist().over(w))
      .orderBy("category", "score", "product")
)

result.show(truncate=False)
print("ntile divides ordered rows into buckets; ties may be split across buckets.")
print("percent_rank and cume_dist have different formulas and tie behavior.")

spark.stop()
