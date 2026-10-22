"""
01 - Ranking window functions: row_number, rank, dense_rank
Run: spark-submit 01_row_number_rank_dense_rank.py
"""
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("WindowRankingDemo").getOrCreate()

data = [
    ("A", "t1", 100),
    ("A", "t2", 100),
    ("A", "t3", 200),
    ("A", "t4", 300),
    ("B", "t5", 50),
    ("B", "t6", 50),
]
df = spark.createDataFrame(data, ["team", "transaction_id", "amount"])

# Partition rows by team; sort by amount within each team.
# transaction_id is a deterministic tie-breaker for row_number.
w_unique = Window.partitionBy("team").orderBy("amount", "transaction_id")
# For rank/dense_rank, order only by amount so equal amounts are peers.
w_ties = Window.partitionBy("team").orderBy("amount")

result = (
    df.withColumn("row_number", F.row_number().over(w_unique))
      .withColumn("rank", F.rank().over(w_ties))
      .withColumn("dense_rank", F.dense_rank().over(w_ties))
      .orderBy("team", "amount", "transaction_id")
)

result.show(truncate=False)  # Without truncation: Alexandria Johnson-Williams
#result.show(truncate=True)   # With truncation: Alexandria Johnson...
print("Interpretation: row_number is unique; rank leaves gaps after ties; dense_rank does not.")

spark.stop()
