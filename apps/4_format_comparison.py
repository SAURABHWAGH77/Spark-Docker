from pyspark.sql import SparkSession
import time


spark = SparkSession.builder \
    .appName("FileFormatComparison") \
    .master("spark://spark-master:7077") \
    .getOrCreate()


# --------------------------------------------------
# 1. Create 1 Million Records
# --------------------------------------------------

print("\n========== Creating Data ==========")

data = [
    (i, f"name_{i}", i * 100, "2026-01-01")
    for i in range(1_000_000)
]

df = spark.createDataFrame(
    data,
    ["id", "name", "salary", "join_date"]
)

print("Records:", df.count())
print("Partitions:", df.rdd.getNumPartitions())

df.printSchema()


# --------------------------------------------------
# 2. CSV WRITE
# --------------------------------------------------

print("\n========== CSV WRITE ==========")

start = time.time()

df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("/opt/spark-data/format_test/csv")

end = time.time()

print("CSV Write Time:", round(end - start, 2), "seconds")


# --------------------------------------------------
# 3. JSON WRITE
# --------------------------------------------------

print("\n========== JSON WRITE ==========")

start = time.time()

df.write \
    .mode("overwrite") \
    .json("/opt/spark-data/format_test/json")

end = time.time()

print("JSON Write Time:", round(end - start, 2), "seconds")


# --------------------------------------------------
# 4. PARQUET WRITE
# --------------------------------------------------

print("\n========== PARQUET WRITE ==========")

start = time.time()

df.write \
    .mode("overwrite") \
    .parquet("/opt/spark-data/format_test/parquet")

end = time.time()

print("Parquet Write Time:", round(end - start, 2), "seconds")


# --------------------------------------------------
# 5. CSV READ
# --------------------------------------------------

print("\n========== CSV READ ==========")

start = time.time()

csv_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/opt/spark-data/format_test/csv")

csv_count = csv_df.count()

end = time.time()

print("CSV Records:", csv_count)
print("CSV Read Time:", round(end - start, 2), "seconds")


# --------------------------------------------------
# 6. JSON READ
# --------------------------------------------------

print("\n========== JSON READ ==========")

start = time.time()

json_df = spark.read \
    .json("/opt/spark-data/format_test/json")

json_count = json_df.count()

end = time.time()

print("JSON Records:", json_count)
print("JSON Read Time:", round(end - start, 2), "seconds")


# --------------------------------------------------
# 7. PARQUET READ
# --------------------------------------------------

print("\n========== PARQUET READ ==========")

start = time.time()

parquet_df = spark.read \
    .parquet("/opt/spark-data/format_test/parquet")

parquet_count = parquet_df.count()

end = time.time()

print("Parquet Records:", parquet_count)
print("Parquet Read Time:", round(end - start, 2), "seconds")


# --------------------------------------------------
# 8. Column Pruning Test
# --------------------------------------------------

print("\n========== COLUMN PRUNING TEST ==========")

start = time.time()

parquet_df.select("name").count()

end = time.time()

print(
    "Parquet - Select only name:",
    round(end - start, 2),
    "seconds"
)


# --------------------------------------------------
# 9. Predicate Pushdown Test
# --------------------------------------------------

print("\n========== PREDICATE PUSHdown TEST ==========")

start = time.time()

parquet_df \
    .filter("salary > 50000000") \
    .count()

end = time.time()

print(
    "Parquet - Filter salary:",
    round(end - start, 2),
    "seconds"
)


spark.stop()