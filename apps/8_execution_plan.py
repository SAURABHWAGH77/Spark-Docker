from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create Spark session
spark = SparkSession.builder \
    .appName("BasicTransformations") \
    .getOrCreate()

# --------------------------------------------------
# 1. Create sample data
# --------------------------------------------------

data = [
    (1, 2),
    (2, 4),
    (3, 6),
    (4, 8),
    (5, 10),
    (6, 12),
    (7, 14),
    (8, 16),
    (9, 18),
    (10, 20)
]

df = spark.createDataFrame(data, ["id", "group"])

print("Original Data:")
df.show()


# --------------------------------------------------
# 2. Apply basic transformations
# --------------------------------------------------

df_transformed = (
    df.select("id", "group")
      .filter(col("group") > 10)
      .withColumn("group_double", col("group") * 2)
      .withColumn("group_plus_one", col("group") + 1)
)


# --------------------------------------------------
# 3. Show the execution plan
# --------------------------------------------------

print("Execution Plan:")
df_transformed.explain() #Function to print Execution Plan


# --------------------------------------------------
# 4. Trigger execution
# --------------------------------------------------

print("Final Data:")
df_transformed.show()


# Stop Spark
spark.stop()