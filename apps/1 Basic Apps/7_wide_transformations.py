from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("WideTransformations") \
    .getOrCreate()

# Create sample data
data = [(i, i % 5) for i in range(1_000_000)]

df = spark.createDataFrame(data, ["id", "group"])

# Wide transformation
df_wide = df.groupBy("group").count()

# Action - triggers execution
df_wide.show()

spark.stop()