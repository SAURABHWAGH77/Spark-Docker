from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("NarrowTransformations") \
    .getOrCreate()

# Create sample data
data = [(i, i % 5) for i in range(1_000_000)]

df = spark.createDataFrame(data, ["id", "group"])

# Narrow transformations
df_narrow = df.filter(df.group > 2).select("id")

# Action - triggers execution
df_narrow.show()

spark.stop()