from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyFirstClusterApp") \
    .getOrCreate()

data = [
    ("Saurabh", 70000),
    ("Rahul", 50000),
    ("Amit", 80000),
    ("Priya", 60000)
]

df = spark.createDataFrame(data, ["name", "salary"])

df.show()

print("Total employees:", df.count())

spark.stop()