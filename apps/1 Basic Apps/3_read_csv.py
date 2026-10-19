from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ReadCSV") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/opt/spark-data/employees_csv")

df.show()

df.printSchema()

spark.stop()