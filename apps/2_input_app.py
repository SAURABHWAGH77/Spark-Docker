from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("GenerateCSV") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Sample data
data = [
    (1, "Rahul", 25000),
    (2, "Priya", 35000),
    (3, "Amit", 30000),
    (4, "Sneha", 40000)
]

columns = ["employee_id", "name", "salary"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Write CSV to the data folder
df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("/opt/spark-data/employees_csv")

print("CSV output generated successfully!")

spark.stop()    