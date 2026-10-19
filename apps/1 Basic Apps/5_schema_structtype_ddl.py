from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType
)

spark = SparkSession.builder \
    .appName("SchemaStructTypeDDL") \
    .master("spark://spark-master:7077") \
    .getOrCreate()


# =========================================================
# STEP 1: Create sample nested data
# =========================================================

data = [
    (1, "Alice", ("NY", "USA")),
    (2, "Bob", ("LA", "USA"))
]

columns = ["id", "name", "location"]

df_raw = spark.createDataFrame(data, columns)

print("\n===== RAW DATA =====")
df_raw.show()
df_raw.printSchema()


# =========================================================
# STEP 2: Write data as JSON
# =========================================================

df_raw.write \
    .mode("overwrite") \
    .json("/opt/spark-data/nested_users")

print("\nJSON written successfully!")


# =========================================================
# STEP 3: Define schema using StructType
# =========================================================

struct_schema = StructType([
    StructField("id", IntegerType(), True),

    StructField("name", StringType(), True),

    StructField(
        "location",
        StructType([
            StructField("city", StringType(), True),
            StructField("country", StringType(), True)
        ]),
        True
    )
])


# =========================================================
# STEP 4: Read JSON using StructType
# =========================================================

df_struct = spark.read \
    .schema(struct_schema) \
    .json("/opt/spark-data/nested_users")

print("\n===== STRUCTTYPE SCHEMA =====")
df_struct.printSchema()

print("\n===== STRUCTTYPE DATA =====")
df_struct.show()


# =========================================================
# STEP 5: Define schema using DDL
# =========================================================

ddl_schema = """
id INT,
name STRING,
location STRUCT<city:STRING, country:STRING>
"""


# =========================================================
# STEP 6: Read JSON using DDL
# =========================================================

df_ddl = spark.read \
    .schema(ddl_schema) \
    .json("/opt/spark-data/nested_users")

print("\n===== DDL SCHEMA =====")
df_ddl.printSchema()

print("\n===== DDL DATA =====")
df_ddl.show()


# =========================================================
# STEP 7: Compare
# =========================================================

print("\n===== COMPARISON =====")

print("StructType schema:")
df_struct.printSchema()

print("DDL schema:")
df_ddl.printSchema()


spark.stop()