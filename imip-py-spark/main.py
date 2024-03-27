import pyspark
from delta import *
from delta.tables import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import json

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

json_data = [
    '{"id": 1, "name": "Alice", "age": 30}',
    '{"id": 2, "name": "Bob", "age": 40}',
    '{"id": 3, "name": "Charlie", "age": 50}'
]

# Read JSON data into a DataFrame
df = spark.read.json(spark.sparkContext.parallelize(json_data))

# Define the path to save the Delta table
delta_table_path = "./delta-table"

# Write DataFrame to Delta format
df.write.format("delta").mode("append").save(delta_table_path)