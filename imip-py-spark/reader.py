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

# # Define your JSON data
# json_data = [
#     '{"name": "Alice", "age": 30}',
#     '{"name": "Bob", "age": 40}',
#     '{"name": "Charlie", "age": 50}'
# ]

# # Read JSON data into a DataFrame
# df = spark.read.json(spark.sparkContext.parallelize(json_data))

# # Define the path to save the Delta table
delta_table_path = "./delta-table"

deltaTable = DeltaTable.forPath(spark, delta_table_path)


# Declare the predicate by using Spark SQL functions.
cons = col('id') == '3'
value = 
deltaTable.update(
  condition = cons,
  set = { "name": lit('Nguyen Ngoc Duong1') }
)

# Read Delta table into a DataFrame
delta_df = spark.read.format("delta").load(delta_table_path)

# Show the contents of the Delta table
delta_df.show()