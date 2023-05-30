# Databricks notebook source
dbutils.fs.mount(
source='wasbs://nestedjson@storagedemosreekanth.blob.core.windows.net/',
mount_point='/mnt/blob5',
extra_configs={'fs.azure.account.key.storagedemosreekanth.blob.core.windows.net':'J+gTyUvE9iLvkq9J/b5dG8366oNlTaQz2PO3NdH3BxM8Pab5lSAy3E17idOfCRLolzDIsrCDNxws+AStgwn3Bg=='}
)

# COMMAND ----------

dbutils.fs.ls('/mnt/blob5/bronze')

# COMMAND ----------

dbutils.fs.ls('/mnt/blob5/bronze/employee.json')

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

# Define the schema for the nested JSON
schema = StructType([
    StructField("status", StringType(), nullable=True),
    StructField("data", ArrayType(StructType([
        StructField("id", IntegerType(), nullable=True),
        StructField("employee_name", StringType(), nullable=True),
        StructField("employee_salary", IntegerType(), nullable=True),
        StructField("employee_age", IntegerType(), nullable=True),
        StructField("profile_image", StringType(), nullable=True)
    ])), nullable=True),
    StructField("message", StringType(), nullable=True)
])

# Read JSON data with schema
df = spark.read.option('multiline','true').schema(schema).json("/mnt/blob5/bronze/employee.json")

# Display the DataFrame
df.show(truncate=False)
#display(df)

# COMMAND ----------


