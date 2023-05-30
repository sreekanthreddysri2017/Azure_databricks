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

from pyspark.sql.functions import explode
df_transformed = df.select(df.message, explode(df.data).alias("employee"), df.status)
df_transformed.show(truncate=False)
#display(df_transformed)

# COMMAND ----------

df = df_transformed.select("employee.*")
display(df)

# COMMAND ----------

df.write.format("delta").option("mergeSchema", True).mode("append").save("/dbfs/mnt3/restApi")

df.display()

# COMMAND ----------

from pyspark.sql.functions import col
df1=df.groupby(col('id')).count()
df1.show()

# COMMAND ----------

df2=df1.join(df,df1['id']==df['id'],'inner').drop(df.id)
df2.show()

# COMMAND ----------

spark.sql("CREATE TABLE IF NOT EXISTS silver_table1 USING DELTA LOCATION '/dbfs/mnt3/restApi'") 
spark.sql("CREATE TABLE IF NOT EXISTS silver_table2 USING DELTA LOCATION '/dbfs/mnt3/restApi'")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_table1

# COMMAND ----------

# gold_table = spark.sql("SELECT t1.*, t2.* FROM silver_table1 t1 JOIN silver_table2 t2 ON t1.employee = t2.employee")
# #gold_table.show()
# display(gold_table)

# COMMAND ----------


