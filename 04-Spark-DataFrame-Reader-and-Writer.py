# Databricks notebook source
# DBTITLE 0,--i18n-ef4d95c5-f516-40e2-975d-71fc17485bba
# MAGIC %md
# MAGIC
# MAGIC ##### Read CSV File from Azure Data Lake Storage Account
# MAGIC  CSV Source File Path : "abfss://working-labs@datalakestorageaccountname.dfs.core.windows.net/bronze/daily-pricing/csv"
# MAGIC
# MAGIC JSON Target File Path : "abfss://working-labs@datalakestorageaccountname.dfs.core.windows.net/bronze/daily-pricing/json"
# MAGIC ##### Spark Methods
# MAGIC - <a href="https://spark.apache.org/docs/latest/sql-getting-started.html#starting-point-sparksession" target="_blank">SparkSession</a>
# MAGIC
# MAGIC - <a href="https://spark.apache.org/docs/latest/sql-data-sources-csv.html" target="_blank">DataFrameReader</a>: **`csv`**,  **`option (header,separator)`** ,  **`schema`**
# MAGIC
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/data_types.html" target="_blank">SparkDataTypes</a>: **`ArrayType`**, **`DoubleType`**, **`IntegerType`**, **`LongType`**, **`StringType`**, **`StructType`**, **`StructField`**
# MAGIC
# MAGIC - <a href="https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.types.StructType.html" target="_blank">StructType</a>
# MAGIC
# MAGIC
# MAGIC - <a href="https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.DataFrameWriter.json.html" target="_blank">DataFrameWriter</a>: **`json`**,  **`mode (overwrite,append)`** 

# COMMAND ----------

storageAccountKey='E38/jVQWJUQi/rjSfHuApa6z9K6qp1HaeI8yX75+jXJZooiz7Q0CNT+U4toxwH3vnaghlyESd3ZL+AStNnGjCg=='
spark.conf.set("fs.azure.account.key.adlstadatalakehousejoel.dfs.core.windows.net",storageAccountKey)

# COMMAND ----------

CSVSourceFilePath = "abfss://working-demopractice@adlstadatalakehousejoel.dfs.core.windows.net/bronze/daily_pricing/csv"
JSONTargetFilePath = "abfss://working-demopractice@adlstadatalakehousejoel.dfs.core.windows.net/bronze/daily_pricing/json"

# COMMAND ----------

sparksourceread=spark.read.csv(CSVSourceFilePath,header=True)


# COMMAND ----------

display(sparksourceread)

# COMMAND ----------

(sparksourceread.write.format("json").mode("overwrite").save(JSONTargetFilePath))

# COMMAND ----------

