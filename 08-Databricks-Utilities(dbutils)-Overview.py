# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ##### Read CSV File from Azure Data Lake Storage Account
# MAGIC  CSV Source File Path : "abfss://working-labs@datalakestorageaccountname.dfs.core.windows.net/bronze/daily-pricing/csv"
# MAGIC
# MAGIC JSON  File Path : "abfss://working-labs@datalakestorageaccountname.dfs.core.windows.net/bronze/daily-pricing/json"
# MAGIC
# MAGIC
# MAGIC ##### Databricks Utilities
# MAGIC - <a href="https://docs.databricks.com/en/dev-tools/databricks-utils.html">dbutils</a>

# COMMAND ----------

storageAccountKey='E38/jVQWJUQi/rjSfHuApa6z9K6qp1HaeI8yX75+jXJZooiz7Q0CNT+U4toxwH3vnaghlyESd3ZL+AStNnGjCg=='
spark.conf.set("fs.azure.account.key.adlstadatalakehousejoel.dfs.core.windows.net",storageAccountKey)

# COMMAND ----------

CSVSourceFilePath="abfss://working-demopractice@adlstadatalakehousejoel.dfs.core.windows.net/bronze/daily_pricing/csv"
JSONFilePath="abfss://working-demopractice@adlstadatalakehousejoel.dfs.core.windows.net/bronze/daily_pricing/json"

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

