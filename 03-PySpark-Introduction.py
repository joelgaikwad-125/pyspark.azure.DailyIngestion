# Databricks notebook source
# MAGIC %md
# MAGIC ##### Source File Details
# MAGIC Source File URL : "https://retailpricing.blob.core.windows.net/labs/lab1/PW_MW_DR_01012023.csv"
# MAGIC
# MAGIC Source File Ingestion Path : "abfss://working-labs@datalakestorageaccountname.dfs.core.windows.net/bronze/daily-pricing/csv"
# MAGIC
# MAGIC ##### Python Core Library Documentation
# MAGIC - <a href="https://pandas.pydata.org/docs/user_guide/index.html#user-guide" target="_blank">pandas</a>
# MAGIC - <a href="https://pypi.org/project/requests/" target="_blank">requests</a>
# MAGIC - <a href="https://docs.python.org/3/library/csv.html" target="_blank">csv</a>
# MAGIC
# MAGIC ##### Spark Methods
# MAGIC - <a href="https://spark.apache.org/docs/latest/sql-getting-started.html#starting-point-sparksession" target="_blank">SparkSession</a>

# COMMAND ----------

storageAccountKey='E38/jVQWJUQi/rjSfHuApa6z9K6qp1HaeI8yX75+jXJZooiz7Q0CNT+U4toxwH3vnaghlyESd3ZL+AStNnGjCg=='
spark.conf.set("fs.azure.account.key.adlstadatalakehousejoel.dfs.core.windows.net",storageAccountKey)

# COMMAND ----------

import pandas

# COMMAND ----------

sourceurl='https://retailpricing.blob.core.windows.net/labs/lab1/PW_MW_DR_01012023.csv'
destnpath='abfss://working-demopractice@adlstadatalakehousejoel.dfs.core.windows.net/bronze/daily_pricing/csv'

# COMMAND ----------

pandas.read_csv(sourceurl)

# COMMAND ----------

sourcefilepandasDF=pandas.read_csv(sourceurl)

# COMMAND ----------

spark.createDataFrame(sourcefilepandasDF)

# COMMAND ----------

sourcefileSparkDF=spark.createDataFrame(sourcefilepandasDF)

# COMMAND ----------

sourcefileSparkDF.write.mode('overwrite').csv(destnpath)