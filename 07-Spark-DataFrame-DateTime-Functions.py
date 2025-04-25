# Databricks notebook source
# DBTITLE 0,--i18n-ef4d95c5-f516-40e2-975d-71fc17485bba
# MAGIC %md
# MAGIC
# MAGIC ##### Read CSV File from Azure Data Lake Storage Account
# MAGIC  CSV Source File Path : "abfss://working-labs@datalakestorageaccountname.dfs.core.windows.net/bronze/daily-pricing/csv"
# MAGIC
# MAGIC JSON  File Path : "abfss://working-labs@datalakestorageaccountname.dfs.core.windows.net/bronze/daily-pricing/json"
# MAGIC
# MAGIC
# MAGIC ##### Spark Methods
# MAGIC - <a href="https://spark.apache.org/docs/latest/sql-getting-started.html#starting-point-sparksession" target="_blank">SparkSession</a>
# MAGIC
# MAGIC - <a href="https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html" target="_blank">GenericDataFrameReader</a>: **`json`**,**`csv`**,  **`option (header,inferSchema)`** ,  **`schema`**
# MAGIC
# MAGIC
# MAGIC ##### DateTime Methods
# MAGIC
# MAGIC **`current_timestamp()`** records the timestamp when the code is executed
# MAGIC
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/column.html" target="_blank">ColumnFunctions</a>: **`cast`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html#datetime-functions" target="_blank">Built-In DateTime Functions</a>: **`date_format`**, **`to_date`**,**`year`**, **`month`**, **`dayofmonth`**, **`minute`**, **`second`**
# MAGIC
# MAGIC ##### Date Time Functions
# MAGIC
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | **`current_timestamp`** | Returns the current timestamp at the start of query evaluation as a timestamp column |
# MAGIC | **`date_format`** | Converts a date/timestamp/string to a value of string in the format specified by the date format given by the second argument. |
# MAGIC | **`dayofmonth`** | Extracts the day of the month as an integer from a given date/timestamp/string |
# MAGIC
# MAGIC
# MAGIC #####  Date Time Formats
# MAGIC | Format | Meaning         | DataType | Sample Output              |
# MAGIC | ------ | --------------- | ------------ | ---------------------- |
# MAGIC | y      | year            | year         | 2020; 20               |
# MAGIC | D      | day-of-year     | number(3)    | 189                    |
# MAGIC | M/L    | month-of-year   | month        | 7; 07; Jul; July       |
# MAGIC | d      | day-of-month    | number(3)    | 28                     |
# MAGIC | Q/q    | quarter-of-year | number/text  | 3; 03; Q3; 3rd quarter |
# MAGIC | E      | day-of-week     | text         | Tue; Tuesday           |
# MAGIC

# COMMAND ----------

storageAccountKey='E38/jVQWJUQi/rjSfHuApa6z9K6qp1HaeI8yX75+jXJZooiz7Q0CNT+U4toxwH3vnaghlyESd3ZL+AStNnGjCg=='
spark.conf.set("fs.azure.account.key.adlstadatalakehousejoel.dfs.core.windows.net",storageAccountKey)

# COMMAND ----------

CSVSourceFilePath = "abfss://working-demopractice@adlstadatalakehousejoel.dfs.core.windows.net/bronze/daily_pricing/csv"
PARQUETSourceFilePath = "abfss://working-demopractice@adlstadatalakehousejoel.dfs.core.windows.net/bronze/daily_pricing/parquet"

# COMMAND ----------

sourcePARAQUETfileDF=spark.read.load(PARQUETSourceFilePath)

# COMMAND ----------

sourcePARAQUETfileDF.show()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

sourcePARAQUETfileTransDF=sourcePARAQUETfileDF.withColumn("UPDATED_DATE",current_timestamp())

# COMMAND ----------

display(sourcePARAQUETfileTransDF)

# COMMAND ----------

sourcePARAQUETfileTransDF1=sourcePARAQUETfileTransDF.withColumn("datalake_file_load_year",year("UPDATED_DATE")).withColumn("datalake_file_load_month",month("UPDATED_DATE")).withColumn("datalake_file_load_day",dayofmonth("UPDATED_DATE"))

# COMMAND ----------

display(sourcePARAQUETfileTransDF1.select("UPDATED_DATE","datalake_file_load_year","datalake_file_load_month","datalake_file_load_day"))

# COMMAND ----------

# DBTITLE 1,concating them into a singel variable in a particular format
from pyspark.sql.functions import concat
sourcePARAQUETfileTransDF1.select("UPDATED_DATE").withColumn("dl_file_load_dateformat",concat(year("UPDATED_DATE"),month("UPDATED_DATE"),dayofmonth("UPDATED_DATE"))).show()

# COMMAND ----------

from pyspark.sql.functions import date_format
sourcePARAQUETfileTransDF1.select("UPDATED_DATE").withColumn("dl_file_load_dateformat_new",date_format(col("UPDATED_DATE"),"yyyyMMdd")).show()

# COMMAND ----------

# DBTITLE 1,converting the string type date format into spark time stamp format
from pyspark.sql.functions import to_date
sourcePARAQUETfileTransDF1.select("DATE_of_PRICING").withColumn("PRICING_DATE",to_date("DATE_of_PRICING","dd/MM/yyyy")).show()

# COMMAND ----------

sourcePARAQUETfileTransDF1.select("DATE_of_PRICING").withColumn("PRICING_DATE",to_date("DATE_of_PRICING","dd/MM/yyyy")).withColumn("PRICING_DATE_FORMAT",date_format(col("PRICING_DATE"),"yyyyMMdd")).show()

# COMMAND ----------

