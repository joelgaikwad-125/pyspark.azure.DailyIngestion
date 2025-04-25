# Databricks notebook source
# DBTITLE 0,--i18n-ef4d95c5-f516-40e2-975d-71fc17485bba
# MAGIC %md
# MAGIC
# MAGIC ##### Read CSV File from Azure Data Lake Storage Account
# MAGIC  CSV Source File Path : "abfss://working-labs@datalakestorageaccountname.dfs.core.windows.net/bronze/daily-pricing/csv"
# MAGIC
# MAGIC JSON  File Path : "abfss://working-labs@datalakestorageaccountname.dfs.core.windows.net/bronze/daily-pricing/json"
# MAGIC
# MAGIC ##### Spark Methods
# MAGIC - <a href="https://spark.apache.org/docs/latest/sql-getting-started.html#starting-point-sparksession" target="_blank">SparkSession</a>
# MAGIC
# MAGIC - <a href="https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html" target="_blank">GenericDataFrameReader</a>: **`json`**,**`csv`**,  **`option (header,inferSchema)`** ,  **`schema`**
# MAGIC
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/column.html" target="_blank">PySparkSQLFunctions</a>: **`col`** , **`alias`** , **`cast`**
# MAGIC
# MAGIC
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html" target="_blank">DataFrame</a> transformations and actions: 
# MAGIC
# MAGIC | Spark Method | Purpose |
# MAGIC | --- | --- |
# MAGIC | **`select`** | Returns a new DataFrame by computing given expression for each element |
# MAGIC | **`drop`** | Returns a new DataFrame with a column dropped |
# MAGIC | **`filter`**, **`where`** | Filters rows using the given condition |
# MAGIC | **`sort`**, **`orderBy`** | Returns a new DataFrame sorted by the given expressions |
# MAGIC | **`show`** , **`display`** | Displays the top n rows of DataFrame in a tabular form |
# MAGIC | **`count`** | Returns the number of rows in the DataFrame |
# MAGIC | **`dropDuplicates`**, **`distinct`** | Returns a new DataFrame with duplicate rows removed |
# MAGIC | **`withColumnRenamed`** | Returns a new DataFrame with a column renamed |
# MAGIC | **`withColumn`** | Returns a new DataFrame by adding a column or replacing the existing column that has the same name |
# MAGIC | **`first`** , **`head`** | Returns the the first row |
# MAGIC | **`collect`** | Returns an array that contains all rows in this DataFrame |
# MAGIC | **`limit`** , **`take`** | Returns a new DataFrame by taking the first n rows |
# MAGIC | **`groupBy`** | Groups the DataFrame using the specified columns, so we can run aggregation on them |
# MAGIC
# MAGIC - Other <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html" target="_blank">DataFrame</a> methods: **`printSchema`**, **`display`**, **`createOrReplaceTempView`**
# MAGIC
# MAGIC - <a href="https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html" target="_blank">GenericDataFrameWriter</a>: **`json`**, **`csv`**, **`mode (overwrite,append)`** 

# COMMAND ----------

storageAccountKey='E38/jVQWJUQi/rjSfHuApa6z9K6qp1HaeI8yX75+jXJZooiz7Q0CNT+U4toxwH3vnaghlyESd3ZL+AStNnGjCg=='
spark.conf.set("fs.azure.account.key.adlstadatalakehousejoel.dfs.core.windows.net",storageAccountKey)

# COMMAND ----------

sourceCSVFilePath = "abfss://working-demopractice@adlstadatalakehousejoel.dfs.core.windows.net/bronze/daily_pricing/csv"
sourceJSONFilePath = "abfss://working-demopractice@adlstadatalakehousejoel.dfs.core.windows.net/bronze/daily_pricing/json"

# COMMAND ----------

sourceJSONfilesparkDF=spark.read.json(sourceJSONFilePath)

# COMMAND ----------

sourceJSONfilesparkDF.show()

# COMMAND ----------

from pyspark.sql.types import DecimalType
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType


# COMMAND ----------

schema1=StructType([StructField("DATE_of_PRICING", StringType(), True),StructField("ROWID", IntegerType(), True), StructField("STATE_NAME", StringType(), True), StructField("MARKET_NAME", StringType(), True), StructField("PRODUCTGROUP_NAME", StringType(), True), StructField("PRODUCT_NAME", StringType(), True), StructField("VARIETY", StringType(), True), StructField("ORIGIN", StringType(), True), StructField("ARRIVAL_IN_TONES", DoubleType(), True), StructField("MINIMUM_PRICE", StringType(), True), StructField("MAXIMUM_PRICE", StringType(), True), StructField("MODAL_PRICE", StringType(), True)])

# COMMAND ----------

sourceJSONfilesparkDF2=(spark.read.schema(schema1).json(sourceJSONFilePath,multiLine=True))

# COMMAND ----------

sourceJSONfilesparkDF2.columns

# COMMAND ----------

# DBTITLE 1,using select Tfunction
(
  sourceJSONfilesparkDF2.
  select("MARKET_NAME")
)

# COMMAND ----------

# DBTITLE 1,using drop Tfunction
sourceJSONfilesparkDF2.drop("ARRIVAL_IN_TONES")

# COMMAND ----------

sourceJSONfilesparkDF2.select("STATE_NAME","PRODUCTGROUP_NAME","PRODUCT_NAME").filter("ARRIVAL_IN_TONES > 100").where(col("STATE_NAME")=="Andhra Pradesh")

# COMMAND ----------

sourceJSONfilesparkDF2.select("STATE_NAME","PRODUCTGROUP_NAME","PRODUCT_NAME","ARRIVAL_IN_TONES").filter("ARRIVAL_IN_TONES > 100").where(col("STATE_NAME")=="Andhra Pradesh").sort("ARRIVAL_IN_TONES").show()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,using chatgpt as there was null value included in prior code as the file is not in correct format
from pyspark.sql.functions import *

# COMMAND ----------

rawDF = spark.read.text(sourceJSONFilePath)

# COMMAND ----------

# DBTITLE 1,new method used as the file is not in correct format
column_mapping = {
    "01/01/2023": "DATE_of_PRICING",
    "12699": "ROWID",
    "Uttar Pradesh": "STATE_NAME",
    "Najibabad": "MARKET_NAME",
    "Fruits": "PRODUCTGROUP_NAME",
    "Raddish5": "PRODUCT_NAME",
    "Raddish6": "VARIETY",
    "NR": "ORIGIN",
    "23": "ARRIVAL_IN_TONES",
    "250": "MINIMUM_PRICE",
    "350": "MAXIMUM_PRICE",
    "300.0": "MODAL_PRICE"
}

# COMMAND ----------

rawDF = spark.read.json(sourceJSONFilePath, multiLine=True)

# COMMAND ----------

for old_col, new_col in column_mapping.items():
    rawDF = rawDF.withColumnRenamed(old_col, new_col)

# COMMAND ----------

sourceJSONfilesparkDF3 = rawDF.select([col(c).cast(schema1[c].dataType) for c in schema1.fieldNames()])

# COMMAND ----------

sourceJSONfilesparkDF3.show()

# COMMAND ----------

