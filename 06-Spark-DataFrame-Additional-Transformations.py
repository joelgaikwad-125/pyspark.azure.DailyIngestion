# Databricks notebook source
# DBTITLE 0,--i18n-ef4d95c5-f516-40e2-975d-71fc17485bba
# MAGIC %md
# MAGIC
# MAGIC ##### Read CSV File from Azure Data Lake Storage Account
# MAGIC  CSV Source File Path : "abfss://working-labs@datalakestorageaccountname.dfs.core.windows.net/bronze/daily-pricing/csv"
# MAGIC
# MAGIC PARQUET Target  File Path : "abfss://working-labs@datalakestorageaccountname.dfs.core.windows.net/bronze/daily-pricing/parquet"
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
# MAGIC | Method | Description |
# MAGIC | --- | --- |
# MAGIC | **`select`** | Returns a new DataFrame by computing given expression for each element |
# MAGIC | **`drop`** | Returns a new DataFrame with a column dropped |
# MAGIC | **`filter`**, **`where`** | Filters rows using the given condition |
# MAGIC | **`sort`**, **`orderBy`** | Returns a new DataFrame sorted by the given expressions |
# MAGIC | **`dropDuplicates`**, **`distinct`** | Returns a new DataFrame with duplicate rows removed |
# MAGIC | **`withColumnRenamed`** | Returns a new DataFrame with a column renamed |
# MAGIC | **`withColumn`** | Returns a new DataFrame by adding a column or replacing the existing column that has the same name |
# MAGIC | **`limit`** , **`take`** | Returns a new DataFrame by taking the first n rows |
# MAGIC | **`groupBy`** | Groups the DataFrame using the specified columns, so we can run aggregation on them |
# MAGIC
# MAGIC - Other <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html" target="_blank">DataFrame</a> methods: **`printSchema`**, **`display`**, **`createOrReplaceTempView`**
# MAGIC
# MAGIC - <a href="https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html" target="_blank">GenericDataFrameWriter</a>: **`parquet`**, **`csv`**, **`mode (overwrite,append)`** 

# COMMAND ----------

storageAccountKey='E38/jVQWJUQi/rjSfHuApa6z9K6qp1HaeI8yX75+jXJZooiz7Q0CNT+U4toxwH3vnaghlyESd3ZL+AStNnGjCg=='
spark.conf.set("fs.azure.account.key.adlstadatalakehousejoel.dfs.core.windows.net",storageAccountKey)

# COMMAND ----------

CSVSourceFilePath = "abfss://working-demopractice@adlstadatalakehousejoel.dfs.core.windows.net/bronze/daily_pricing/csv"
PARQUETTargetFilePath = "abfss://working-demopractice@adlstadatalakehousejoel.dfs.core.windows.net/bronze/daily_pricing/parquet"

# COMMAND ----------

sourceCSVdf=(spark.
            read.
            load(CSVSourceFilePath,format="csv",header="true",inferSchema="true")
)             
             
            

# COMMAND ----------

from pyspark.sql.types import DecimalType
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# COMMAND ----------

schema1=StructType([StructField("DATE_of_PRICING", StringType(), True),StructField("ROWID", IntegerType(), True), StructField("STATE_NAME", StringType(), True), StructField("MARKET_NAME", StringType(), True), StructField("PRODUCTGROUP_NAME", StringType(), True), StructField("PRODUCT_NAME", StringType(), True), StructField("VARIETY", StringType(), True), StructField("ORIGIN", StringType(), True), StructField("ARRIVAL_IN_TONES", DoubleType(), True), StructField("MINIMUM_PRICE", StringType(), True), StructField("MAXIMUM_PRICE", StringType(), True), StructField("MODAL_PRICE", StringType(), True)])

# COMMAND ----------

sourceJSONfilesparkDF2=(spark.read.schema(schema1).csv(CSVSourceFilePath,multiLine=True , inferSchema=True))

# COMMAND ----------

from pyspark.sql.functions import col
sourceJSONfilesparkDF2.withColumn("ARRIVAL_IN_KILOGRAMS" , col("ARRIVAL_IN_TONES")*1000)

# COMMAND ----------

sourceCSVTransDF3=sourceJSONfilesparkDF2.withColumn("ARRIVAL_IN_KILOGRAMS" , col("ARRIVAL_IN_TONES")*1000)

# COMMAND ----------

display(sourceCSVTransDF3)

# COMMAND ----------

from pyspark.sql.functions import *
sourceCSVTransDF4=sourceCSVTransDF3.groupBy("STATE_NAME","PRODUCT_NAME").agg(sum("ARRIVAL_IN_KILOGRAMS").alias("TOTAL_ARRIVAL_IN_KILOGRAMS"))

# COMMAND ----------

sourceCSVTransDF4.show()

# COMMAND ----------

sourceCSVTransDF3.write.mode("overwrite").save(PARQUETTargetFilePath)

# COMMAND ----------

