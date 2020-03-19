# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC This notebook shows you how to create and query a table or DataFrame loaded from data stored in Azure Blob storage.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 1: Set the data location and type
# MAGIC 
# MAGIC There are two ways to access Azure Blob storage: account keys and shared access signatures (SAS).
# MAGIC 
# MAGIC To get started, we need to set the location and type of the file.

# COMMAND ----------

storage_account_name = "azdbksd7stg1"
storage_account_access_key = "+D6GeGvd01IT559ngKZbmGzFWwXtRmXE+TyiAlwYtQ+whtGtMWrRF4VJpcDHooRx9LFCtshe6wbD+HoYzoGM5A=="

# COMMAND ----------

#file_location = "wasbs://azdbksd7cont1@azdbksd7stg1.blob.core.windows.net/azdbksd7cont1/weekly-rent-paid-by-household-2018-census-csv.csv"
file_location = "wasbs://azdbksd7cont1@azdbksd7stg1.blob.core.windows.net/weekly-rent-paid-by-household-2018-census-csv.csv"
file_type = "csv"
#https://azdbksd7stg1.blob.core.windows.net/azdbksd7cont1/weekly-rent-paid-by-household-2018-census-csv.csv

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 2: Read the data
# MAGIC 
# MAGIC Now that we have specified our file metadata, we can create a DataFrame. Notice that we use an *option* to specify that we want to infer the schema from the file. We can also explicitly set this to a particular schema if we have one already.
# MAGIC 
# MAGIC First, let's create a DataFrame in Python.

# COMMAND ----------

df = spark.read.format(file_type).option("inferSchema", "true").option("header", "true").load(file_location)
df = df.withColumnRenamed("Weekly rent paid by household", "weeklyrent")
df = df.withColumnRenamed("Households in rented occupied private dwellings", "household")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 3: Query the data
# MAGIC 
# MAGIC Now that we have created our DataFrame, we can query it. For instance, you can identify particular columns to select and display.

# COMMAND ----------

display(df.select("code"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 4: (Optional) Create a view or table
# MAGIC 
# MAGIC If you want to query this data as a table, you can simply register it as a *view* or a table.

# COMMAND ----------

df.createOrReplaceTempView("weeklyrenttemp")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We can query this view using Spark SQL. For instance, we can perform a simple aggregation. Notice how we can use `%sql` to query the view from SQL.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --SELECT EXAMPLE_GROUP, SUM(EXAMPLE_AGG) FROM YOUR_TEMP_VIEW_NAME GROUP BY EXAMPLE_GROUP
# MAGIC select * from weeklyrenttemp

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Since this table is registered as a temp view, it will be available only to this notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.

# COMMAND ----------

df.write.format("parquet").saveAsTable("weeklytemp")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC This table will persist across cluster restarts and allow various users across different notebooks to query this data.

# COMMAND ----------

# MAGIC %sql select * from weeklytemp

# COMMAND ----------

df.write.csv("/databricks/new/test.csv")

# COMMAND ----------

df.coalesce(1).write.csv("/databricks/new/test1.csv")

# COMMAND ----------

df.coalesce(1).write.csv("wasbs://azdbksd7cont1@azdbksd7stg1.blob.core.windows.net/test1.csv", mode="overwrite", header="true") #.option("inferSchema", "true").option("header", "true")

# COMMAND ----------

# MAGIC %md
# MAGIC https://spark.apache.org/docs/2.3.0/sql-programming-guide.html

# COMMAND ----------

spark.version

# COMMAND ----------

from pyspark.sql import SparkSession

# COMMAND ----------

# MAGIC %md
# MAGIC https://docs.databricks.com/data/data-sources/sql-databases.html#python-example

# COMMAND ----------

jdbcUrl = "jdbc:mysql://{0}:{1}/{2}".format(jdbcHostname, jdbcPort, jdbcDatabase)
connectionProperties = {
  "user" : jdbcUsername,
  "password" : jdbcPassword,
  "driver" : "com.mysql.jdbc.Driver"
}jdbcHostname = "<hostname>"
jdbcDatabase = "employees"
jdbcPort = 3306
jdbcUrl = "jdbc:mysql://{0}:{1}/{2}?user={3}&password={4}".format(jdbcHostname, jdbcPort, jdbcDatabase, username, password)

# COMMAND ----------

pushdown_query = "(select * from employees where emp_no < 10008) emp_alias"
df = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties)
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE <jdbcTable>
# MAGIC USING org.apache.spark.sql.jdbc
# MAGIC OPTIONS (
# MAGIC   url "jdbc:<databaseServerType>://<jdbcHostname>:<jdbcPort>",
# MAGIC   dbtable "<jdbcDatabase>.atable",
# MAGIC   user "<jdbcUsername>",
# MAGIC   password "<jdbcPassword>"
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO diamonds
# MAGIC SELECT * FROM diamonds LIMIT 10 -- append 10 records to the table
# MAGIC 
# MAGIC SELECT count(*) record_count FROM diamonds --count increased by 10

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE TABLE diamonds
# MAGIC SELECT carat, cut, color, clarity, depth, TABLE AS table_number, price, x, y, z FROM diamonds
# MAGIC 
# MAGIC SELECT count(*) record_count FROM diamonds --count returned to original value (10 less)

# COMMAND ----------

# MAGIC %md
# MAGIC There are two APIs for specifying partitioning, high level and low level.
# MAGIC 
# MAGIC The high level API takes the name of a numeric column (columnName), two range endpoints (lowerBound, upperBound) and a target numPartitions and generates Spark tasks by evenly splitting the specified range into numPartitions tasks. This work well if your database table has an indexed numeric column with fairly evenly-distributed values, such as an auto-incrementing primary key; it works somewhat less well if the numeric column is extremely skewed, leading to imbalanced tasks.

# COMMAND ----------

# MAGIC %md
# MAGIC The low level API:org.apache.spark.sql.DataFrame), accessible in Scala, accepts an array of WHERE conditions that can be used to define custom partitions: this is useful for partitioning on non-numeric columns or for dealing with skew. When defining custom partitions, do not forget to consider NULL when the partition columns are Nullable. We do not suggest that you manually define partitions using more than two columns since writing the boundary predicates require much more complex logic.

# COMMAND ----------

# Use the Spark CSV datasource with options specifying:
# - First line of file is a header
# - Automatically infer the schema of the data
%python
data = spark.read.csv("/databricks-datasets/samples/population-vs-price/data_geo.csv", header="true", inferSchema="true")
data.cache() # Cache data for faster reuse
data = data.dropna() # drop rows with missing values

# COMMAND ----------

# MAGIC %md
# MAGIC https://docs.databricks.com/data/data-sources/sql-databases.html#python-example

# COMMAND ----------

# MAGIC %python
# MAGIC data.take(10)

# COMMAND ----------

# MAGIC %python
# MAGIC display(data)

# COMMAND ----------

# Register table so it is accessible via SQL Context
%python
data.createOrReplaceTempView("data_geo")

# COMMAND ----------

# MAGIC %sql
# MAGIC select `State Code`, `2015 median sales price` from data_geo

# COMMAND ----------

# MAGIC %sql
# MAGIC select City, `2014 Population estimate` from data_geo where `State Code` = 'WA';

# COMMAND ----------

# MAGIC %python
# MAGIC # Use the Spark CSV datasource with options specifying:
# MAGIC #  - First line of file is a header
# MAGIC #  - Automatically infer the schema of the data
# MAGIC data = spark.read.csv("/databricks-datasets/samples/population-vs-price/data_geo.csv", header="true", inferSchema="true") 
# MAGIC data.cache()  # Cache data for faster reuse
# MAGIC data = data.dropna() # drop rows with missing values

# COMMAND ----------

# MAGIC %md
# MAGIC https://docs.databricks.com/getting-started/spark/dataframes.html

# COMMAND ----------

# MAGIC %md
# MAGIC https://docs.databricks.com/spark/latest/dataframes-datasets/introduction-to-dataframes-python.html

# COMMAND ----------

# MAGIC %md
# MAGIC https://databricks.com/glossary/pyspark