-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Lesson 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Write data to delat lake (managed table)
-- MAGIC 2. Write data to delta lake (external table)
-- MAGIC 3. Read data from delta lake (Table)
-- MAGIC 4. Read data from delta lake (File)

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE DATABASE IF NOT EXISTS f1_demo
-- MAGIC LOCATION 'abfss://demo@formulaumdatalake.dfs.core.windows.net/';

-- COMMAND ----------

-- MAGIC %python
-- MAGIC results_df = spark.read \
-- MAGIC   .option("printSchema", True) \
-- MAGIC   .json("abfss://raw@formulaumdatalake.dfs.core.windows.net/2021-03-28/results.json")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC results_df.write.format("delta").mode("overwrite").saveAsTable("f1_demo.results_managed")

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT * FROM f1_demo.results_managed;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC results_df.write.format("delta").mode("overwrite").save("abfss://demo@formulaumdatalake.dfs.core.windows.net/results_external")

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE TABLE f1_demo.results_external
-- MAGIC USING DELTA
-- MAGIC LOCATION 'abfss://demo@formulaumdatalake.dfs.core.windows.net/results_external';

-- COMMAND ----------

-- MAGIC %python
-- MAGIC results_external_df = spark.read.format("delta").load("abfss://demo@formulaumdatalake.dfs.core.windows.net/results_external")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(results_external_df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC results_df.write.format("delta").mode("overwrite").partitionBy("constructorId").saveAsTable("f1_demo.results_partitioned")

-- COMMAND ----------

SELECT * FROM f1_demo.results_partitioned;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Lesson 2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Update Delta Table
-- MAGIC 2. Delete From Delta Table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Update

-- COMMAND ----------

SELECT * FROM f1_demo.results_managed;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC UPDATE f1_demo.results_managed
-- MAGIC   SET points = 11 - position
-- MAGIC WHERE position <= 10
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from delta.tables import *
-- MAGIC from pyspark.sql.functions import *
-- MAGIC
-- MAGIC deltaTable = DeltaTable.forPath(spark, "abfss://demo@formulaumdatalake.dfs.core.windows.net/results_managed");
-- MAGIC
-- MAGIC deltaTable.update("position <= 10", { "points": "21 - position"})

-- COMMAND ----------

SELECT * FROM f1_demo.results_managed;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Delete

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC DELETE FROM f1_demo.results_managed
-- MAGIC WHERE position > 10;

-- COMMAND ----------

SELECT * FROM f1_demo.results_managed;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from delta.tables import *
-- MAGIC from pyspark.sql.functions import *
-- MAGIC
-- MAGIC deltaTable = DeltaTable.forPath(spark, "abfss://demo@formulaumdatalake.dfs.core.windows.net/results_managed");
-- MAGIC
-- MAGIC deltaTable.delete("points = 0")

-- COMMAND ----------

SELECT * FROM f1_demo.results_managed;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Lesson 3

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Upset using merge

-- COMMAND ----------

-- MAGIC %python
-- MAGIC drivers_day1_df = spark.read.option("inferSchema", True).json("abfss://raw@formulaumdatalake.dfs.core.windows.net/2021-03-28/drivers.json").filter("driverid <= 10").select("driverId", "dob", "name.forename", "name.surname")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(drivers_day1_df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import upper
-- MAGIC
-- MAGIC drivers_day1_df = spark.read.option("inferschema", True).json("abfss://raw@formulaumdatalake.dfs.core.windows.net/2021-03-28/drivers.json").filter("driverid BETWEEN 6 AND 15").select("driverId", "dob", upper("name.forename").alias('forename'), upper("name.surname").alias("surname"))

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC display(drivers_day1_df)