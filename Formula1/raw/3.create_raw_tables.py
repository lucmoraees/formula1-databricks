# Databricks notebook source
# MAGIC %md
# MAGIC ### Create raw tables - part 3

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create Lap Times table
# MAGIC 1. CSV file
# MAGIC 2. Multiple files

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.lap_times;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
# MAGIC   raceId INT,
# MAGIC   driverId INT,
# MAGIC   lap INT,
# MAGIC   position INT,
# MAGIC   time STRING,
# MAGIC   milliseconds INT
# MAGIC )
# MAGIC USING csv
# MAGIC OPTIONS(path "abfss://raw@formulaumdatalake.dfs.core.windows.net/lap_times")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_raw.lap_times

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create Qualifying Table
# MAGIC 1. JSON file
# MAGIC 2. Multiline JSON
# MAGIC 3. Multiple files

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.qualifying;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
# MAGIC constructorId INT,
# MAGIC driverId INT,
# MAGIC number INT,
# MAGIC position INT,
# MAGIC q1 STRING,
# MAGIC q2 STRING,
# MAGIC q3 STRING,
# MAGIC qualifyId INT,
# MAGIC raceId INT
# MAGIC )
# MAGIC USING JSON
# MAGIC OPTIONS (path "abfss://raw@formulaumdatalake.dfs.core.windows.net/qualifying", multiLine true)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_raw.qualifying