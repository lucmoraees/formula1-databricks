# Databricks notebook source
# MAGIC %md
# MAGIC ### Create raw tables - part 2

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create constructors table
# MAGIC 1. Single Line Json
# MAGIC 2. Simple structure

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.constructors;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.constructors(
# MAGIC   constructorId INT,
# MAGIC   constructorRef STRING,
# MAGIC   name STRING,
# MAGIC   nationality STRING,
# MAGIC   url STRING
# MAGIC )
# MAGIC USING JSON
# MAGIC OPTIONS(path "abfss://raw@formulaumdatalake.dfs.core.windows.net/constructors.json")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_raw.constructors;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create drivers table
# MAGIC 1. Single Line Json
# MAGIC 2. Complex structure

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.drivers;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.drivers(
# MAGIC   driverId INT,
# MAGIC   driverRef STRING,
# MAGIC   number INT,
# MAGIC   code STRING,
# MAGIC   name STRUCT<forename: STRING, surname: STRING>,
# MAGIC   dob DATE,
# MAGIC   nationality STRING,
# MAGIC   url STRING
# MAGIC )
# MAGIC USING JSON
# MAGIC OPTIONS(path "abfss://raw@formulaumdatalake.dfs.core.windows.net/drivers.json")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_raw.drivers;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create results table
# MAGIC 1. Single Line Json
# MAGIC 2. Simple structure

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.results;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.results(
# MAGIC   resultId INT,
# MAGIC   raceId INT,
# MAGIC   driverId INT,
# MAGIC   constructorId INT,
# MAGIC   number INT,
# MAGIC   grid INT,
# MAGIC   position INT,
# MAGIC   positionText STRING,
# MAGIC   positionOrder STRING,
# MAGIC   points INT,
# MAGIC   laps INT,
# MAGIC   time STRING,
# MAGIC   milliseconds INT,
# MAGIC   fastestLap INT,
# MAGIC   rank INT,
# MAGIC   fastestLapTime STRING,
# MAGIC   fastestLapSpeed FLOAT,
# MAGIC   statusId STRING
# MAGIC )
# MAGIC USING JSON
# MAGIC OPTIONS(path "abfss://raw@formulaumdatalake.dfs.core.windows.net/results.json")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_raw.results;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create pit stops table
# MAGIC 1. Multi Line Json
# MAGIC 2. Simple structure

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.pit_stops;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
# MAGIC   driverId INT,
# MAGIC   duration STRING,
# MAGIC   lap INT,
# MAGIC   milliseconds INT,
# MAGIC   raceId INT,
# MAGIC   stop INT,
# MAGIC   time STRING
# MAGIC )
# MAGIC USING JSON
# MAGIC OPTIONS(path "abfss://raw@formulaumdatalake.dfs.core.windows.net/pit_stops.json", multiLine true)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_raw.pit_stops;