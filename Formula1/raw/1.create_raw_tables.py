# Databricks notebook source
# MAGIC %md
# MAGIC ### Create raw tables - part 1

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_raw
# MAGIC LOCATION 'abfss://raw@formulaumdatalake.dfs.core.windows.net/';

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create Circuits Table

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.circuits;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.circuits(
# MAGIC   circuitId INT,
# MAGIC   circuitRef STRING,
# MAGIC   name STRING,
# MAGIC   location STRING,
# MAGIC   country STRING,
# MAGIC   lat DOUBLE,
# MAGIC   lng DOUBLE,
# MAGIC   alt INT,
# MAGIC   url STRING
# MAGIC )
# MAGIC USING csv
# MAGIC OPTIONS (path "abfss://raw@formulaumdatalake.dfs.core.windows.net/circuits.csv", header true);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_raw.circuits;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create Races Table

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.races;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.races(
# MAGIC   raceId INT,
# MAGIC   year INT,
# MAGIC   round INT,
# MAGIC   circuitId INT,
# MAGIC   name STRING,
# MAGIC   date DATE,
# MAGIC   lng DOUBLE,
# MAGIC   alt INT,
# MAGIC   url STRING
# MAGIC )
# MAGIC USING csv
# MAGIC OPTIONS (path "abfss://raw@formulaumdatalake.dfs.core.windows.net/races.csv", header true);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_raw.races;