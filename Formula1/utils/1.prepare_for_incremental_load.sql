-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Drop all the tables

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_processed CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION 'abfss://processed@formulaumdatalake.dfs.core.windows.net/';

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_presentation CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION 'abfss://presentation@formulaumdatalake.dfs.core.windows.net/'; 