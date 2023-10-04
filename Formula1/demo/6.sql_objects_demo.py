# Databricks notebook source
# MAGIC %md
# MAGIC ##### Lesson Objects
# MAGIC 1. Create managed table using Python
# MAGIC 2. Create managed table using SQL
# MAGIC 3. Effect of dropping a managed table
# MAGIC 4. Describe table

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %python
# MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

# MAGIC %python
# MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE demo;
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC EXTENDED race_results_python;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE race_results_sql
# MAGIC AS 
# MAGIC SELECT *
# MAGIC   FROM demo.race_results_python
# MAGIC   WHERE race_year = 2018;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED race_results_sql

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE race_results_python;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE demo;