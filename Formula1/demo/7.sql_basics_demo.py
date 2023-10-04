# Databricks notebook source
# MAGIC %sql
# MAGIC SHOW DATABASES;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_database()

# COMMAND ----------

# MAGIC %sql
# MAGIC USE f1_processed;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM drivers
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC drivers;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM drivers
# MAGIC WHERE nationality = "German"
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT name, dob
# MAGIC FROM drivers
# MAGIC WHERE (nationality = 'German'
# MAGIC   AND dob >= '1990-01-01'
# MAGIC   OR nationality = 'Indian'
# MAGIC )
# MAGIC ORDER BY dob DESC;