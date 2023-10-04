# Databricks notebook source
# MAGIC %sql
# MAGIC USE f1_processed;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *, CONCAT(driver_ref, '-', code) AS new_driver_ref
# MAGIC FROM drivers;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *, SPLIT(name, ' ')[0] forname, SPLIT(name, ' ')[1] surname 
# MAGIC FROM drivers;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *, current_timestamp 
# MAGIC FROM drivers;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *, date_format(dob, 'dd-MM-yyyy') 
# MAGIC FROM drivers;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *, date_add(dob, 1)
# MAGIC FROM drivers;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT MAX(dob)
# MAGIC FROM drivers;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM drivers WHERE dob = '2000-05-11'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM drivers
# MAGIC WHERE nationality = 'British'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT nationality, COUNT(*) as total
# MAGIC FROM drivers
# MAGIC GROUP BY nationality
# MAGIC HAVING total > 100
# MAGIC ORDER BY total DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT nationality, name, dob, RANK() OVER(PARTITION BY nationality ORDER BY dob DESC) AS age_rank
# MAGIC FROM drivers

# COMMAND ----------

