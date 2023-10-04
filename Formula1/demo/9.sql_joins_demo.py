# Databricks notebook source
# MAGIC %sql
# MAGIC USE f1_presentation;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC driver_standings;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW v_driver_standings_2018
# MAGIC AS 
# MAGIC SELECT race_year, driver_name, team, total_points, wins, rank
# MAGIC FROM driver_standings
# MAGIC WHERE race_year = 2018;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM v_driver_standings_2018;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW v_driver_standings_2020
# MAGIC AS 
# MAGIC SELECT race_year, driver_name, team, total_points, wins, rank
# MAGIC FROM driver_standings
# MAGIC WHERE race_year = 2020;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM v_driver_standings_2020;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### INNER JOIN

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC  FROM v_driver_standings_2018 d_2018
# MAGIC  JOIN v_driver_standings_2020 d_2020
# MAGIC   ON (d_2018.driver_name = d_2020.driver_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### LEFT JOIN

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC  FROM v_driver_standings_2018 d_2018
# MAGIC  LEFT JOIN v_driver_standings_2020 d_2020
# MAGIC   ON (d_2018.driver_name = d_2020.driver_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### RIGHT JOIN

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC  FROM v_driver_standings_2018 d_2018
# MAGIC  RIGHT JOIN v_driver_standings_2020 d_2020
# MAGIC   ON (d_2018.driver_name = d_2020.driver_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### FULL JOIN

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC  FROM v_driver_standings_2018 d_2018
# MAGIC  FULL JOIN v_driver_standings_2020 d_2020
# MAGIC   ON (d_2018.driver_name = d_2020.driver_name)