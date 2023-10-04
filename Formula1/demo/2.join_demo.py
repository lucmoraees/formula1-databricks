# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits") \
    .filter("circuit_id < 70") \
    .withColumnRenamed("name", "circuit_name")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races").filter("race_year = 2019") \
    .withColumnRenamed("name", "race_name")

# COMMAND ----------

display(races_df)

# COMMAND ----------

races_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "inner") \
    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(races_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Outer Joins

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Left Join

# COMMAND ----------

races_circuits_left_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "left") \
    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(races_circuits_left_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Right Join

# COMMAND ----------

races_circuits_right_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "right") \
    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(races_circuits_right_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Full outer join

# COMMAND ----------

races_circuits_full_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "full") \
    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(races_circuits_full_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Semi Joins

# COMMAND ----------

races_circuits_semi_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "semi")

# COMMAND ----------

display(races_circuits_semi_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Anti Joins

# COMMAND ----------

races_circuits_anti_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "anti")

# COMMAND ----------

display(races_circuits_anti_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Cross Joins

# COMMAND ----------

races_circuits_cross_df = races_df.crossJoin(circuits_df)

# COMMAND ----------

display(races_circuits_cross_df)