# Databricks notebook source
# MAGIC %md
# MAGIC ### Call all ingestion notebooks

# COMMAND ----------

def run_notebooks(notebooks_names, ):
    for notebook in notebooks_names:
        status_result = dbutils.notebook.run(notebook, 0, {'p_data_source': 'Ergast API', 'p_file_date': '2021-04-18'})
        if status_result == "success":
            print(f"{notebook}: Success")
        else:
            print(f"{notebook}: Failed")

# COMMAND ----------

notebooks_names = [
    "1.ingest_circuits_file",
    "2.ingest_races_file",
    "3.ingest_constructors_file",
    "4.ingest_drivers_file",
    "5.ingest_results_file",
    "6.ingest_pit_stops_file",
    "7.ingest_lap_times_file",
    "8.ingest_qualifying_file"
]

# COMMAND ----------

run_notebooks(notebooks_names)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, count(1)
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id

# COMMAND ----------

2021-03-21
2021-03-28
2021-04-18