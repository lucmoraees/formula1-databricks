# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using SAS token
# MAGIC 1. Set the spark config for SAS token
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuits.csv file

# COMMAND ----------

formula1dl_demo_sas_token = dbutils.secrets.get(scope='formula1-scope', key='formula1dl-demo-token-sas')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formulaumdatalake.dfs.core.windows.net", "SAS")

spark.conf.set("fs.azure.sas.token.provider.type.formulaumdatalake.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")

spark.conf.set("fs.azure.sas.fixed.token.formulaumdatalake.dfs.core.windows.net", formula1dl_demo_sas_token)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formulaumdatalake.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formulaumdatalake.dfs.core.windows.net/circuits.csv"))