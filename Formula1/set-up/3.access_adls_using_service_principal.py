# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using service principal
# MAGIC 1. Register Azure AD Application / Service Principal
# MAGIC 1. Generate a secret/password for the Application 
# MAGIC 1. Set Spark Config with App/Client Id, Directory/Tenant Id & Secret
# MAGIC 1. Assign Role 'Storage Blob Data Contributor' to the Data Lake

# COMMAND ----------

client_id = ""
client_secret = ""
tenant_id = ""

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formulaumdatalake.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formulaumdatalake.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formulaumdatalake.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formulaumdatalake.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formulaumdatalake.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formulaumdatalake.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formulaumdatalake.dfs.core.windows.net/circuits.csv"))