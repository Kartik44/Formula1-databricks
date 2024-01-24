# Databricks notebook source
# MAGIC %md

# COMMAND ----------

# MAGIC %md
# MAGIC #Access azure data lake using Service Principle
# MAGIC ####1.Get client,tenent,secret values fro  key vault
# MAGIC ####2.set spark congfig for app/client_id/tenant_id/secrets.
# MAGIC ####4.Assign role"Storage Blob Data Contributor" to the data lake.
# MAGIC
# MAGIC ###5. Creating just  one scope is workale for all keys
# MAGIC
# MAGIC

# COMMAND ----------

client_id=dbutils.secrets.get(scope='formula1-access-key-scope',key='formula2dl-client-id')
tenant_id=dbutils.secrets.get(scope='formula1-access-key-scope',key='formula1dl-tenant-id')
client_secret=dbutils.secrets.get(scope='formula1-access-key-scope',key='formula1dl-secret-value')

# COMMAND ----------

configs={"fs.azure.account.auth.type":"OAuth",
         "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
         "fs.azure.account.oauth2.client.id": client_id,
         "fs.azure.account.oauth2.client.secret": client_secret,
         "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
         

# COMMAND ----------

dbutils.fs.mount(
    source="abfss://demo@formula1dl1999.dfs.core.windows.net/",
    mount_point="/mnt/formula1dl1999/demo",
    extra_configs=configs)


# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1dl1999/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/formula1dl1999/demo/circuits.csv"))

# COMMAND ----------


