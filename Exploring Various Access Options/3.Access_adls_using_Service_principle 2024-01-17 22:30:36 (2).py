# Databricks notebook source
# MAGIC %md

# COMMAND ----------

# MAGIC %md
# MAGIC #Access azure data lake using Service Principle
# MAGIC ####1.Register at azure active directory under "Registrations"
# MAGIC ####2.Generate a secret/password for the application
# MAGIC ####3.set spark congfig for app/client_id/tenant_id/secrets.
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


spark.conf.set("fs.azure.account.auth.type.formula1dl1999.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1dl1999.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1dl1999.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dl1999.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dl1999.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1dl1999.dfs.core.windows.net")

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dl1999.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


