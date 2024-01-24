# Databricks notebook source
# MAGIC %md

# COMMAND ----------

# MAGIC %md
# MAGIC ####Access azure data lake using cluster scoped authentication
# MAGIC ####### Cluster scoped didn't work as my cluster settings were diffrent from video
# MAGIC #### Tried new method called 'Credential Passthrough' 
# MAGIC ####### 1.edit cluster enable the passthrough checkbox
# MAGIC ####### 2.go to storage account<<IAM<<Assign storage blob role to user(member) Kartik Patil 
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

client_id="8f1ad84b-ef28-41f2-b2dd-b27bc12432a9"
tenant_id="e3ac2efc-3123-41ba-9c56-12bd07b67e67"
client_secret="42S8Q~K5F5nmehNpKUJ5Ig453tCNIBD46r.jybTO"

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


