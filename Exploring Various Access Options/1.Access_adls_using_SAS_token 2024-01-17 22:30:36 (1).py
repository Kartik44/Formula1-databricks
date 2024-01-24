# Databricks notebook source
# MAGIC %md

# COMMAND ----------

# MAGIC %md
# MAGIC #Access azure data lake using SAS Token
# MAGIC ####1.Set spark config for SAS Token (get itfrom storagename<<click on 3 dots then generate SAS token give read ,list access)
# MAGIC ###### Can get from storage explorer also same procedure as above remove '?'
# MAGIC ####2.List files from demo container
# MAGIC ####3.read cirsuits.csv file
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

formula1dl_SAS_key=dbutils.secrets.get(scope='formula2dl-SAS-scope',key='formula2dl-SAS')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dl1999.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1dl1999.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1dl1999.dfs.core.windows.net", formula1dl_SAS_key)

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1dl1999.dfs.core.windows.net")

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dl1999.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


