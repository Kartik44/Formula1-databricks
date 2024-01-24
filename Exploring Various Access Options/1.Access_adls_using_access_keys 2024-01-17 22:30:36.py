# Databricks notebook source
# MAGIC %md

# COMMAND ----------

# MAGIC %md
# MAGIC #Access azure data lake using Access Keys
# MAGIC ## 1.Set spark config fs.azure.account.key
# MAGIC ##2.List files fromdemo container
# MAGIC ##3.read cirsuits.csv file
# MAGIC
# MAGIC

# COMMAND ----------

formula1dl_account_key=dbutils.secrets.get(scope='formula1-access-key-scope',key='formula1dl1999-key')

# COMMAND ----------

spark.conf.set(

    "fs.azure.account.key.formula1dl1999.dfs.core.windows.net",
    formula1dl_account_key
)

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1dl1999.dfs.core.windows.net")

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dl1999.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


