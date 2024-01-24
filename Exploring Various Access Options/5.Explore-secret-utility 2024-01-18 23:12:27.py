# Databricks notebook source
dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope='formula1-access-key-scope')

# COMMAND ----------

dbutils.secrets.get(scope='formula1-access-key-scope',key='formula1dl1999-key')

# COMMAND ----------


