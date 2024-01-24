# Databricks notebook source
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

def add_injestion_date(input_df):
    output_df=input_df.withcolumn("injestion_date",current_timestamp())
    return output_df

# COMMAND ----------


