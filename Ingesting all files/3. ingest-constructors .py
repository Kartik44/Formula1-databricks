# Databricks notebook source
#Rather than using structfield and structType

constructors_schema="constructorId INT,constructorRef STRING,name STRING ,nationality STRING, url STRING"

# COMMAND ----------

constructors_df=spark.read.schema(constructors_schema).json("/mnt/formula1dl1999/raw/constructors.json")
display(constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Drop unwanted columns
# MAGIC ####### col can also be used instead of table name
# MAGIC
# MAGIC

# COMMAND ----------

constructors_dropped_df=constructors_df.drop(constructors_df['url'])
display(constructors_dropped_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructors_final_df=constructors_dropped_df.withColumnRenamed("constructorId","constructor_id") \
    .withColumnRenamed("constructorRef","constructor_ref")  \
    .withColumn("ingestion_date",current_timestamp())

                                             
                                             
display(constructors_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write to parquet

# COMMAND ----------

constructors_final_df.write.mode('overwrite').parquet("/mnt/formula1dl1999/processed/constructors")

# COMMAND ----------

constructors_final_df.write.mode('overwrite').format('parquet').saveAsTable("f1_processed.constructors")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.constructors;
