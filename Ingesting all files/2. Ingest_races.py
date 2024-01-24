# Databricks notebook source
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType,DateType

# COMMAND ----------

races_schema=StructType(fields=[StructField("raceId",IntegerType(),False),
                                   StructField("year",IntegerType(),True),
                                   StructField("round",IntegerType(),True),
                                   StructField("circuitId",IntegerType(),True),
                                   StructField("name",StringType(),True),
                                   StructField("date",DateType(),True),
                                   StructField("time",StringType(),True),
                                   StructField("url",StringType(),True),            
                                               
                                               
                                               ])

# COMMAND ----------

races_df=spark.read.csv("/mnt/formula1dl1999/raw/races.csv",header=True,schema=races_schema)
display(races_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit,to_timestamp,col,concat

# COMMAND ----------

races_with_timestamp_df=races_df.withColumn("ingestion_date",current_timestamp())\
    .withColumn("race_timestamp",to_timestamp(concat(col('date'),lit(' '),col('time')),'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

display(races_with_timestamp_df)

# COMMAND ----------

races_selected_df=races_with_timestamp_df.select(col('raceId').alias('race_id'),col('year').alias('race_year'),col('round').alias('circuit_id'),col('name'),col('ingestion_date'),col('race_timestamp'))

display(races_selected_df)

# COMMAND ----------

races_selected_df.write.mode('overwrite').parquet('/mnt/formula1dl1999/processed/races')

# COMMAND ----------

display(spark.read.parquet('/mnt/formula1dl1999/processed/races'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Partition by year (check in storage explorer)

# COMMAND ----------

races_selected_df.write.mode('overwrite').partitionBy('race_year').parquet('/mnt/formula1dl1999/processed/races')

# COMMAND ----------

display(spark.read.parquet('/mnt/formula1dl1999/processed/races'))

# COMMAND ----------

races_selected_df.write.mode('overwrite').format('parquet').saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.races;
