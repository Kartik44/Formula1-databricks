# Databricks notebook source
# MAGIC %md
# MAGIC #####Read csv by spark dataframe reader

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

v_data_source

# COMMAND ----------

# MAGIC %run "../includes/configurations"
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType

# COMMAND ----------

circuits_schema=StructType(fields=[StructField("circuitID",IntegerType(),False),
                                   StructField("circuitRef",StringType(),True),
                                   StructField("name",StringType(),True),
                                   StructField("location",StringType(),True),
                                   StructField("country",StringType(),True),
                                   StructField("lat",DoubleType(),True),
                                   StructField("lng",DoubleType(),True),
                                   StructField("alt",IntegerType(),True),
                                   StructField("url",StringType(),True),            
                                               
                                               
                                               ])

# COMMAND ----------

circuits_df=spark.read.csv('/mnt/formula1dl1999/raw/circuits.csv',header=True,schema=circuits_schema)

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### select only requd columns
# MAGIC

# COMMAND ----------

circuits_selected_df=circuits_df.select("circuitId","circuitRef","name","location","country","lat","lng","alt")

display(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ######### OTHER WAYS FOR SELECTIING COLUMNS
# MAGIC
# MAGIC #########circuits_selected_df=circuits_df.select(circuits_df.circuitId,circuits_df.circuitRef,circuits_df.name,circuits_df.location,circuits_df.country,circuits_df.lat,circuits_df.lng,circuits_df.alt)
# MAGIC #########display(circuits_selected_df)
# MAGIC
# MAGIC #########circuits_selected_df=circuits_df.select(circuits_df["circuitId"],circuits_df["circuitRef"],circuits_df["name"],circuits_df["location"],circuits_df["country"],circuits_df["lat"],circuits_df["lng"],circuits_df["alt"])
# MAGIC #########display(circuits_selected_df)
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col,lit,current_timestamp

# COMMAND ----------

# USE THIS METHOD FOR GREATER FLEXIBILITY

circuits_selected_df=circuits_df.select(col("circuitId"),col("circuitRef"),col("name"),col("location"),col("country"),col("lat"),col("lng"),col("alt"))
display(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Column Renaming

# COMMAND ----------

circuits_renamed_df=circuits_selected_df.withColumnRenamed("circuitId","circuit_id")\
    .withColumnRenamed("circuitsRef","circuits_ref")\
    .withColumnRenamed("lat","latitude")\
     .withColumnRenamed("lng","longitude")\
          .withColumnRenamed("alt","altitude")\
              .withColumn("data_source",lit(v_data_source))
    
display(circuits_renamed_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

circuits_final_df=circuits_renamed_df.withColumn("ingestion_date",current_timestamp())\
    .withColumn("env",lit("Production"))

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write Data to parquet

# COMMAND ----------

circuits_final_df.write.mode('overwrite').parquet("/mnt/formula1dl1999/processed/circuits")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dl1999/processed/circuits

# COMMAND ----------

df=spark.read.parquet("/mnt/formula1dl1999/processed/circuits")
display(df)

# COMMAND ----------

circuits_final_df.write.mode('overwrite').format('parquet').saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.circuits;

# COMMAND ----------


