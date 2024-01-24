# Databricks notebook source
from pyspark.sql.types import StructType, StructField,IntegerType,StringType,FloatType


# COMMAND ----------

laptimes_schema=StructType(fields=[ StructField("raceId",IntegerType(),False),
                                     StructField("driverId",IntegerType(),True),
                                        StructField("lap",IntegerType(),True),
                                         StructField("position",IntegerType(),True),
                                           StructField("time",StringType(),True),
                                             StructField("milliseconds",IntegerType(),True),
                                   
                                
                               
                               
                               
                               ])

# COMMAND ----------

# MAGIC %md
# MAGIC ### MULTLine 

# COMMAND ----------

laptimes_df=spark.read.schema(laptimes_schema).csv("/mnt/formula1dl1999/raw/lap_times")
display(laptimes_df)

# COMMAND ----------

from pyspark.sql.functions import col,concat,current_timestamp 
             

# COMMAND ----------

laptimes_final_df=laptimes_df.withColumnRenamed("driverId","driver_id")\
        .withColumnRenamed("raceId","race_id")\
                                    .withColumn("ingestion_date",current_timestamp())
                                   

         
                    
                    
                    
                    
                  
             

   


# COMMAND ----------

laptimes_final_df.write.mode('overwrite').parquet("/mnt/formula1dl1999/processed/laptimes")
display(spark.read.parquet("/mnt/formula1dl1999/processed/laptimes"))

# COMMAND ----------

laptimes_final_df.write.mode('overwrite').format('parquet').saveAsTable("f1_processed.laptime")



# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from f1_processed.laptime
