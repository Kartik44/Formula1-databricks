# Databricks notebook source
from pyspark.sql.types import StructType, StructField,IntegerType,StringType,FloatType

                               
                       

# COMMAND ----------

pitstops_schema=StructType(fields=[ StructField("raceId",IntegerType(),False),
                                     StructField("driverId",IntegerType(),True),
                                      StructField("stop",StringType(),True),
                                        StructField("lap",IntegerType(),True),
                                         StructField("time",StringType(),True),
                                         StructField("duration",StringType(),True),
                                           StructField("milliseconds",IntegerType(),True),
                                   
                                
                               
                               
                               
                               ])

# COMMAND ----------

# MAGIC %md
# MAGIC ### MULTLine 

# COMMAND ----------

pitstops_df=spark.read.schema(pitstops_schema).option("multiline",True).json("/mnt/formula1dl1999/raw/pit_stops.json")
display(pitstops_df)

# COMMAND ----------

from pyspark.sql.functions import col,concat,current_timestamp              

# COMMAND ----------

pitstops_final_df=pitstops_df.withColumnRenamed("driverId","driver_id")\
        .withColumnRenamed("raceId","race_id")\
                                    .withColumn("ingestion_date",current_timestamp())
                                   

         
                    
                    
                    
                    
                  
             

   


# COMMAND ----------

pitstops_final_df.write.mode('overwrite').parquet("/mnt/formula1dl1999/processed/pitstops")
display(spark.read.parquet("/mnt/formula1dl1999/processed/pitstops"))

# COMMAND ----------

pitstops_final_df.write.mode('overwrite').format('parquet').saveAsTable("f1_processed.pitstops")



# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from f1_processed.pitstops
