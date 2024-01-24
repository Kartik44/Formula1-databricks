# Databricks notebook source
from pyspark.sql.types import StructType, StructField,IntegerType,StringType,FloatType


# COMMAND ----------

qualifying_schema=StructType(fields=[ StructField("qualifyId",IntegerType(),False),
                                   StructField("raceId",IntegerType(),False),
                                     StructField("driverId",IntegerType(),True),
                                        StructField("constructorId",IntegerType(),True),
                                        StructField("number",IntegerType(),True),
                                         StructField("position",IntegerType(),True),
                                           StructField("q1",StringType(),True),
                                             StructField("q2",StringType(),True),
                                              StructField("q3",StringType(),True),
                                   
                                
                               
                               
                               
                               ])

# COMMAND ----------

# MAGIC %md
# MAGIC ### MULTLine 

# COMMAND ----------

qualifying_df=spark.read.schema(qualifying_schema).option("multiline",True).json("/mnt/formula1dl1999/raw/qualifying")
display(qualifying_df)


# COMMAND ----------

from pyspark.sql.functions import col,concat,current_timestamp 
             

# COMMAND ----------

qualifying_final_df=qualifying_df.withColumnRenamed("driverId","driver_id")\
        .withColumnRenamed("raceId","race_id")\
                .withColumnRenamed("qualifyId","qualify_id")\
                        .withColumnRenamed("constructorId","constructor_id")\
                                    .withColumn("ingestion_date",current_timestamp())
                                   

         
                    
                    
                    
                    
                  
             

   


# COMMAND ----------

qualifying_final_df.write.mode('overwrite').parquet("/mnt/formula1dl1999/processed/qualifying")
display(spark.read.parquet("/mnt/formula1dl1999/processed/qualifying"))

# COMMAND ----------

qualifying_final_df.write.mode('overwrite').format('parquet').saveAsTable("f1_processed.qualifying")



# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from f1_processed.laptime

# COMMAND ----------


