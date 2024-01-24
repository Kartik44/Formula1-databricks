# Databricks notebook source
from pyspark.sql.types import StructType, StructField,IntegerType,StringType,FloatType

                               
                       

# COMMAND ----------

results_schema=StructType(fields=[StructField("resultId",IntegerType(),True),
                                  StructField("raceId",IntegerType(),True),
                                   StructField("driverId",IntegerType(),True),
                                    StructField("constructorId",IntegerType(),True),
                                     StructField("number",IntegerType(),True),
                                      StructField("grid",IntegerType(),True),
                                       StructField("position",IntegerType(),True),
                                        StructField("positionText",StringType(),True),
                                        StructField("positionOrder",IntegerType(),True),
                                        StructField("points",FloatType(),True),
                                        StructField("laps",IntegerType(),True),
                                        StructField("time",StringType(),True),
                                        StructField("milliseconds",IntegerType(),True),
                                        StructField("fastestLap",IntegerType(),True),
                                        StructField("rank",IntegerType(),True),
                                        StructField("fastestLapTime",StringType(),True),
                                        StructField("fastestLapSpeed",FloatType(),True),
                                        StructField("statusId",StringType(),True)




                               
                               
                               
                               
                               
                               ])

# COMMAND ----------

results_df=spark.read.schema(results_schema).json("/mnt/formula1dl1999/raw/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Rename columns

# COMMAND ----------

from pyspark.sql.functions import col,concat,current_timestamp

                               
                       

# COMMAND ----------

results_with_columns_df=results_df.withColumnRenamed("driverId","driver_id")\
    .withColumnRenamed("resultId","result_id")\
        .withColumnRenamed("raceId","race_id")\
            .withColumnRenamed("constructorId","constructor_id")\
                .withColumnRenamed("positionText","position_text")\
                    .withColumnRenamed("positionOrder","position_order")\
                        .withColumnRenamed("fastestLap","fastest_lap")\
                            .withColumnRenamed("fastestLapTime","fastest_lap_time")\
                                .withColumnRenamed("fastestLapSpeed","fastest_lap_speed")\
                                    .withColumn("ingestion_date",current_timestamp())
                                   

         
                    
                    
                    
                    
                  
             

   


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Drop unwanted column

# COMMAND ----------

results_final_df=results_with_columns_df.drop(col("statusId"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write to parquet
# MAGIC

# COMMAND ----------

results_final_df.write.mode('overwrite').partitionBy('race_id').parquet('/mnt/formula1dl1999/processed/results')

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1dl1999/processed/results"))

# COMMAND ----------

results_final_df.write.mode('overwrite').format('parquet').saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.results
