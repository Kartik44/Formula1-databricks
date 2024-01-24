# Databricks notebook source
from pyspark.sql.types import StructType, StructField,IntegerType,StringType,DoubleType, DateType

                               
                       

# COMMAND ----------

name_schema=StructType(fields=[StructField("forename",StringType(),True),
                               StructField("surname",StringType(),True)
                               ])

# COMMAND ----------

drivers_schema=StructType(fields=[StructField("driverId",StringType(),True),
                                  StructField("driverRef",StringType(),True),
                                   StructField("number",IntegerType(),True),
                                    StructField("code",StringType(),True),
                                     StructField("name",name_schema),
                                      StructField("dob",DateType(),True),
                                       StructField("nationality",StringType(),True),
                                        StructField("url",StringType(),True),


                               
                               
                               
                               
                               
                               ])

# COMMAND ----------

drivers_df=spark.read.schema(drivers_schema).json("/mnt/formula1dl1999/raw/drivers.json")

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

from pyspark.sql.functions import lit,col,concat,current_timestamp

                               
                       

# COMMAND ----------

drivers_with_columns_df=drivers_df.withColumnRenamed("driverId","driver_id")\
    .withColumnRenamed("driverRef","driver_ref")\
         .withColumn("ingestion_date",current_timestamp())\
                     .withColumn("name",concat(col("name.forename"),lit(' '),col("name.surname")))
             

   


# COMMAND ----------

display(drivers_with_columns_df)

# COMMAND ----------

drivers_final_df=drivers_with_columns_df.drop(col("url"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write file to parquet

# COMMAND ----------

drivers_final_df.write.mode('overwrite').parquet("/mnt/formula1dl1999/processed/drivers")

# COMMAND ----------

drivers_final_df.write.mode('overwrite').format('parquet').saveAsTable("f1_processed.drivers")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.drivers

# COMMAND ----------


