# Databricks notebook source
# MAGIC %sql
# MAGIC create database if not exists f1_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists f1_raw.circuits;
# MAGIC create table if not exists f1_raw.circuits(circuitId int, 
# MAGIC                                           circuitRef string,
# MAGIC                                           name string,
# MAGIC                                           location string,
# MAGIC                                           country string,
# MAGIC                                           lat double,
# MAGIC                                           lng double,
# MAGIC                                           alt int,
# MAGIC                                           url string
# MAGIC
# MAGIC                                           )
# MAGIC using csv
# MAGIC options (path "/mnt/formula1dl1999/raw/circuits.csv",header true)

# COMMAND ----------

# MAGIC %sql
# MAGIC select* from f1_raw.circuits

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create races 

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists f1_raw.races;
# MAGIC create table if not exists f1_raw.races(raceId int, 
# MAGIC                                         year int,
# MAGIC                                         round int,
# MAGIC                                         circuitId string,
# MAGIC                                         name string,
# MAGIC                                         date Date,
# MAGIC                                         time string,
# MAGIC                                         url string
# MAGIC
# MAGIC                                           )
# MAGIC using csv
# MAGIC options (path "/mnt/formula1dl1999/raw/races.csv",header true)

# COMMAND ----------

# MAGIC %sql
# MAGIC select* from f1_raw.races

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create tables for json files 
# MAGIC ###### 1.constructors 2.drivers

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists f1_raw.constructors;
# MAGIC create table if not exists f1_raw.constructors(constructorId int, 
# MAGIC                                         constructorRef string,
# MAGIC                                         name string,
# MAGIC                                         nationality string,
# MAGIC                                         url string
# MAGIC
# MAGIC                                           )
# MAGIC using json
# MAGIC options (path "/mnt/formula1dl1999/raw/constructors.json",header true)

# COMMAND ----------

# MAGIC %sql
# MAGIC select* from f1_raw.constructors

# COMMAND ----------

# MAGIC %md
# MAGIC ##### create drivers table

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists f1_raw.drivers;
# MAGIC create table if not exists f1_raw.drivers(driverId int, 
# MAGIC                                         driverRef string,
# MAGIC                                         number int,
# MAGIC                                         code string,
# MAGIC                                         name STRUCT<forename: STRING, surname: STRING>,
# MAGIC                                         dob date,
# MAGIC                                         nationality string,
# MAGIC                                         url string
# MAGIC
# MAGIC                                           )
# MAGIC using json
# MAGIC options (path "/mnt/formula1dl1999/raw/drivers.json",header true)

# COMMAND ----------

# MAGIC %sql
# MAGIC select* from f1_raw.drivers

# COMMAND ----------

# MAGIC %md
# MAGIC ##### create results table
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists f1_raw.results;
# MAGIC create table if not exists f1_raw.results(resultId Int,
# MAGIC                                   raceId int,
# MAGIC                                    driverId  int,
# MAGIC                                     constructorId int,
# MAGIC                                      number Int,
# MAGIC                                       grid Int,
# MAGIC                                        position Int,
# MAGIC                                         positionText String,
# MAGIC                                         positionOrder Int,
# MAGIC                                         points Float,
# MAGIC                                         laps Int,
# MAGIC                                         time String,
# MAGIC                                         milliseconds Int,
# MAGIC                                         fastestLap Int,
# MAGIC                                         rank Int,
# MAGIC                                         fastestLapTime String,
# MAGIC                                         fastestLapSpeed Float,
# MAGIC                                         statusId String
# MAGIC
# MAGIC )
# MAGIC using json
# MAGIC options (path "/mnt/formula1dl1999/raw/results.json",header true)

# COMMAND ----------

# MAGIC %sql
# MAGIC select* from f1_raw.results

# COMMAND ----------

# MAGIC %md
# MAGIC ##### create pitstops

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists f1_raw.pitstops;
# MAGIC create table if not exists f1_raw.pitstops( driverId  int,
# MAGIC                                             duration string,
# MAGIC                                             lap int,
# MAGIC                                             milliseconds int,
# MAGIC                                             raceId int,
# MAGIC                                             stop int,
# MAGIC                                             time string
# MAGIC                                     
# MAGIC                                         
# MAGIC
# MAGIC )
# MAGIC using json
# MAGIC options (path "/mnt/formula1dl1999/raw/pitstops.json",multiline True)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_raw.pitstops

# COMMAND ----------

# MAGIC %md
# MAGIC #### create lap times table

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists f1_raw.lap_times;
# MAGIC create table if not exists f1_raw.lap_times( raceId int,
# MAGIC                                             driverId  int,
# MAGIC                                             lap int,
# MAGIC                                             position int,
# MAGIC                                             time string,
# MAGIC                                             milliseconds int
# MAGIC                                     
# MAGIC                                         
# MAGIC
# MAGIC )
# MAGIC using csv
# MAGIC options (path "/mnt/formula1dl1999/raw/lap_times")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from f1_raw.lap_times

# COMMAND ----------

# MAGIC %md
# MAGIC ##### create qualfying file

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists f1_raw.qualifying;
# MAGIC create table if not exists f1_raw.qualifying( constructorId int,
# MAGIC                                             driverId  int,
# MAGIC                                             number int,
# MAGIC                                             position int,
# MAGIC                                             q1 string,
# MAGIC                                             q2 string,
# MAGIC                                             q3 string,
# MAGIC                                             qualifyingId int,
# MAGIC                                             raceId int
# MAGIC
# MAGIC )
# MAGIC using json
# MAGIC options (path "/mnt/formula1dl1999/raw/qualifying",multiLine true)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_raw.qualifying

# COMMAND ----------


