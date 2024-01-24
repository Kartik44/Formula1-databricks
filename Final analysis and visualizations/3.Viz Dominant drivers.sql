-- Databricks notebook source
select * from f1_presentation.calculated_race_results

-- COMMAND ----------

select * from f1_presentation_driver_standings

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Dominant nations of the past 20 years 
-- MAGIC

-- COMMAND ----------

select   driver_nationality,race_year,

max(total_points) as total_points 

from f1_presentation_driver_standings
where race_year>=2004
group by race_year,driver_nationality


order by total_points desc


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Drivers with more than 5 wins per season

-- COMMAND ----------

select name,race_year,count(name) from f1_presentation.calculated_race_results
where position=1 
group by race_year,name
having count(name)>5
order by race_year desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Created a view
-- MAGIC

-- COMMAND ----------

create or  replace TEMP VIEW v_dominant_drivers as 
select calculated_race_results.name, count(1)as total_races,
sum(calculated_points) as total_points , avg(calculated_points) as avg_points,
rank() over (order by  avg(calculated_points) desc)  as  driver_rnk
 from f1_presentation.calculated_race_results
group by calculated_race_results.name
having count(1)>=50
order by avg_points desc

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##### Most dominant drivers in the history

-- COMMAND ----------

select name,race_year ,
count(1) as total_races,
sum(calculated_points) as total_points , 
avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
where name in (select  name from v_dominant_drivers where driver_rnk<=10)
group by name,race_year
order by race_year,avg_points desc


-- COMMAND ----------

select name,race_year ,
count(1) as total_races,
sum(calculated_points) as total_points , 
avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
where name in (select  name from v_dominant_drivers where driver_rnk<=10)
group by name,race_year
order by race_year,avg_points desc


-- COMMAND ----------

select name,race_year ,
count(1) as total_races,
sum(calculated_points) as total_points , 
avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
where name in (select  name from v_dominant_drivers where driver_rnk<=10)
group by name,race_year
order by race_year,avg_points desc


-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pandas as pd
-- MAGIC %matplotlib inline
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC import seaborn as sns
-- MAGIC

-- COMMAND ----------

sns.distplot (cars.hp)
