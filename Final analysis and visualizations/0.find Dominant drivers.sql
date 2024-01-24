-- Databricks notebook source
select * from f1_presentation_driver_standings

-- COMMAND ----------

select  * from f1_presentation_races_results

-- COMMAND ----------

select calculated_race_results.name, count(1)as total_races,
sum(calculated_points) as total_points , avg(calculated_points) as avg_points from f1_presentation.calculated_race_results
group by calculated_race_results.name
having count(1)>=50
order by avg_points desc


-- COMMAND ----------

 select calculated_race_results.name, count(1)as total_races,
sum(calculated_points) as total_points , round(avg(calculated_points),2) as avg_points from f1_presentation.calculated_race_results
where calculated_race_results.race_year between 2001 and 2010
group by calculated_race_results.name
having count(1)>=50
order by avg_points desc


-- COMMAND ----------


