-- Databricks notebook source
select calculated_race_results.name1, count(1)as total_races,
sum(calculated_points) as total_points , avg(calculated_points) as avg_points from f1_presentation.calculated_race_results
group by calculated_race_results.name1
having count(1)>=100
order by avg_points desc


-- COMMAND ----------

 select calculated_race_results.name1, count(1)as total_races,
sum(calculated_points) as total_points , round(avg(calculated_points),2) as avg_points from f1_presentation.calculated_race_results
where calculated_race_results.race_year between 2011 and 2020
group by calculated_race_results.name1
having count(1)>=100
order by avg_points desc


-- COMMAND ----------

 select calculated_race_results.name1, count(1)as total_races,
sum(calculated_points) as total_points , round(avg(calculated_points),2) as avg_points from f1_presentation.calculated_race_results
where calculated_race_results.race_year between 2001 and 2010
group by calculated_race_results.name1
having count(1)>=100
order by avg_points desc


-- COMMAND ----------


