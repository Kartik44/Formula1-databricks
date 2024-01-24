-- Databricks notebook source
create or  replace TEMP VIEW v_dominant_teams as 
select calculated_race_results.name1, count(1)as total_races,
sum(calculated_points) as total_points , avg(calculated_points) as avg_points,
rank() over (order by  avg(calculated_points) desc)  as team_rnk
 from f1_presentation.calculated_race_results
group by calculated_race_results.name1
having count(1)>=100
order by avg_points desc


-- COMMAND ----------

select name1,race_year,count(name1) from f1_presentation.calculated_race_results
where position=1 
group by race_year,name1
having count(name1)>5
order by race_year desc

-- COMMAND ----------

select name1,race_year ,
count(1) as total_races,
sum(calculated_points) as total_points , 
avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
where name1 in (select  name1 from v_dominant_teams where team_rnk<=5)
group by name1,race_year
order by race_year,avg_points desc


-- COMMAND ----------

select name1,race_year ,
count(1) as total_races,
sum(calculated_points) as total_points , 
avg(calculated_points) as avg_points
from f1_presentation.calculated_race_results
where name1 in (select  name1 from v_dominant_teams where team_rnk<=5)
group by name1,race_year
order by race_year,avg_points desc


-- COMMAND ----------


