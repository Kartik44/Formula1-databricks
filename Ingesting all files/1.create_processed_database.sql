-- Databricks notebook source
create database if not exists f1_processed
location "/mnt/formula1dl1999/processed"

-- COMMAND ----------

desc database f1_processed
