# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM global_temp.gv_race_results
# MAGIC WHERE race_year == 2020