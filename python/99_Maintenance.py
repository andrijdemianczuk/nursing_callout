# Databricks notebook source
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS ademianczuk.lhe.b_autoloader_test;
# MAGIC DROP TABLE IF EXISTS ademianczuk.lhe.b_callout;
# MAGIC DROP TABLE IF EXISTS ademianczuk.lhe.b_staff;

# COMMAND ----------

df = spark.table("ademianczuk.lhe.nc_med_units")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE ademianczuk.lhe;
# MAGIC SHOW TABLES;
