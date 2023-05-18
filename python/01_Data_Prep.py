# Databricks notebook source
# MAGIC %md
# MAGIC ## Health & Life Sciences: Staffing Data Insights
# MAGIC <br/>
# MAGIC <img src="https://imageio.forbes.com/specials-images/imageserve/5dbb4182d85e3000078fddae/0x0.jpg?format=jpg" width="750" />
# MAGIC
# MAGIC ## Introduction
# MAGIC With modernized facilities, growing staff counts and increasing demand for just-in-time healthcare, staff resourcing is a critical asset for all health care providers. In an effort to maximize staff distribution we will examine how Datbricks and the Lakehouse can help organizations drive better staffing outcomes with data-driven decision making.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Why lookup tables?
# MAGIC
# MAGIC Lookup tables help us during the ETL process to provide normalization and deduplication. They can also improve overall performance of the system and reduce the memory footprint of a dataset and decrease the likelihood of data entropy by simplifying data structures.
# MAGIC
# MAGIC The advantage of using a lookup table is that it provides very fast access to the data. Normalizing the data this way allows the table to quickly find the index of the array where the value is stored. This makes it an efficient data structure for applications that require frequent lookups or searching.
# MAGIC
# MAGIC Lookup tables can be used in a variety of applications, including databases, programming languages, and operating systems. They are often used to store configuration settings, perform language translation, and maintain indexes for faster searching in large datasets.
# MAGIC <br/>
# MAGIC <br/>
# MAGIC <img src="https://exceleratorbi.com.au/wp-content/uploads/2016/11/image_thumb-2.png" width=750 />

# COMMAND ----------

# MAGIC %md
# MAGIC # Preparing the lookup tables
# MAGIC
# MAGIC We will be using three lookup tables to help enrich our tables later on. We'll use keyed pairs for filling in details on:
# MAGIC 1. Facility data
# MAGIC 1. Medical Unit data
# MAGIC 1. Position detail data

# COMMAND ----------

display(dbutils.fs.ls("/Users/andrij.demianczuk@databricks.com/data/lookups"))

# COMMAND ----------

#Create or update the facilities table that will be used to join for details
df = spark.read.option("header", True).option("inferSchema", True).csv("/Users/andrij.demianczuk@databricks.com/data/lookups/l_hls_facilities.csv")
df.drop(df._c0).write.format('delta').option("mergeSchema", "true").mode('overwrite').saveAsTable("canada_west.ad.l_hls_facilities")

#Create or update the medical units table that will be used to join for details
df = spark.read.option("header", True).option("inferSchema", True).csv("/Users/andrij.demianczuk@databricks.com/data/lookups/l_hls_medunits.csv")
df.drop(df._c0).write.format('delta').option("mergeSchema", "true").mode('overwrite').saveAsTable("canada_west.ad.l_hls_medunits")

#Create or update the positions table that will be used to join for details
df = spark.read.option("header", True).option("inferSchema", True).csv("/Users/andrij.demianczuk@databricks.com/data/lookups/l_hls_positions.csv")
df.drop(df._c0).write.format('delta').option("mergeSchema", "true").mode('overwrite').saveAsTable("canada_west.ad.l_hls_positions")
