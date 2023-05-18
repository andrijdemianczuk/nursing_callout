# Databricks notebook source
# MAGIC %md
# MAGIC ## Health & Life Sciences: Staffing Data Insights
# MAGIC <br/>
# MAGIC <img src="https://imageio.forbes.com/specials-images/imageserve/5dbb4182d85e3000078fddae/0x0.jpg?format=jpg" width="750" />

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://upload.wikimedia.org/wikipedia/commons/thumb/f/f3/Apache_Spark_logo.svg/1200px-Apache_Spark_logo.svg.png" width=250 />
# MAGIC
# MAGIC # Aggregate the data and build reporting tables
# MAGIC
# MAGIC Data aggregation is important to do as late-stage as possible. Although we typically want to aggregate on all data points for a given dataset we also want to reduce the dimensionality as much as possible beforehand to improve processing times. Filtering data early-stage is often the fastest and easiest way to see overall performance gains in a transformation pipeline. In the case of Apache Spark, Filtering and Mapping data is a parallelized function and scales linearly with worker and slot counts.

# COMMAND ----------

from pyspark.sql import functions as fn
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Adding a feature column
# MAGIC Sometimes it's helpful to pre-emptively create a feature column or binning group to help classify data points. This is especially true if we have large datasets with high-cardinality data. A simple way to reduce cardinality is to introduce a feature column that we can expect to have significance on how we partition data and / or how we classify it later on. Day-of-week is a simple extrapolation we can use where we know we'll have 7 bins of data (0 through 6). Alternatively if the data set is large enough we may want to consider day-of-month or day-of-year based on timestamp. If we encode these feature columns early or mid-stage it reduces the amount of feature engineering we'll have to do later on at the DSML / AI stage.

# COMMAND ----------

# DBTITLE 1,Enrich the unfilled callouts with day-of-week
#Create the initial dataframe
df = spark.table("canada_west.ad.s_hls_unfilled_callouts")

#Perform transformations (extract DOW)
df = (df
    .withColumn("DOW", fn.date_format(fn.col("Date"), "F").cast(IntegerType()))
    .select("DOW", "Date", "Shift", "Unit", "Number_Needed", "Reason", "Facility"))

#Write the final dataframe to a gold table
(df.write.format('delta')
    .option("overwriteSchema", "true")
    .partitionBy("Facility")
    .mode('overwrite')
    .saveAsTable("canada_west.ad.g_hls_unfilled_summary"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create aggregations to reduce data depth
# MAGIC
# MAGIC Aggregations are critical for reporting. If we report on our un-filtered or non-aggregated data BI tools and visualizations can become overwhelmed or messy. To avoid this we want to create aggregations downstream and late(r)-stage after we've filtered, mapped and cleaned our data. Understanding the business requirements are key to building quality aggregate (gold) tables. This is often where logic and tables are forked.

# COMMAND ----------

# DBTITLE 1,Aggregate staffing availability into a single table
#Create the initial dataframe
df = spark.table("canada_west.ad.s_hls_staff_augmented")

#Count by Credentials, Shifts and Unit
df = (df.select("Credentials", "Shift", "Unit")
    .groupBy("Credentials", "Shift", "Unit").count())

#Write the dataframe to a gold table
(df.write.format('delta')
    .option("overwriteSchema", "true")
    .mode('overwrite')
    .saveAsTable("canada_west.ad.g_hls_staff_availability"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Organizing our data into well-defined structures
# MAGIC Sometimes we can take advantage of certain characteristics or behaviors of APIs. In this case, we can take advantage of the pyspark functions api to convert a timestamp into a date. This will convert all atomic timestamps to the YYYY-MM-dd format, omitting the time of the callout. This is important since it's an easy aggregation group we can apply as a byproduct of the APIs behavior.

# COMMAND ----------

# DBTITLE 1,Isolate the staff required each day
#Create the initial dataframe
df = spark.table("canada_west.ad.s_hls_callout_augmented")

#Handle our data types
df = (df.withColumn("Date", df.Date.cast(TimestampType()))
    .withColumn("Number_Needed", df.Number_Needed.cast(IntegerType())))

#Figure out the sum needed of each type by date, shift, unit an facility
df = (df.groupBy("Date", "Shift", "Unit", "Facility")
    .agg(fn.sum("Number_Needed").alias("Total_Needed")))

#Write the dataframe to a gold table
(df.write.format('delta')
    .option("overwriteSchema", "true")
    .mode('overwrite')
    .saveAsTable("canada_west.ad.g_hls_staff_absences"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Separating the heavy and light workers
# MAGIC Based on our logic in the previous notebook to create a weighting based on hours worked we can now quickly and easily separate the workers into one of two categories.
# MAGIC
# MAGIC ### Building out better insights
# MAGIC It is important to note a point of potential improvement - we can further improve data reliability by tracking whether or not a worker fulfills a requested callout. This would help us evolve the decision structure around who would be likely to fill a request.

# COMMAND ----------

# DBTITLE 1,Create a table with only the heavy workers
#Create the initial dataframe
df = spark.table("canada_west.ad.s_hls_staff_augmented")

#Select only workers above the average
df = (df.where((fn.col("HrsThisYearWeight") > 1)))

#Write the dataframe to a gold table
(df.write.format('delta')
    .option("overwriteSchema", "true")
    .mode('overwrite')
    .saveAsTable("canada_west.ad.g_hls_heavy_workers"))

# COMMAND ----------

# DBTITLE 1,Create a table with only the light workers
#Create the initial dataframe
df = spark.table("canada_west.ad.s_hls_staff_augmented")

#Select only workers above the average
df = (df.where((fn.col("HrsThisYearWeight") < 1)))

#Write the dataframe to a gold table
(df.write.format('delta')
    .option("overwriteSchema", "true")
    .mode('overwrite')
    .saveAsTable("canada_west.ad.g_hls_light_workers"))
