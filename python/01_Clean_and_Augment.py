# Databricks notebook source
# MAGIC %md
# MAGIC ## Health & Life Sciences: Staffing Data Insights
# MAGIC <br/>
# MAGIC <img src="https://imageio.forbes.com/specials-images/imageserve/5dbb4182d85e3000078fddae/0x0.jpg?format=jpg" width="750" />

# COMMAND ----------

# DBTITLE 1,Initialize global parameters
CATALOG = "main"

# COMMAND ----------

# MAGIC %md
# MAGIC # Cleaning & Augmenting the Data
# MAGIC
# MAGIC <img src="https://images.prismic.io/coresignal-website/c3225f2b-9991-4ebe-a5a6-26542a929052_Data%20Transformation%20Benefits_%20Types,%20and%20Processes.jpg?ixlib=gatsbyFP&auto=compress%2Cformat&fit=max&q=75" width=500 />
# MAGIC
# MAGIC Cleaning and augmenting data is a critical step for a successful ETL pipeline. This step of the pipeline can perform a number of tasks that are critical to the integrity of the resultant data. This also has implications on what data and which tables are shared or can be forked safely. Data cleaning and augmentation may occur over several tasks in a typical pipeline granting developers atomic access to the data at any point. The repeatability of this process is based on the rules defined on the structure and the tenacity of the data schema. In the event that data structure changes, Databricks supports schema evolution preserving any datapoints that change convention.

# COMMAND ----------

# DBTITLE 1,Imports & dependencies
from pyspark.sql.functions import avg, col
from pyspark import pandas as pd
from pyspark.sql.types import DoubleType

# COMMAND ----------

# MAGIC %md
# MAGIC ### Weighting dimensions
# MAGIC
# MAGIC Dimensional weights are one way to normalize values across a given axis. This strategy is similar to a dispersion gradient but puts equal measure on values between -1 and 1 (or any similar denomination). The intent is to amplify the relationship between two known dimensions as a measure of intensity or cause.

# COMMAND ----------

# DBTITLE 1,Preview the initial bronze (raw) table
display(spark.sql(f"SELECT * FROM {CATALOG}.hls.b_hls_staff"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating weight based on hours worked
# MAGIC
# MAGIC Weighting based on hours worked on average is the simplest factor when determining criteria for 'heavy' and 'light' workers. Workers that average both more hours per week and hours per year based on the group median are considered 'heavy' workers and thus more likely to accept a callout. Workers with fewer than the average in both categories are considered 'light' and generally less likely to accept a callout.
# MAGIC
# MAGIC **Caution!**
# MAGIC Although we are ascribing a weighting based on hours worked there are many other factors that will likely result in an effective callout. Since we are in the data transformation stage of the pipeline and not the DSML stage, this relationship is strictly and early indicator. We would want to do an evaluation on feature importance and bias to help us determine the likelihood of a target candidate accepting a callout request.
# MAGIC
# MAGIC This weighting is for demonstration purposes only.

# COMMAND ----------

# DBTITLE 1,Figure out the weights for all staff based on hours worked
#Read the source table
df = spark.table(f"{CATALOG}.hls.b_hls_staff")

#Identify the overall average of hours for all staff - this will be used for weighting
averageYear = df.select(avg("HrsThisYear")).collect()[0][0] #returns the value of the first row & first column
averageYear = float("{:.2f}".format(averageYear))
averageYear = float(averageYear)

#Identify the overall average of weekly hours for all staff - this will be used for secondary weighting
averageWeek = df.select(avg("AvgWeeklyHrs")).collect()[0][0]
averageWeek = float("{:.2f}".format(averageWeek)) 
averageWeek = float(averageWeek)

#Create the augmented dataframe with weights
df = df.withColumn("HrsThisYearWeight", col("HrsThisYear")/averageYear).withColumn("AvgWeeklyWeight", col("AvgWeeklyHrs")/averageWeek)

#Write the dataframe to a new table with the paritions set
df.write.format('delta').option("mergeSchema", "true").partitionBy("Unit", "Shift").mode('overwrite').saveAsTable(f"{CATALOG}.hls.s_hls_staff_augmented")

# COMMAND ----------

# DBTITLE 1,Join the medical unit & facilities data for the callouts table
#Create our three datafames that we will be joining for our augmented callouts table
calloutDF = spark.table(f"{CATALOG}.hls.b_hls_callout").drop("_rescued_data")
facilitiesDF = (spark.table(f"{CATALOG}.hls.l_nc_facilities")
    .withColumnRenamed("Short", "fShort")
    .withColumnRenamed("Long", "fLong")
    .withColumnRenamed("Description", "fDescription"))
medunitsDF = (spark.table(f"{CATALOG}.hls.l_nc_med_units")
    .withColumnRenamed("Short", "mShort")
    .withColumnRenamed("Description", "mDescription"))

#Join the dataframes and write to a new delta table
calloutDF = calloutDF.join(facilitiesDF, calloutDF.Facility == facilitiesDF.fShort).join(medunitsDF, calloutDF.Unit == medunitsDF.mShort)
calloutDF.write.format('delta').option("mergeSchema", "true").partitionBy("Facility").mode('overwrite').saveAsTable(f"{CATALOG}.hls.s_hls_callout_augmented")

# COMMAND ----------

# DBTITLE 1,Create a filter silver table with only unfilled callouts
#Load and save only the unfilled callouts as a new delta table
unfilledCalloutsDF = spark.table(f"{CATALOG}.hls.s_hls_callout_augmented").filter(col("Filled") == "False")
unfilledCalloutsDF.write.format('delta').option("mergeSchema", "true").partitionBy("Facility").mode('overwrite').saveAsTable(f"{CATALOG}.hls.s_hls_unfilled_callouts")
