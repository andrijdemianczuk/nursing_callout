# Databricks notebook source
# import the libraries necessary for this demonstration
from faker import Factory
import pandas as pd
import random
import datetime as dt

from faker import Faker
fake = Faker()

# COMMAND ----------

# MAGIC %sql
# MAGIC USE canada_west.ad;
# MAGIC SHOW TABLES;

# COMMAND ----------

#load lookup tables
df_facilities = spark.table("canada_west.ad.l_hls_facilities")
df_medunits = spark.table("canada_west.ad.l_hls_medunits")
df_positions = spark.table("canada_west.ad.l_hls_positions")

# COMMAND ----------

#map short name to lists that will be used for random selection later
facilities=df_facilities.select(df_facilities.Short).rdd.flatMap(lambda x: x).collect()
medunits=df_medunits.select(df_medunits.Short).rdd.flatMap(lambda x: x).collect()
positions=df_positions.select(df_positions.Short).rdd.flatMap(lambda x: x).collect()

#supporting data lists
shifts = ['Day', 'Evening', 'Night']
reason = ['Sick', 'Family Emergency', 'No-Show', 'Vacation', 'Training', 'PTO']

facilities, medunits, positions, shifts, reason

# COMMAND ----------

staffDF = pd.DataFrame(columns=("FName", "LName", "Credentials", "DateAvailable", "Shift", "Unit", "Phone", "SSN", "HrsThisYear", "AvgWeeklyHrs"))

for i in range(1000):
  staff = [fake.first_name_nonbinary()
    ,fake.last_name()
    ,random.choice(positions)
    ,fake.date_this_month()
    ,random.choice(shifts)
    ,random.choice(medunits)
    ,fake.phone_number()
    ,fake.ssn()
    ,fake.random_int(250, 560)
    ,fake.random_int(30, 50)]

  staffDF.loc[i] = [item for item in staff]

staffDF

# COMMAND ----------

#Write the dataset to a csv file which we'll re-use for loading the Delta Live Tables
now = dt.datetime.now().strftime("%Y-%m-%d_%H_%M_%s")
staffDF.to_csv(f"/dbfs/Users/andrij.demianczuk@databricks.com/data/hls_source/staff/out_{now}.csv")

# COMMAND ----------

#Write the staffing table to delta
sDF = spark.createDataFrame(staffDF)
sDF.coalesce(1)
sDF.write.format('delta').option("mergeSchema", "true").partitionBy("Unit").mode('overwrite').saveAsTable("canada_west.ad.b_hls_staff")
