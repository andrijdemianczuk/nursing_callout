# Databricks notebook source
# import the libraries necessary for this demonstration
from faker import Factory
import pandas as pd
import random

import datetime as dt

from faker import Faker
fake = Faker()

# COMMAND ----------

#load lookup tables
df_facilities = spark.table("ademianczuk.hls.l_nc_facilities")
df_medunits = spark.table("ademianczuk.hls.l_nc_medunits")
df_positions = spark.table("ademianczuk.hls.l_nc_positions")

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

calloutDF = pd.DataFrame(columns=("Date", "Shift", "Unit", "Number_Needed", "Reason", "Facility", "Filled"))

for i in range(2000):
  calloutRec = [fake.date_this_year()
    ,random.choice(shifts)
    ,random.choice(medunits)
    ,random.randint(1,5)
    ,random.choice(reason)
    ,random.choice(facilities)
    ,fake.boolean(70)]

  calloutDF.loc[i] = [item for item in calloutRec]
  
calloutDF

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

now = dt.datetime.now().strftime("%Y-%m-%d_%H_%M_%s")

# COMMAND ----------

calloutDF.to_csv(f"/dbfs/FileStore/Users/andrij.demianczuk@databricks.com/data/hls_source/NC_Callouts/out_{now}.csv")
staffDF.to_csv(f"/dbfs/FileStore/Users/andrij.demianczuk@databricks.com/data/hls_source/NC_Staffing/out_{now}.csv")

# COMMAND ----------


