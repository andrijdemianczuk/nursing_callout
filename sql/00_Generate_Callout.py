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

#map short name to lists that will be used for random selection later
facilities=df_facilities.select(df_facilities.Short).rdd.flatMap(lambda x: x).collect()
medunits=df_medunits.select(df_medunits.Short).rdd.flatMap(lambda x: x).collect()

#supporting data lists
shifts = ['Day', 'Evening', 'Night']
reason = ['Sick', 'Family Emergency', 'No-Show', 'Vacation', 'Training', 'PTO']

facilities, medunits, shifts, reason

# COMMAND ----------

calloutDF = pd.DataFrame(columns=("Date", "Shift", "Unit", "Number_Needed", "Reason", "Facility", "Filled"))

for i in range(5):
  calloutRec = [dt.date.today()
    ,random.choice(shifts)
    ,random.choice(medunits)
    ,random.randint(1,5)
    ,random.choice(reason)
    ,random.choice(facilities)
    ,fake.boolean(70)]

  calloutDF.loc[i] = [item for item in calloutRec]
  
calloutDF

# COMMAND ----------

now = dt.datetime.now().strftime("%Y-%m-%d_%H_%M_%s")
calloutDF.to_csv(f"/dbfs/FileStore/Users/andrij.demianczuk@databricks.com/data/hls_source/callouts/out_{now}.csv")

# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/Users/andrij.demianczuk@databricks.com/data/hls_source/callouts/")
