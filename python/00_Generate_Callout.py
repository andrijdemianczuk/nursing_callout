# Databricks notebook source
# MAGIC %pip install faker pytz

# COMMAND ----------

# DBTITLE 1,Initialize global parameters
USER = "andrij.demianczuk@databricks.com"
CATALOG = "main"

# COMMAND ----------

# import the libraries necessary for this demonstration
from faker import Factory
import pandas as pd
import random
import pytz
import datetime as dt

from faker import Faker
fake = Faker()

# COMMAND ----------

#load lookup tables
df_facilities = spark.table(f"{CATALOG}.hls.l_nc_facilities")
df_medunits = spark.table(f"{CATALOG}.hls.l_nc_med_units")

#map short name to lists that will be used for random selection later
facilities=df_facilities.select(df_facilities.Short).rdd.flatMap(lambda x: x).collect()
medunits=df_medunits.select(df_medunits.Short).rdd.flatMap(lambda x: x).collect()

#supporting data lists
shifts = ['Day', 'Evening', 'Night']
reason = ['Sick', 'Family Emergency', 'No-Show', 'Vacation', 'Training', 'PTO']

facilities, medunits, shifts, reason

# COMMAND ----------

calloutDF = pd.DataFrame(columns=("Date", "Shift", "Unit", "Number_Needed", "Reason", "Facility", "Filled"))

#If you would rather back-date entries for callouts, you can replace the datetime from now, to the following example:
#dt.datetime.now(pytz.timezone('US/Mountain')).strftime("%Y-%m-%d %H:%M:%S")
#fake.date_time_this_year()

#Modify the range input value (int) if you need more / fewer callouts
for i in range(50):
  calloutRec = [dt.datetime.now(pytz.timezone('US/Mountain')).strftime("%Y-%m-%d %H:%M:%S")
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
dbutils.fs.mkdirs(f"dbfs:/Users/{USER}/data/hls_source/callouts")
calloutDF.to_csv(f"/dbfs/Users/{USER}/data/hls_source/callouts/out_{now}.csv")

# COMMAND ----------

display(dbutils.fs.ls(f"dbfs:/Users/{USER}/data/hls_source/callouts/"))
