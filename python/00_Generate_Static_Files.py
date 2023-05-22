# Databricks notebook source
# MAGIC %pip install faker

# COMMAND ----------

# DBTITLE 1,Initialize global properties
#Replace with the root catalog of your choice. This should be the same catalog where the hls schema was created with the init lookups notebook.
USER = "andrij.demianczuk@databricks.com"
CATALOG = "main"

# COMMAND ----------

# import the libraries necessary for this demonstration
from faker import Factory
import pandas as pd
import random
import datetime as dt

from faker import Faker
fake = Faker()

# COMMAND ----------

#load lookup tables
df_facilities = spark.table(f"{CATALOG}.hls.l_nc_facilities")
df_medunits = spark.table(f"{CATALOG}.hls.l_nc_med_units")
df_positions = spark.table(f"{CATALOG}.hls.l_nc_nurse_positions")

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

#Uncomment the next line if you want to start fresh with a new set of staff
#dbutils.fs.rm(f"dbfs:/Users/{USER}/data/hls_source/staff", True)

# COMMAND ----------

#Write the dataset to a csv file which we'll re-use for loading the Delta Live Tables
now = dt.datetime.now().strftime("%Y-%m-%d_%H_%M_%s")
dbutils.fs.mkdirs(f"dbfs:/Users/{USER}/data/hls_source/staff")
staffDF.to_csv(f"/dbfs/Users/{USER}/data/hls_source/staff/out_{now}.csv")

# COMMAND ----------

#Write the staffing table to delta
sDF = spark.createDataFrame(staffDF)
sDF.coalesce(1)
sDF.write.format('delta').option("mergeSchema", "true").partitionBy("Unit").mode('overwrite').saveAsTable(f"{CATALOG}.hls.b_hls_staff")
