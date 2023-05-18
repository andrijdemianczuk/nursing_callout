# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://delta.io/static/delta-sharing-logo-13e769d1a6148b9cc2655f97e90feab5.svg" />

# COMMAND ----------

# DBTITLE 1,First, let's make sure the dependencies are installed. This can also be done at the cluster-level too.
# MAGIC %pip install delta-sharing

# COMMAND ----------

#Bring the library in scope
import delta_sharing

# COMMAND ----------

# DBTITLE 1,Identify where we stored the downloaded credentials file
#This file was placed in the following location after we downloaded the file when we created the receiver.
creds = "dbfs:/Users/andrij.demianczuk@databricks.com/shares/ademianczuk_int_config.share"

# COMMAND ----------

# DBTITLE 1,Let's quickly preview the credentials to make sure they're legit
# MAGIC %fs head /Users/andrij.demianczuk@databricks.com/shares/ademianczuk_int_config.share

# COMMAND ----------

# DBTITLE 1,Test the connection, and view the shared objects
#Location of our profile credentials
#This is the same location as above, but with the file API format instead
profile_file = '/dbfs/Users/andrij.demianczuk@databricks.com/shares/ademianczuk_int_config.share'

# Create a SharingClient
client = delta_sharing.SharingClient(profile_file)

# List all shared tables.
client.list_all_tables()

# COMMAND ----------

# DBTITLE 1,Format the output of the share objects - this is just for readability
shares = client.list_shares()

for share in shares:
  schemas = client.list_schemas(share)
  for schema in schemas:
    tables = client.list_tables(schema)
    for table in tables:
      print(f'name = {table.name}, share = {table.share}, schema = {table.schema}')

# COMMAND ----------

# DBTITLE 1,Define the location of the credentials and table we want to use
profile_file = '/Users/andrij.demianczuk@databricks.com/shares/ademianczuk_int_config.share'
table_url = f"{profile_file}#hls_ad_internal.ad.g_hls_unfilled_summary"

# COMMAND ----------

# DBTITLE 1,Using the location and credentials, let's create a simple dataframe from the delta share we have access to
from pyspark.sql.functions import *
from pyspark.sql.types import *

#Now let's view our dataframe we would be using for further ETL / enrichment
from pyspark.sql.functions import sum, col, count

#This includes the location of the delta sharing server, auth token and table we want to query.
df = delta_sharing.load_as_spark(table_url)

df = (df
    .withColumn("DOW", col("DOW").cast(IntegerType()))
    .withColumn("Date", col("Date").cast(DateType()))
    .withColumn("Number_Needed", col("Number_Needed").cast(IntegerType()))
    )

display(df
    .groupBy("DOW", "Shift", "Unit", "Facility")
    .agg(sum("Number_Needed").alias("Number_Needed"))
    .orderBy(col("Number_Needed").desc())
    )
