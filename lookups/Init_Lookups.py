# Databricks notebook source
# DBTITLE 1,Global Variables
USER = "andrij.demianczuk@databricks.com"
CATALOG = "main"

#Definition of lookup titles. DO NOT CHANGE!
lookups = ['NC_Med_Units', 'NC_Facilities', 'NC_Nurse_Positions']

# COMMAND ----------

# MAGIC %md
# MAGIC This is an example of a non-iterated read line for a csv file in a repo
# MAGIC
# MAGIC ```python
# MAGIC df=spark.read.csv(f"file:{os.getcwd()}/NC_Facilities.csv", header=True) # "file:" prefix and absolute file path are required for PySpark
# MAGIC display(df)
# MAGIC ```
# MAGIC
# MAGIC Alternatively, if we were to use a direct link from git, this would be another option as well:
# MAGIC
# MAGIC ```python
# MAGIC import urllib
# MAGIC
# MAGIC urllib.request.urlretrieve("https://raw.githubusercontent.com/andrijdemianczuk/nursing_callout/25d5989fa1d7b5c92a346e1510b0b6ef51d94cc5/lookups/NC_Nurse_Positions.csv", "/tmp/NC_Nurse_Positions.csv")
# MAGIC urllib.request.urlretrieve("https://raw.githubusercontent.com/andrijdemianczuk/nursing_callout/25d5989fa1d7b5c92a346e1510b0b6ef51d94cc5/lookups/NC_Facilities.csv", "/tmp/NC_Facilities.csv")
# MAGIC urllib.request.urlretrieve("https://raw.githubusercontent.com/andrijdemianczuk/nursing_callout/25d5989fa1d7b5c92a346e1510b0b6ef51d94cc5/lookups/NC_Med_Units.csv", "/tmp/NC_Med_Units.csv")
# MAGIC
# MAGIC lookups = ['NC_Med_Units', 'NC_Facilities', 'NC_Nurse_Positions']
# MAGIC
# MAGIC for i in lookups:
# MAGIC     df = spark.read.format("csv").option("header", True).load("file:/tmp/" + i + ".csv") 
# MAGIC     display(df)
# MAGIC ```
# MAGIC
# MAGIC Another option is to use an iterator for the csv files inline. This is the method we will be creating as a class / object feature:
# MAGIC ```python
# MAGIC # Print the path
# MAGIC import sys
# MAGIC import os
# MAGIC
# MAGIC print("\n".join(sys.path))
# MAGIC sys.path.append(os.path.abspath('/Workspace/Repos/andrij.demianczuk@databricks.com/nursing_callout'))
# MAGIC
# MAGIC lookups = ['NC_Med_Units', 'NC_Facilities', 'NC_Nurse_Positions']
# MAGIC
# MAGIC for i in lookups:
# MAGIC     df=spark.read.csv(f"file:{os.getcwd()}/" + i + ".csv", header=True) # "file:" prefix and absolute file path are required for PySpark
# MAGIC     display(df)
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Create the HLS schema if one doesn't already exist
#NOTE: Users must have WRITE privileges to the catalog in defined above
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.hls")

# COMMAND ----------

# DBTITLE 1,Iterate and read each lookup CSV and commit it as a delta table
import sys
import os

class NurseLookups():
    '''
    We'll need to PyDoc this class
    '''
    try:
        
        def __init__(self, user:str):
            '''
            user is a required input parameter when instancing this object. This defines under which user the current repository lives.
            '''
            self.user = user

        def appendPath(self):
            '''
            Ensures that the repo path is part of the Python path
            '''
            sys.path.append(os.path.abspath('/Workspace/Repos/' + self.user + '/nursing_callout'))
            #print("\n".join(sys.path)) #un-comment for path debugging

        
        def compileLookups(self, lookups:list):
            '''
            A simple iterator that reads the input list and builds a set of delta tables off of them. This is matched to the file names
            '''
            for i in lookups:
                df=spark.read.csv(f"file:{os.getcwd()}/" + i + ".csv", header=True)
                df.write.format('delta').option("mergeSchema", "true").mode('overwrite').saveAsTable(f"{CATALOG}.hls.l_" + i.lower())


    except Exception as err:
        print(err)

n = NurseLookups(user=USER)
n.appendPath()
n.compileLookups(lookups=lookups)
