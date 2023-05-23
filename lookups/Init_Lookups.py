# Databricks notebook source
# MAGIC %md
# MAGIC # Health & Life Sciences: Staffing Data Insights
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

# DBTITLE 1,Global Variables
USER = "<user.name@databricks.com>"
CATALOG = "main"

#Definition of lookup titles. DO NOT CHANGE!
lookups = ['NC_Med_Units', 'NC_Facilities', 'NC_Nurse_Positions']

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparing the lookup tables
# MAGIC
# MAGIC We will be using three lookup tables to help enrich our tables later on. We'll use keyed pairs for filling in details on:
# MAGIC 1. Facility data
# MAGIC 1. Medical Unit data
# MAGIC 1. Position detail data

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

#Create an instance of our NurseLookups() object and call upon the necessary functions to create our lookup delta files from the stored CSV files
n = NurseLookups(user=USER)
n.appendPath()
n.compileLookups(lookups=lookups)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Other ways to refer to external CSV files for import
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
