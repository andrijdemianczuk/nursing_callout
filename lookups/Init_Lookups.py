# Databricks notebook source
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

# COMMAND ----------

# Print the path
import sys
import os

print("\n".join(sys.path))
sys.path.append(os.path.abspath('/Workspace/Repos/andrij.demianczuk@databricks.com/nursing_callout'))

lookups = ['NC_Med_Units', 'NC_Facilities', 'NC_Nurse_Positions']

for i in lookups:
    df=spark.read.csv(f"file:{os.getcwd()}/" + i + ".csv", header=True) # "file:" prefix and absolute file path are required for PySpark
    display(df)

# COMMAND ----------

class blah():
    try:
        def __init__(self, name:str = "John Doe"):
            self.greet(name)

        def greet(self, name: str) -> str:
            print("hi, " + name + "!")
    except:
        print("Invalid Exceptional Error")

print(blah())

# COMMAND ----------


