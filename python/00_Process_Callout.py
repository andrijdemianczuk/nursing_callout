# Databricks notebook source
# DBTITLE 1,Initialize global parameters
USER = "andrij.demianczuk@databricks.com"
CATALOG = "main"

# COMMAND ----------

file_path = f"/Users/{USER}/data/hls_source/callouts/"
write_path = f"/Users/{USER}/data/hls_delta/b_callout_stream"
checkpoint_path = f"/Users/{USER}/data/hls_source/callouts_cp/"

raw_df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", checkpoint_path)
    .option("maxFilesPerTrigger", 1)
    .load(file_path)
    .writeStream
    .trigger(availableNow=True)
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .toTable(f"{CATALOG}.hls.b_hls_callout")
)

# COMMAND ----------

df = spark.table(f"{CATALOG}.hls.b_hls_callout")

# COMMAND ----------

display(df)
