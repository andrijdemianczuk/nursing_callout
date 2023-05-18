# Databricks notebook source
file_path = "/FileStore/Users/andrij.demianczuk@databricks.com/data/hls_source/callouts/"
write_path = "/FileStore/Users/andrij.demianczuk@databricks.com/data/hls_delta/b_callout_stream"
checkpoint_path = "/dbfs/FileStore/Users/andrij.demianczuk@databricks.com/data/hls_source/callouts_cp/"

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
    .toTable("ademianczuk.hls.b_autoloader_test")
)

# COMMAND ----------

df = spark.table("ademianczuk.hls.b_autoloader_test")

# COMMAND ----------

display(df)
