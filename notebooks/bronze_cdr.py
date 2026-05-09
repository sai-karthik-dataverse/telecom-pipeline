# Databricks notebook source
# bronze layer ingestion for telecom datasets
# handles:
#   1. cdr csv files
#   2. support ticket json files

# COMMAND ----------
dbutils.widgets.text("env", "dev")
dbutils.widgets.text("load_date", "")
dbutils.widgets.text("source", "cdr")

env = dbutils.widgets.get("env")
load_date = dbutils.widgets.get("load_date")
source = dbutils.widgets.get("source")

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------
storage_account = "datalakestorage01"

raw_container = "raw"
bronze_container = "bronze"

base_raw = f"abfss://{raw_container}@{storage_account}.dfs.core.windows.net/telecom"
base_bronze = f"abfss://{bronze_container}@{storage_account}.dfs.core.windows.net/telecom"

paths = {
    "cdr": {
        "raw_path": f"{base_raw}/cdr/",
        "bronze_path": f"{base_bronze}/cdr/",
        "bad_path": f"{base_raw}/_bad_records/cdr/"
    },

    "tickets": {
        "raw_path": f"{base_raw}/support_tickets/",
        "bronze_path": f"{base_bronze}/support_tickets/",
        "bad_path": f"{base_raw}/_bad_records/support_tickets/"
    }
}

cfg = paths[source]

# COMMAND ----------
# keeping everything as string initially except anomaly flag
# type cleanup can happen later downstream

cdr_schema = StructType([
    StructField("cdr_id", StringType(), True),
    StructField("subscriber_id", StringType(), True),
    StructField("call_type", StringType(), True),
    StructField("call_start_ts", StringType(), True),
    StructField("call_end_ts", StringType(), True),
    StructField("duration_seconds", StringType(), True),
    StructField("data_mb", StringType(), True),
    StructField("originating_number", StringType(), True),
    StructField("terminating_number", StringType(), True),
    StructField("tower_id", StringType(), True),
    StructField("network_type", StringType(), True),
    StructField("roaming_country", StringType(), True),
    StructField("call_cost", StringType(), True),
    StructField("rating_status", StringType(), True),
    StructField("is_anomaly", IntegerType(), True),
    StructField("event_date", StringType(), True)
])

# COMMAND ----------
if source == "cdr":

    df_raw = (
        spark.read
            .format("csv")
            .option("header", "true")
            .option("badRecordsPath", cfg["bad_path"])
            .schema(cdr_schema)
            .load(cfg["raw_path"])
    )

    partition_col = "event_date"

else:

    df_raw = (
        spark.read
            .option("badRecordsPath", cfg["bad_path"])
            .json(cfg["raw_path"])
    )

    # tickets don't have direct partition field
    df_raw = df_raw.withColumn(
        "_partition_date",
        F.to_date("created_at").cast("string")
    )

    partition_col = "_partition_date"

# COMMAND ----------
ingest_date = (
    F.lit(load_date)
    if load_date
    else F.current_date().cast("string")
)

df_bronze = (
    df_raw
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_load_date", ingest_date)
)

# COMMAND ----------
(
    df_bronze.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .partitionBy(partition_col)
        .save(cfg["bronze_path"])
)

# COMMAND ----------
# lightweight monitoring only
# avoid count() on very large datasets in real prod jobs

row_count = df_bronze.count()

print(f"[bronze][{source}] rows written = {row_count}")