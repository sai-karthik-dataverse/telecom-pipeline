# Databricks notebook source
# bronze_subscribers.py
# raw subscriber ingest -> bronze delta

# COMMAND ----------
dbutils.widgets.text("env", "dev")
dbutils.widgets.text("load_date", "")

env = dbutils.widgets.get("env")
load_date = dbutils.widgets.get("load_date")

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import *
import uuid

# COMMAND ----------
# storage config
storage_account = "datalakestorage01"

raw_container = "raw"
bronze_container = "bronze"

raw_path = (
    f"abfss://{raw_container}@{storage_account}.dfs.core.windows.net/"
    "telecom/subscribers/"
)

bronze_path = (
    f"abfss://{bronze_container}@{storage_account}.dfs.core.windows.net/"
    "telecom/subscribers/"
)

bad_records_path = (
    f"abfss://{raw_container}@{storage_account}.dfs.core.windows.net/"
    "telecom/_bad_records/subscribers/"
)

# COMMAND ----------
# source sends occasional junk values for spend / credit columns
subscriber_schema = StructType([
    StructField("subscriber_id", StringType(), True),
    StructField("msisdn", StringType(), True),
    StructField("account_number", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("dob", StringType(), True),
    StructField("state", StringType(), True),
    StructField("plan_code", StringType(), True),
    StructField("plan_start_date", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("imei", StringType(), True),
    StructField("network_type", StringType(), True),
    StructField("is_active", IntegerType(), True),
    StructField("activation_date", StringType(), True),
    StructField("deactivation_date", StringType(), True),
    StructField("churn_reason", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("monthly_spend", StringType(), True),
    StructField("credit_score", StringType(), True),
    StructField("num_lines", IntegerType(), True),
    StructField("ported_from", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True)
])

# COMMAND ----------
df_raw = (
    spark.read
        .format("csv")
        .option("header", "true")
        .option("badRecordsPath", bad_records_path)
        .schema(subscriber_schema)
        .load(raw_path)
)

# COMMAND ----------
batch_id = str(uuid.uuid4())

df_bronze = (
    df_raw
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        .withColumn(
            "_load_date",
            F.lit(load_date) if load_date else F.current_date().cast("string")
        )
        .withColumn("_batch_id", F.lit(batch_id))
)

# COMMAND ----------
row_count = df_bronze.count()

(
    df_bronze.write
        .format("delta")
        .mode("append")
        .partitionBy("_load_date")
        .save(bronze_path)
)

print(f"bronze load complete - {row_count} rows written")
