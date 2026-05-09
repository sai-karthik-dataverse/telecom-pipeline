# Databricks notebook source
# silver_subscribers.py
# Cleans and deduplicates subscriber data from bronze -> silver
# Uses SCD Type 1 behavior for subscriber updates

# COMMAND ----------
dbutils.widgets.text("env", "dev")
dbutils.widgets.text("load_date", "")

env = dbutils.widgets.get("env")
load_date = dbutils.widgets.get("load_date")

# COMMAND ----------
storage_account = "datalakestorage01"

bronze_container = "bronze"
silver_container = "silver"

bronze_path = (
    f"abfss://{bronze_container}@{storage_account}.dfs.core.windows.net/"
    f"telecom/subscribers/"
)

silver_path = (
    f"abfss://{silver_container}@{storage_account}.dfs.core.windows.net/"
    f"telecom/subscribers/"
)

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import DoubleType, IntegerType
from delta.tables import DeltaTable

# COMMAND ----------
# Read incremental partition when load_date is passed
if load_date:
    bronze_df = (
        spark.read.format("delta")
        .load(bronze_path)
        .filter(F.col("_load_date") == load_date)
    )
else:
    bronze_df = spark.read.format("delta").load(bronze_path)

# COMMAND ----------
# Standard cleanup / normalization
clean_df = (
    bronze_df
    .withColumn("first_name", F.initcap(F.trim("first_name")))
    .withColumn("last_name", F.initcap(F.trim("last_name")))
    .withColumn("email", F.lower(F.trim("email")))
    .withColumn("state", F.upper(F.trim("state")))
    .withColumn("plan_code", F.upper(F.trim("plan_code")))
    .withColumn("device_type", F.lower(F.trim("device_type")))
    .withColumn("network_type", F.upper(F.trim("network_type")))

    # numeric conversions
    .withColumn(
        "monthly_spend_num",
        F.col("monthly_spend").cast(DoubleType())
    )
    .withColumn(
        "credit_score_num",
        F.col("credit_score").cast(IntegerType())
    )

    # simple email validation
    .withColumn(
        "email_valid",
        F.col("email")
         .rlike(r"^[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}$")
         .cast(IntegerType())
    )

    # date / timestamp parsing
    .withColumn(
        "plan_start_date_ts",
        F.to_timestamp("plan_start_date", "yyyy-MM-dd HH:mm:ss")
    )
    .withColumn(
        "activation_date_ts",
        F.to_timestamp("activation_date", "yyyy-MM-dd HH:mm:ss")
    )
    .withColumn(
        "deactivation_date_ts",
        F.to_timestamp("deactivation_date", "yyyy-MM-dd HH:mm:ss")
    )
    .withColumn(
        "dob_date",
        F.to_date("dob", "yyyy-MM-dd")
    )
    .withColumn(
        "created_at_ts",
        F.to_timestamp("created_at", "yyyy-MM-dd HH:mm:ss")
    )
    .withColumn(
        "updated_at_ts",
        F.to_timestamp("updated_at", "yyyy-MM-dd HH:mm:ss")
    )
)

# COMMAND ----------
# Deduplicate within the incoming batch
# Keep the latest record per subscriber_id
dedup_window = (
    Window
    .partitionBy("subscriber_id")
    .orderBy(
        F.col("updated_at_ts").desc(),
        F.col("_ingested_at").desc()
    )
)

dedup_df = (
    clean_df
    .withColumn("row_num", F.row_number().over(dedup_window))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

# COMMAND ----------
# Data quality checks
# Records are retained and tagged instead of being removed
final_df = (
    dedup_df
    .withColumn(
        "_dq_flags",
        F.array(
            F.when(
                F.col("subscriber_id").isNull(),
                F.lit("NULL_SUBSCRIBER_ID")
            ),
            F.when(
                F.col("msisdn").isNull(),
                F.lit("NULL_MSISDN")
            ),
            F.when(
                F.col("email_valid") == 0,
                F.lit("INVALID_EMAIL")
            ),
            F.when(
                F.col("monthly_spend_num").isNull() &
                F.col("monthly_spend").isNotNull(),
                F.lit("INVALID_SPEND")
            ),
            F.when(
                (F.col("is_active") == 0) &
                F.col("deactivation_date_ts").isNull(),
                F.lit("CHURNED_NO_DEACT_DATE")
            )
        ).cast("array<string>")
    )
    .withColumn(
        "_dq_flags",
        F.array_compact("_dq_flags")
    )
    .withColumn(
        "_has_dq_issues",
        F.size("_dq_flags") > 0
    )
)

# COMMAND ----------
# Merge into silver Delta table
# Existing records are overwritten (SCD Type 1 behavior)
if DeltaTable.isDeltaTable(spark, silver_path):

    silver_table = DeltaTable.forPath(spark, silver_path)

    (
        silver_table.alias("tgt")
        .merge(
            final_df.alias("src"),
            "tgt.subscriber_id = src.subscriber_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

else:

    (
        final_df
        .write
        .format("delta")
        .partitionBy("state")
        .save(silver_path)
    )

print("Silver subscriber load completed")
