# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC # GLPI Reservations — Silver to Gold (current snapshot + KPIs)
# MAGIC
# MAGIC Enrichment:
# MAGIC - Join to Silver users to add user login + full name (like tickets)
# MAGIC - Join to Silver reserveditems to add reservation_item_name
# MAGIC
# MAGIC Outputs (Gold Delta):
# MAGIC - reservations_enriched_current
# MAGIC - KPI tables:
# MAGIC   - cards_current
# MAGIC   - by_month
# MAGIC   - by_user_current
# MAGIC   - by_item_current
# MAGIC   - invalid_current

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Access & Configuration

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

storage_account = "startupvillagedatalake"
scope_name = "kv-startupvillage"   

client_id = dbutils.secrets.get(scope=scope_name, key="sp-client-id")
client_secret = dbutils.secrets.get(scope=scope_name, key="sp-client-secret")
tenant_id = dbutils.secrets.get(scope=scope_name, key="tenant-id")

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(
    f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
)
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", client_secret)
spark.conf.set(
    f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net",
    f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paths

# COMMAND ----------

SILVER_ROOT = "abfss://silver@startupvillagedatalake.dfs.core.windows.net/glpi"
GOLD_ROOT = "abfss://gold@startupvillagedatalake.dfs.core.windows.net/reservations"

silver_res_path = f"{SILVER_ROOT}/reservations"
silver_users_path = f"{SILVER_ROOT}/users"
silver_reserveditems_path = "abfss://silver@startupvillagedatalake.dfs.core.windows.net/glpi/reserveditems"

gold_res_enriched_path = f"{GOLD_ROOT}/reservations_enriched_current"

gold_kpi_cards_path = f"{GOLD_ROOT}/reservations_kpis/cards_current"
gold_kpi_peak_period_path = f"{GOLD_ROOT}/reservations_kpis/peak_period_current"
gold_kpi_trends_by_month_path = f"{GOLD_ROOT}/reservations_kpis/trends_by_month"
gold_kpi_by_item_path = f"{GOLD_ROOT}/reservations_kpis/by_item_current"
gold_kpi_by_user_path = f"{GOLD_ROOT}/reservations_kpis/by_user_current"
gold_kpi_duration_dist_path = f"{GOLD_ROOT}/reservations_kpis/duration_distribution_current"
gold_kpi_invalid_path = f"{GOLD_ROOT}/reservations_kpis/invalid_current"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Silver

# COMMAND ----------

df_r = spark.read.format("delta").load(silver_res_path)

df_u = spark.read.format("delta").load(silver_users_path).select(
    col("user_id"),
    col("user_name").alias("user_login"),
    col("full_name").alias("user_full_name"),
)

df_ri = spark.read.format("delta").load(silver_reserveditems_path).select(
    col("reservation_item_id"),
    col("reservation_item_name"),
).dropDuplicates(["reservation_item_id"])

print("Reservations rows:", df_r.count())
print("Users rows:", df_u.count())
print("Reserveditems rows:", df_ri.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enrich + derived columns

# COMMAND ----------

df_gold = (
    df_r
    .join(df_u, on="user_id", how="left")
    .join(df_ri, on="reservation_item_id", how="left")
    .withColumn("_ingest_gold_ts", current_timestamp())
    .withColumn("start_date", to_date(col("start_ts")))
    .withColumn("start_month", date_format(col("start_ts"), "yyyy-MM"))
    .withColumn("start_year", year(col("start_ts")))
    .withColumn("start_hour", hour(col("start_ts")).cast("int"))
)

# COMMAND ----------

# Build hour window label "HH:00-HH+2:00" for peak period (2-hour window)
# Example: 9 -> "09:00-11:00"
df_gold = df_gold.withColumn(
    "start_hour_2h_window",
    concat(
        lpad(col("start_hour").cast("string"), 2, "0"), lit(":00-"),
        lpad(((col("start_hour") + lit(2)) % lit(24)).cast("string"), 2, "0"), lit(":00")
    )
)

# COMMAND ----------

df_gold_out = df_gold.select(
    "reservation_id",
    "user_id",
    "user_login",
    "user_full_name",
    "reservation_item_id",
    "reservation_item_name",
    "group_id",
    "start_ts",
    "end_ts",
    "duration_hours",
    "is_invalid_duration",
    "comment",
    "start_date",
    "start_month",
    "start_year",
    "start_hour",
    "start_hour_2h_window",
    "ingestion_date",
    "_source_file",
    "_source_system",
    "_ingest_bronze_ts",
    "_ingest_silver_ts",
    "_ingest_gold_ts",
)

# COMMAND ----------

df_gold_out.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Gold enriched (current)

# COMMAND ----------

(df_gold_out.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(gold_res_enriched_path)
)

print("✅ Wrote:", gold_res_enriched_path, "rows:", df_gold_out.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## KPI Tables

# COMMAND ----------

build_ts = current_timestamp()
dfp = df_gold_out

# Exclude invalid duration rows from hours-based KPIs (reserved_hours, avg_duration)
valid_duration = (col("is_invalid_duration") == lit(False)) & col("duration_hours").isNotNull() & (col("duration_hours") >= 0)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ---------------------------
# MAGIC ### KPI: Cards (global current)
# MAGIC ### ---------------------------

# COMMAND ----------

kpi_cards = (
    dfp.agg(
        count(lit(1)).alias("total_reservations"),
        sum(when(valid_duration, col("duration_hours")).otherwise(lit(0.0))).alias("reserved_hours"),
        avg(when(valid_duration, col("duration_hours")).otherwise(lit(None).cast("double"))).alias("avg_duration_hours"),
        sum(when(col("is_invalid_duration") == True, 1).otherwise(0)).alias("invalid_duration_count"),
        countDistinct("reservation_item_id").alias("distinct_items"),
        countDistinct("user_id").alias("distinct_users"),
    )
    .withColumn("gold_kpi_build_ts", build_ts)
)

# COMMAND ----------

(kpi_cards.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .save(gold_kpi_cards_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### --------------------------------
# MAGIC ### KPI: Peak period (2-hour window)
# MAGIC ### --------------------------------

# COMMAND ----------


kpi_peak_period = (
    dfp.where(col("start_hour_2h_window").isNotNull())
       .groupBy("start_hour_2h_window")
       .agg(count(lit(1)).alias("reservations_count"))
       .orderBy(col("reservations_count").desc(), col("start_hour_2h_window"))
       .limit(1)
       .withColumn("gold_kpi_build_ts", build_ts)
)

# COMMAND ----------

(kpi_peak_period.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .save(gold_kpi_peak_period_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### --------------------------------------------
# MAGIC ### KPI: Trends by month (reservations + hours)
# MAGIC ### --------------------------------------------

# COMMAND ----------


kpi_trends_by_month = (
    dfp.where(col("start_month").isNotNull())
       .groupBy("start_month")
       .agg(
           count(lit(1)).alias("reservations_count"),
           sum(when(valid_duration, col("duration_hours")).otherwise(lit(0.0))).alias("reserved_hours"),
           avg(when(valid_duration, col("duration_hours")).otherwise(lit(None).cast("double"))).alias("avg_duration_hours"),
           sum(when(col("is_invalid_duration") == True, 1).otherwise(0)).alias("invalid_duration_count"),
       )
       .withColumn("gold_kpi_build_ts", build_ts)
       .orderBy("start_month")
)

# COMMAND ----------

(kpi_trends_by_month.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .save(gold_kpi_trends_by_month_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### -----------------------------------------
# MAGIC ### KPI: By item/room id (current top demand)
# MAGIC ### -----------------------------------------

# COMMAND ----------


kpi_by_item = (
    dfp.groupBy("reservation_item_id", "reservation_item_name")
       .agg(
           count(lit(1)).alias("reservations_count"),
           sum(when(valid_duration, col("duration_hours")).otherwise(lit(0.0))).alias("reserved_hours"),
           avg(when(valid_duration, col("duration_hours")).otherwise(lit(None).cast("double"))).alias("avg_duration_hours"),
       )
       .withColumn("gold_kpi_build_ts", build_ts)
       .orderBy(col("reservations_count").desc(), col("reservation_item_id"))
)

# COMMAND ----------

(kpi_by_item.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .save(gold_kpi_by_item_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ----------------------------
# MAGIC ### KPI: By user (current usage)
# MAGIC ### ----------------------------

# COMMAND ----------

kpi_by_user = (
    dfp.groupBy("user_id", "user_login", "user_full_name")
       .agg(
           count(lit(1)).alias("reservations_count"),
           sum(when(valid_duration, col("duration_hours")).otherwise(lit(0.0))).alias("reserved_hours"),
           avg(when(valid_duration, col("duration_hours")).otherwise(lit(None).cast("double"))).alias("avg_duration_hours"),
       )
       .withColumn("gold_kpi_build_ts", build_ts)
       .orderBy(col("reservations_count").desc())
)

# COMMAND ----------

(kpi_by_user.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .save(gold_kpi_by_user_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### -----------------------------------
# MAGIC ### KPI: Duration distribution (current)
# MAGIC ### Buckets: <1h, 1-2h, 2-4h, 4-8h, >8h
# MAGIC ### -----------------------------------

# COMMAND ----------

df_duration_bucketed = (
    dfp.where(valid_duration)
       .withColumn(
           "duration_bucket",
           when(col("duration_hours") < 1, lit("<1h"))
           .when((col("duration_hours") >= 1) & (col("duration_hours") < 2), lit("1-2h"))
           .when((col("duration_hours") >= 2) & (col("duration_hours") < 4), lit("2-4h"))
           .when((col("duration_hours") >= 4) & (col("duration_hours") < 8), lit("4-8h"))
           .otherwise(lit(">8h"))
       )
)

kpi_duration_dist = (
    df_duration_bucketed.groupBy("duration_bucket")
                        .agg(count(lit(1)).alias("reservations_count"))
                        .withColumn("gold_kpi_build_ts", build_ts)
)

# COMMAND ----------

(kpi_duration_dist.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .save(gold_kpi_duration_dist_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### -------------------------------------
# MAGIC ### KPI: Invalid durations detail (current)
# MAGIC ### -------------------------------------

# COMMAND ----------


kpi_invalid = (
    dfp.where(col("is_invalid_duration") == True)
       .select(
           "reservation_id",
           "user_id",
           "user_login",
           "user_full_name",
           "reservation_item_id",
           "reservation_item_name",
           "start_ts",
           "end_ts",
           "ingestion_date",
           "_source_file",
       )
       .withColumn("gold_kpi_build_ts", build_ts)
)

# COMMAND ----------

(kpi_invalid.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .save(gold_kpi_invalid_path)
)

# COMMAND ----------

print("✅ Wrote reservations KPIs under:", f"{GOLD_ROOT}/reservations_kpis/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick validation (optional)

# COMMAND ----------

display(spark.read.format("delta").load(gold_kpi_cards_path))
display(spark.read.format("delta").load(gold_kpi_peak_period_path))
display(spark.read.format("delta").load(gold_kpi_trends_by_month_path).orderBy("start_month"))
