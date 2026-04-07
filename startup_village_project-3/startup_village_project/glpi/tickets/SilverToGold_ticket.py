# Databricks notebook source
# MAGIC %md
# MAGIC # Data Access And Configuration

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

dbutils.fs.ls("abfss://silver@startupvillagedatalake.dfs.core.windows.net/glpi/")

# COMMAND ----------

SILVER_ROOT = "abfss://silver@startupvillagedatalake.dfs.core.windows.net/glpi"
GOLD_ROOT = "abfss://gold@startupvillagedatalake.dfs.core.windows.net/tickets"

# COMMAND ----------

tickets_path   = f"{SILVER_ROOT}/tickets"
users_path     = f"{SILVER_ROOT}/users"

status_path    = f"{SILVER_ROOT}/ticket_status"
priority_path  = f"{SILVER_ROOT}/ticket_priority"
type_path      = f"{SILVER_ROOT}/ticket_type"
category_path  = f"{SILVER_ROOT}/ticket_category"
location_path  = f"{SILVER_ROOT}/ticket_location"

# COMMAND ----------

# MAGIC %md
# MAGIC #Read silver Tickets DATA

# COMMAND ----------

df_t = spark.read.format("delta").load(tickets_path)
df_u = spark.read.format("delta").load(users_path)

# COMMAND ----------

df_status   = spark.read.format("delta").load(status_path)
df_priority = spark.read.format("delta").load(priority_path)
df_type     = spark.read.format("delta").load(type_path)
df_category = spark.read.format("delta").load(category_path)
df_location = spark.read.format("delta").load(location_path)

# COMMAND ----------

df_type.display()

# COMMAND ----------

df_status.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Prepare role-playing user dims

# COMMAND ----------

df_users_small = df_u.select("user_id", "user_name", "full_name")

# COMMAND ----------

df_req = (
    df_users_small
    .withColumnRenamed("user_id", "requester_user_id")
    .withColumnRenamed("user_name", "requester_login")
    .withColumnRenamed("full_name", "requester_full_name")
)

# COMMAND ----------

df_upd = (
    df_users_small
    .withColumnRenamed("user_id", "last_updater_user_id")
    .withColumnRenamed("user_name", "last_updater_login")
    .withColumnRenamed("full_name", "last_updater_full_name")
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Enrich tickets with labels + user full names

# COMMAND ----------

df_gold = (
    df_t
    .join(df_status,   on="status_code",   how="left")
    .join(df_priority, on="priority_code", how="left")
    .join(df_type,     on="type_code",     how="left")
    .join(df_category, on="category_id",   how="left")
    .join(df_location, on="location_id",   how="left")
    .join(df_req,      on="requester_user_id",    how="left")
    .join(df_upd,      on="last_updater_user_id", how="left")
)

# COMMAND ----------

df_gold.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Add Gold metrics/flags

# COMMAND ----------

df_gold = (
    df_gold
    .withColumn("_ingest_gold_ts", current_timestamp())
    .withColumn(
        "is_open",
        when(col("closed_ts").isNull() & col("solved_ts").isNull(), lit(1)).otherwise(lit(0))
    )
    .withColumn(
        "time_to_solve_hours",
        when(col("solved_ts").isNull() | col("created_ts").isNull(), lit(None).cast("double"))
        .when(col("solved_ts") < col("created_ts"), lit(None).cast("double"))  # invalid negative duration
        .otherwise((unix_timestamp(col("solved_ts")) - unix_timestamp(col("created_ts"))) / lit(3600.0))
    )
    .withColumn(
        "time_to_close_hours",
        when(col("closed_ts").isNull() | col("created_ts").isNull(), lit(None).cast("double"))
        .when(col("closed_ts") < col("created_ts"), lit(None).cast("double"))  # invalid negative duration
        .otherwise((unix_timestamp(col("closed_ts")) - unix_timestamp(col("created_ts"))) / lit(3600.0))
    )
    .withColumn(
        "is_invalid_solve_duration",
        (
            col("solved_ts").isNotNull()
            & col("created_ts").isNotNull()
            & (col("solved_ts") < col("created_ts"))
        ).cast("boolean")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Select final schema

# COMMAND ----------

df_gold_out = df_gold.select(
    "ticket_id",
    "title",

    "category_id",
    "category",

    "location_id",
    "location",

    "requester_user_id",
    "requester_login",
    "requester_full_name",

    "last_updater_user_id",
    "last_updater_login",
    "last_updater_full_name",
    
    "type_code",
    "type",


    "status_code",
    "status",
    "is_open",
    "is_deleted",
    "priority_code",
    "priority",


    "created_ts",
    "updated_ts",
    "solved_ts",
    "closed_ts",    
    "time_to_solve_hours",
    "time_to_close_hours",
    
    "_source_file",
    "_source_system",
    "ingestion_date",
    "_ingest_bronze_ts",
    "_ingest_silver_ts",
    "_ingest_gold_ts"

)

# COMMAND ----------

df_gold_out.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **quality check**

# COMMAND ----------

df_gold_out.select("status", "is_open").distinct().display()

# COMMAND ----------

df_gold_out.filter(col("time_to_solve_hours") < 0).count(), df_gold_out.filter(col("time_to_close_hours") < 0).count()

# COMMAND ----------


display(df_gold_out.filter(col("is_invalid_solve_duration")))

# COMMAND ----------

# MAGIC %md
# MAGIC # Write physical Gold Delta table

# COMMAND ----------

gold_path = f"{GOLD_ROOT}/tickets_enriched"

(df_gold_out.write
  .format("delta")
  .mode("overwrite")
  .option("overwriteSchema", "true")
  .save(gold_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC # KPI Tables

# COMMAND ----------

df_t_gold = spark.read.format("delta").load(f"{GOLD_ROOT}/tickets_enriched")

# COMMAND ----------

df_t_gold.count()

# COMMAND ----------

df_t_gold.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Standardize date helpers

# COMMAND ----------

df = (df_t_gold
      .withColumn("year", year(col("created_ts")))
      .withColumn("period", date_format(col("created_ts"), "yyyy-MM"))
)

df_all = df.withColumn("period", lit("ALL"))
dfp = df.unionByName(df_all)

build_ts = current_timestamp()

# COMMAND ----------

df.display()

# COMMAND ----------

df_all.display()

# COMMAND ----------

dfp.display()

# COMMAND ----------

is_resolved = col("is_open") == lit(0)
is_open = col("is_open") == lit(1)

# COMMAND ----------

df.select("status").distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Cards (top row)

# COMMAND ----------

kpi_cards = (
    dfp.groupBy("period")
    .agg(
        count(lit(1)).alias("total_tickets"),
        sum(when(is_open, 1).otherwise(0)).alias("is_open"),
        sum(when(is_resolved, 1).otherwise(0)).alias("is_resolved"),
        countDistinct(col("last_updater_user_id")).alias("technicians"),
    )
    .withColumn("open_rate",
                when(col("total_tickets") == 0, lit(0.0))
                .otherwise(col("is_open") / col("total_tickets")))
    .withColumn("gold_kpi_build_ts", build_ts)
    .orderBy("period")
)

# COMMAND ----------

kpi_cards.display()

# COMMAND ----------

cards_path = f"{GOLD_ROOT}/tickets_kpis/tickets_cards"
(kpi_cards.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(cards_path))

# COMMAND ----------

# MAGIC %md
# MAGIC # Distribution by Status

# COMMAND ----------

kpi_status = (
    dfp.groupBy("period", "status_code", "status")
    .agg(count(lit(1)).alias("ticket_count"))
    .withColumn("gold_kpi_build_ts", build_ts)
    .orderBy("period", col("ticket_count").desc())
)

# COMMAND ----------

kpi_status.display()

# COMMAND ----------

status_path = f"{GOLD_ROOT}/tickets_kpis/kpi_status_distribution_period"
(kpi_status.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(status_path))

# COMMAND ----------

# MAGIC %md
# MAGIC # Distribution by Priority

# COMMAND ----------

kpi_priority = (
    dfp.groupBy("period", "priority_code", "priority")
    .agg(count(lit(1)).alias("ticket_count"))
    .withColumn("gold_kpi_build_ts", build_ts)
    .orderBy("period", col("ticket_count").desc())
)

# COMMAND ----------

kpi_priority.display()

# COMMAND ----------

priority_path = f"{GOLD_ROOT}/tickets_kpis/kpi_priority_distribution_period"
(kpi_priority.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(priority_path))

# COMMAND ----------

# MAGIC %md
# MAGIC # Distribution by year

# COMMAND ----------

kpi_by_year = (
    df.groupBy("year")
    .agg(count(lit(1)).alias("ticket_count"))
    .withColumn("gold_kpi_build_ts", build_ts)
    .orderBy("year")
)

# COMMAND ----------

kpi_by_year.display()

# COMMAND ----------

year_path = f"{GOLD_ROOT}/tickets_kpis/kpi_tickets_by_year"
(kpi_by_year.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(year_path))

# COMMAND ----------

# MAGIC %md
# MAGIC # Distribution by month

# COMMAND ----------

kpi_by_month = (
    df.groupBy("period")
    .agg(count(lit(1)).alias("ticket_count"))
    .withColumn("gold_kpi_build_ts", build_ts)
    .orderBy("period")
)

# COMMAND ----------

kpi_by_month.display()

# COMMAND ----------

month_path = f"{GOLD_ROOT}/tickets_kpis/kpi_tickets_by_month"
(kpi_by_month.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(month_path))

# COMMAND ----------

# MAGIC %md
# MAGIC # Tickets by technician

# COMMAND ----------

kpi_by_technician = (
    dfp.groupBy("period", "last_updater_user_id", "last_updater_full_name")
    .agg(count(lit(1)).alias("ticket_count"))
    .withColumn("gold_kpi_build_ts", build_ts)
    .orderBy("period", col("ticket_count").desc())
)

# COMMAND ----------

kpi_by_technician.display()

# COMMAND ----------

tech_path = f"{GOLD_ROOT}/tickets_kpis/kpi_tickets_by_technician_period"
(kpi_by_technician.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(tech_path))

# COMMAND ----------

# MAGIC %md
# MAGIC # priority each technician

# COMMAND ----------

kpi_load_tech_prio = (
    dfp.groupBy("period", "last_updater_user_id", "last_updater_full_name", "priority_code", "priority")
    .agg(count(lit(1)).alias("ticket_count"))
    .withColumn("gold_kpi_build_ts", build_ts)
    .orderBy("period", col("last_updater_full_name"), col("priority_code").desc())
)

# COMMAND ----------

kpi_load_tech_prio.display()

# COMMAND ----------

tech_prio_path = f"{GOLD_ROOT}/tickets_kpis/kpi_load_by_technician_priority_period"
(kpi_load_tech_prio.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(tech_prio_path))

# COMMAND ----------

# MAGIC %md
# MAGIC # Distribution by category

# COMMAND ----------

kpi_cat_counts = (
    dfp.groupBy("period", "category_id", "category")
    .agg(count(lit(1)).alias("ticket_count"))
)

kpi_cat_totals = (
    dfp.groupBy("period")
    .agg(count(lit(1)).alias("total_tickets"))
)

kpi_by_category = (
    kpi_cat_counts.join(kpi_cat_totals, on="period", how="left")
                  .withColumn("share",
                              when(col("total_tickets") == 0, lit(0.0))
                              .otherwise(col("ticket_count") / col("total_tickets")))
                  .withColumn("gold_kpi_build_ts", build_ts)
                  .orderBy("period", col("ticket_count").desc())
)

# COMMAND ----------

kpi_by_category.display()

# COMMAND ----------

cat_path = f"{GOLD_ROOT}/tickets_kpis/kpi_tickets_by_category_period"
(kpi_by_category.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(cat_path))

# COMMAND ----------

# MAGIC %md
# MAGIC # Tickets by Requester (period)

# COMMAND ----------

kpi_requester = (
    dfp.groupBy("period", "requester_user_id", "requester_login", "requester_full_name")
       .agg(count(lit(1)).alias("ticket_count"))
       .withColumn("gold_kpi_build_ts", current_timestamp())
)

# COMMAND ----------

kpi_requester.display()

# COMMAND ----------

req_path = f"{GOLD_ROOT}/tickets_kpis/kpi_tickets_by_requester_period"
(kpi_requester.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(req_path))

# COMMAND ----------

# MAGIC %md
# MAGIC # Tickets by Location (period) + share

# COMMAND ----------

kpi_location_counts = (
    dfp.groupBy("period", "location_id", "location")
       .agg(count(lit(1)).alias("ticket_count"))
)

kpi_location_totals = (
    dfp.groupBy("period")
       .agg(count(lit(1)).alias("total_tickets"))
)

kpi_location = (
    kpi_location_counts.join(kpi_location_totals, on="period", how="left")
       .withColumn("share",
                   when(col("total_tickets") == 0, lit(0.0))
                   .otherwise(col("ticket_count") / col("total_tickets")))
       .withColumn("gold_kpi_build_ts", current_timestamp())
)

# COMMAND ----------

kpi_location.display()

# COMMAND ----------

loc_path = f"{GOLD_ROOT}/tickets_kpis/kpi_tickets_by_location_period"
(kpi_location.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(loc_path))