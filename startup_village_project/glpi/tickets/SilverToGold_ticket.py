# Databricks notebook source
# MAGIC %md
# MAGIC # GLPI Tickets — Silver to Gold (current snapshot + KPIs)
# MAGIC
# MAGIC Enrichment:
# MAGIC - Join labels (status/priority/type/category/location)
# MAGIC - Join to users to add requester + technician (last updater) identity fields
# MAGIC
# MAGIC Outputs (Gold Delta):
# MAGIC - tickets_enriched
# MAGIC - KPI tables (tickets_kpis):
# MAGIC   - tickets_cards
# MAGIC   - kpi_status_distribution_period
# MAGIC   - kpi_priority_distribution_period
# MAGIC   - kpi_tickets_by_year
# MAGIC   - kpi_tickets_by_month
# MAGIC   - kpi_tickets_by_technician_period
# MAGIC   - kpi_load_by_technician_priority_period
# MAGIC   - kpi_tickets_by_category_period
# MAGIC   - kpi_tickets_by_requester_period
# MAGIC   - kpi_tickets_by_location_period

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Access And Configuration

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
GOLD_ROOT   = "abfss://gold@startupvillagedatalake.dfs.core.windows.net/tickets"

tickets_path   = f"{SILVER_ROOT}/tickets"
users_path     = f"{SILVER_ROOT}/users"

status_path    = f"{SILVER_ROOT}/ticket_status"
priority_path  = f"{SILVER_ROOT}/ticket_priority"
type_path      = f"{SILVER_ROOT}/ticket_type"
category_path  = f"{SILVER_ROOT}/ticket_category"
location_path  = f"{SILVER_ROOT}/ticket_location"

gold_path = f"{GOLD_ROOT}/tickets_enriched"

cards_path        = f"{GOLD_ROOT}/tickets_kpis/tickets_cards"
status_kpi_path   = f"{GOLD_ROOT}/tickets_kpis/kpi_status_distribution_period"
priority_kpi_path = f"{GOLD_ROOT}/tickets_kpis/kpi_priority_distribution_period"
year_kpi_path     = f"{GOLD_ROOT}/tickets_kpis/kpi_tickets_by_year"
month_kpi_path    = f"{GOLD_ROOT}/tickets_kpis/kpi_tickets_by_month"
tech_kpi_path     = f"{GOLD_ROOT}/tickets_kpis/kpi_tickets_by_technician_period"
tech_prio_kpi_path= f"{GOLD_ROOT}/tickets_kpis/kpi_load_by_technician_priority_period"
cat_kpi_path      = f"{GOLD_ROOT}/tickets_kpis/kpi_tickets_by_category_period"
req_kpi_path      = f"{GOLD_ROOT}/tickets_kpis/kpi_tickets_by_requester_period"
loc_kpi_path      = f"{GOLD_ROOT}/tickets_kpis/kpi_tickets_by_location_period"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Silver

# COMMAND ----------

df_t = spark.read.format("delta").load(tickets_path)
df_u = spark.read.format("delta").load(users_path)

df_status   = spark.read.format("delta").load(status_path)
df_priority = spark.read.format("delta").load(priority_path)
df_type     = spark.read.format("delta").load(type_path)
df_category = spark.read.format("delta").load(category_path)
df_location = spark.read.format("delta").load(location_path)

print("Tickets rows:", df_t.count())
print("Users rows:", df_u.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare role-playing user dims

# COMMAND ----------

df_users_small = df_u.select("user_id", "user_name", "full_name")

df_req = (
    df_users_small
    .withColumnRenamed("user_id", "requester_user_id")
    .withColumnRenamed("user_name", "requester_login")
    .withColumnRenamed("full_name", "requester_full_name")
)

df_upd = (
    df_users_small
    .withColumnRenamed("user_id", "last_updater_user_id")
    .withColumnRenamed("user_name", "last_updater_login")
    .withColumnRenamed("full_name", "last_updater_full_name")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enrich tickets with labels + user full names

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

# MAGIC %md
# MAGIC ## Add Gold metrics/flags (same outputs as before)

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
# MAGIC ## Select final schema (UNCHANGED to match your PostgreSQL DDL)

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

df_gold_out.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write physical Gold Delta table

# COMMAND ----------

df_gold_out = (
    df_gold_out
    .orderBy(col("created_ts").desc_nulls_last())
)

# COMMAND ----------

(df_gold_out.write
  .format("delta")
  .mode("overwrite")
  .option("overwriteSchema", "true")
  .save(gold_path)
)

print("Wrote:", gold_path, "rows:", df_gold_out.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## KPI Tables (NO raw-row duplication for ALL)

# COMMAND ----------

dfp = df_gold_out
build_ts = current_timestamp()

# define period from created_ts (no new columns persisted)
dfp_period = (
    dfp
    .withColumn("period", date_format(col("created_ts"), "yyyy-MM"))
)

is_resolved = col("is_open") == lit(0)
is_open = col("is_open") == lit(1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### KPI: Cards (period = ALL + yyyy-MM)

# COMMAND ----------

kpi_cards_all = (
    dfp.agg(
        count(lit(1)).alias("total_tickets"),
        sum(when(is_open, 1).otherwise(0)).alias("is_open"),
        sum(when(is_resolved, 1).otherwise(0)).alias("is_resolved"),
        countDistinct(col("last_updater_user_id")).alias("technicians"),
    )
    .withColumn("period", lit("ALL"))
)

kpi_cards_month = (
    dfp_period.groupBy("period")
    .agg(
        count(lit(1)).alias("total_tickets"),
        sum(when(is_open, 1).otherwise(0)).alias("is_open"),
        sum(when(is_resolved, 1).otherwise(0)).alias("is_resolved"),
        countDistinct(col("last_updater_user_id")).alias("technicians"),
    )
)

kpi_cards = (
    kpi_cards_all.unionByName(kpi_cards_month)
    .withColumn("open_rate",
                when(col("total_tickets") == 0, lit(0.0))
                .otherwise(col("is_open") / col("total_tickets")))
    .withColumn("gold_kpi_build_ts", build_ts)
    .orderBy("period")
)

kpi_cards.display()

(kpi_cards.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .save(cards_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ### KPI: Distribution by Status (period = ALL + yyyy-MM)

# COMMAND ----------

kpi_status_all = (
    dfp.groupBy("status_code", "status")
       .agg(count(lit(1)).alias("ticket_count"))
       .withColumn("period", lit("ALL"))
)

kpi_status_month = (
    dfp_period.where(col("period").isNotNull())
       .groupBy("period", "status_code", "status")
       .agg(count(lit(1)).alias("ticket_count"))
)

kpi_status = (
    kpi_status_all.unionByName(kpi_status_month)
    .withColumn("gold_kpi_build_ts", build_ts)
    .orderBy("period", col("ticket_count").desc())
)

kpi_status.display()

(kpi_status.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .save(status_kpi_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ### KPI: Distribution by Priority (period = ALL + yyyy-MM)

# COMMAND ----------

kpi_priority_all = (
    dfp.groupBy("priority_code", "priority")
       .agg(count(lit(1)).alias("ticket_count"))
       .withColumn("period", lit("ALL"))
)

kpi_priority_month = (
    dfp_period.where(col("period").isNotNull())
       .groupBy("period", "priority_code", "priority")
       .agg(count(lit(1)).alias("ticket_count"))
)

kpi_priority = (
    kpi_priority_all.unionByName(kpi_priority_month)
    .withColumn("gold_kpi_build_ts", build_ts)
    .orderBy("period", col("ticket_count").desc())
)

kpi_priority.display()

(kpi_priority.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .save(priority_kpi_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ### KPI: Distribution by year (unchanged schema)

# COMMAND ----------

kpi_by_year = (
    dfp.where(col("created_ts").isNotNull())
       .withColumn("year", year(col("created_ts")).cast("int"))
       .groupBy("year")
       .agg(count(lit(1)).alias("ticket_count"))
       .withColumn("gold_kpi_build_ts", build_ts)
       .orderBy("year")
)

kpi_by_year.display()

(kpi_by_year.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .save(year_kpi_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ### KPI: Distribution by month (table schema expects column named period)

# COMMAND ----------

kpi_by_month = (
    dfp_period.where(col("period").isNotNull())
       .groupBy("period")
       .agg(count(lit(1)).alias("ticket_count"))
       .withColumn("gold_kpi_build_ts", build_ts)
       .orderBy("period")
)

kpi_by_month.display()

(kpi_by_month.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .save(month_kpi_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ### KPI: Tickets by technician (period = ALL + yyyy-MM)

# COMMAND ----------

kpi_by_technician_all = (
    dfp.groupBy("last_updater_user_id", "last_updater_full_name")
       .agg(count(lit(1)).alias("ticket_count"))
       .withColumn("period", lit("ALL"))
)

kpi_by_technician_month = (
    dfp_period.where(col("period").isNotNull())
       .groupBy("period", "last_updater_user_id", "last_updater_full_name")
       .agg(count(lit(1)).alias("ticket_count"))
)

kpi_by_technician = (
    kpi_by_technician_all.unionByName(kpi_by_technician_month)
    .withColumn("gold_kpi_build_ts", build_ts)
    .orderBy("period", col("ticket_count").desc())
)

kpi_by_technician.display()

(kpi_by_technician.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .save(tech_kpi_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ### KPI: Load by technician + priority (period = ALL + yyyy-MM)

# COMMAND ----------

kpi_load_tech_prio_all = (
    dfp.groupBy("last_updater_user_id", "last_updater_full_name", "priority_code", "priority")
       .agg(count(lit(1)).alias("ticket_count"))
       .withColumn("period", lit("ALL"))
)

kpi_load_tech_prio_month = (
    dfp_period.where(col("period").isNotNull())
       .groupBy("period", "last_updater_user_id", "last_updater_full_name", "priority_code", "priority")
       .agg(count(lit(1)).alias("ticket_count"))
)

kpi_load_tech_prio = (
    kpi_load_tech_prio_all.unionByName(kpi_load_tech_prio_month)
    .withColumn("gold_kpi_build_ts", build_ts)
    .orderBy("period", col("last_updater_full_name"), col("priority_code").desc())
)

kpi_load_tech_prio.display()

(kpi_load_tech_prio.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .save(tech_prio_kpi_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ### KPI: Tickets by category + share (period = ALL + yyyy-MM)

# COMMAND ----------

kpi_cat_counts_all = (
    dfp.groupBy("category_id", "category")
       .agg(count(lit(1)).alias("ticket_count"))
       .withColumn("period", lit("ALL"))
)

kpi_cat_totals_all = (
    dfp.agg(count(lit(1)).alias("total_tickets"))
       .withColumn("period", lit("ALL"))
)

kpi_by_category_all = (
    kpi_cat_counts_all.join(kpi_cat_totals_all, on="period", how="left")
                  .withColumn("share",
                              when(col("total_tickets") == 0, lit(0.0))
                              .otherwise(col("ticket_count") / col("total_tickets")))
)

kpi_cat_counts_month = (
    dfp_period.where(col("period").isNotNull())
       .groupBy("period", "category_id", "category")
       .agg(count(lit(1)).alias("ticket_count"))
)

kpi_cat_totals_month = (
    dfp_period.where(col("period").isNotNull())
       .groupBy("period")
       .agg(count(lit(1)).alias("total_tickets"))
)

kpi_by_category_month = (
    kpi_cat_counts_month.join(kpi_cat_totals_month, on="period", how="left")
                  .withColumn("share",
                              when(col("total_tickets") == 0, lit(0.0))
                              .otherwise(col("ticket_count") / col("total_tickets")))
)

kpi_by_category = (
    kpi_by_category_all.unionByName(kpi_by_category_month)
    .withColumn("gold_kpi_build_ts", build_ts)
    .orderBy("period", col("ticket_count").desc())
)

kpi_by_category.display()

(kpi_by_category.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .save(cat_kpi_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ### KPI: Tickets by requester (period = ALL + yyyy-MM)

# COMMAND ----------

kpi_requester_all = (
    dfp.groupBy("requester_user_id", "requester_login", "requester_full_name")
       .agg(count(lit(1)).alias("ticket_count"))
       .withColumn("period", lit("ALL"))
)

kpi_requester_month = (
    dfp_period.where(col("period").isNotNull())
       .groupBy("period", "requester_user_id", "requester_login", "requester_full_name")
       .agg(count(lit(1)).alias("ticket_count"))
)

kpi_requester = (
    kpi_requester_all.unionByName(kpi_requester_month)
    .withColumn("gold_kpi_build_ts", build_ts)
    .orderBy("period", col("ticket_count").desc())
)

kpi_requester.display()

(kpi_requester.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .save(req_kpi_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ### KPI: Tickets by location + share (period = ALL + yyyy-MM)

# COMMAND ----------

kpi_location_counts_all = (
    dfp.groupBy("location_id", "location")
       .agg(count(lit(1)).alias("ticket_count"))
       .withColumn("period", lit("ALL"))
)

kpi_location_totals_all = (
    dfp.agg(count(lit(1)).alias("total_tickets"))
       .withColumn("period", lit("ALL"))
)

kpi_location_all = (
    kpi_location_counts_all.join(kpi_location_totals_all, on="period", how="left")
       .withColumn("share",
                   when(col("total_tickets") == 0, lit(0.0))
                   .otherwise(col("ticket_count") / col("total_tickets")))
)

kpi_location_counts_month = (
    dfp_period.where(col("period").isNotNull())
       .groupBy("period", "location_id", "location")
       .agg(count(lit(1)).alias("ticket_count"))
)

kpi_location_totals_month = (
    dfp_period.where(col("period").isNotNull())
       .groupBy("period")
       .agg(count(lit(1)).alias("total_tickets"))
)

kpi_location_month = (
    kpi_location_counts_month.join(kpi_location_totals_month, on="period", how="left")
       .withColumn("share",
                   when(col("total_tickets") == 0, lit(0.0))
                   .otherwise(col("ticket_count") / col("total_tickets")))
)

kpi_location = (
    kpi_location_all.unionByName(kpi_location_month)
    .withColumn("gold_kpi_build_ts", build_ts)
    .orderBy("period", col("ticket_count").desc())
)

kpi_location.display()

(kpi_location.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .save(loc_kpi_path))

# COMMAND ----------

print("Wrote tickets KPIs under:", f"{GOLD_ROOT}/tickets_kpis/")