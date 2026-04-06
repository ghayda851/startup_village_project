# Databricks notebook source
# MAGIC %md
# MAGIC # Data Access And Configuration

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

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

dbutils.fs.ls("abfss://bronze@startupvillagedatalake.dfs.core.windows.net/glpi/")

# COMMAND ----------

bronze_path = "abfss://bronze@startupvillagedatalake.dfs.core.windows.net/glpi/"
silver_path = "abfss://silver@startupvillagedatalake.dfs.core.windows.net/glpi/tickets"

# COMMAND ----------

# MAGIC %md
# MAGIC # Read Bronze Tickets

# COMMAND ----------
df_ticket_b = spark.read.format("delta").load(f'{bronze_path}/tickets')

latest_ingestion_date = df_ticket_b.select(max("ingestion_date")).first()[0]

df_ticket_b = spark.read.format("delta").load(
    f"{bronze_path}/tickets/ingestion_date={latest_ingestion_date}"
)

# COMMAND ----------

df_ticket_b.display()

# COMMAND ----------

print(df_ticket_b.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Transformation

# COMMAND ----------

def clean_str(cname: str):
    c = col(cname)
    return when(c.isNull() | (trim(c) == ""), lit(None)).otherwise(trim(c))

def to_long(cname: str):
    return col(cname).cast("long")

def to_int(cname: str):
    # handles null/""/"null" text sometimes
    c = when(col(cname).isNull(), lit(None)).otherwise(trim(col(cname)))
    c = when(lower(c) == "null", lit(None)).otherwise(c)
    c = regexp_replace(c, ",", ".")
    return c.cast("int")

def to_double(cname: str):
    c = when(col(cname).isNull(), lit(None)).otherwise(trim(col(cname)))
    c = when(lower(c) == "null", lit(None)).otherwise(c)
    c = regexp_replace(c, ",", ".")
    return c.cast("double")

def seconds_to_hours(cname: str, out_col: str):
    # returns a Column expr: seconds / 3600
    return (col(cname).cast("double") / lit(3600.0)).alias(out_col)

df_ticket_s = (
    df_ticket_b
    # ---------- ids ----------
    .withColumn("ticket_id", to_long("id"))
    .withColumn("entity_id", to_long("entities_id"))
    .withColumn("category_id", to_long("itilcategories_id"))
    .withColumn("location_id", to_long("locations_id"))
    .withColumn("request_type_id", to_long("requesttypes_id"))
    .withColumn("requester_user_id", to_long("users_id_recipient"))              # in GLPI this is corresponds to demandeur
    .withColumn("last_updater_user_id", to_long("users_id_lastupdater"))

    # ---------- dates/timestamps ----------
    .withColumn("ticket_date_ts", to_timestamp(col("date")))
    .withColumn("created_ts", to_timestamp(col("date_creation")))
    .withColumn("updated_ts", to_timestamp(col("date_mod")))
    .withColumn("solved_ts", to_timestamp(col("solvedate")))
    .withColumn("closed_ts", to_timestamp(col("closedate")))
    .withColumn("waiting_begin_ts", to_timestamp(col("begin_waiting_date")))
    .withColumn("ola_ttr_begin_ts", to_timestamp(col("ola_ttr_begin_date")))

    # ---------- text ----------
    .withColumn("title", clean_str("name"))
    .withColumn("description_html", clean_str("content"))

    # ---------- flags / enums ----------
    .withColumn("status_code", to_int("status"))
    .withColumn("type_code", to_int("type"))
    .withColumn("priority_code", to_int("priority"))
    .withColumn("impact_code", to_int("impact"))
    .withColumn("urgency_code", to_int("urgency"))
    .withColumn("is_deleted", to_int("is_deleted"))

    .withColumn("global_validation_code", to_int("global_validation"))
    .withColumn("validation_percent", to_double("validation_percent"))

    # ---------- raw time fields (typically seconds) ----------
    .withColumn("action_time_sec", to_int("actiontime"))
    .withColumn("waiting_duration_sec", to_int("waiting_duration"))
    .withColumn("ola_waiting_duration_sec", to_int("ola_waiting_duration"))
    .withColumn("sla_waiting_duration_sec", to_int("sla_waiting_duration"))

    .withColumn("time_to_own_sec", to_int("time_to_own"))
    .withColumn("time_to_resolve_sec", to_int("time_to_resolve"))
    .withColumn("internal_time_to_own_sec", to_int("internal_time_to_own"))
    .withColumn("internal_time_to_resolve_sec", to_int("internal_time_to_resolve"))

    .withColumn("take_into_account_delay_sec", to_int("takeintoaccount_delay_stat"))
    .withColumn("solve_delay_sec", to_int("solve_delay_stat"))
    .withColumn("close_delay_sec", to_int("close_delay_stat"))

    # ---------- SLA/OLA ids ----------
    .withColumn("slas_id_tto", to_long("slas_id_tto"))
    .withColumn("slas_id_ttr", to_long("slas_id_ttr"))
    .withColumn("slalevels_id_ttr", to_long("slalevels_id_ttr"))
    .withColumn("olas_id_tto", to_long("olas_id_tto"))
    .withColumn("olas_id_ttr", to_long("olas_id_ttr"))
    .withColumn("olalevels_id_ttr", to_long("olalevels_id_ttr"))

    # ---------- metadata ----------
    .withColumn("ingestion_date", col("ingestion_date").cast("date"))
    .withColumn("_ingest_silver_ts", current_timestamp())
)

# COMMAND ----------

df_ticket_s = df_ticket_s.select(
    col("ticket_id"),
    col("entity_id"),
    col("category_id"),
    col("request_type_id"),  #we dont need it it is always 1
    col("location_id"),
    col("requester_user_id"),
    col("last_updater_user_id"),
    col("title"),
    col("created_ts"),
    col("updated_ts"),
    col("solved_ts"),
    col("closed_ts"),
    col("status_code"),
    col("priority_code"),
    col("type_code"),
    col("is_deleted"),
    col("ingestion_date"),
    col("_source_file"),
    col("_source_system"),
    col("_ingest_bronze_ts"),
    col("_ingest_silver_ts"),
)

# COMMAND ----------

display(df_ticket_s.limit(20))

# COMMAND ----------

print(df_ticket_s.count())

# COMMAND ----------

df_ticket_s.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Writing To Silver

# COMMAND ----------

# MAGIC %md
# MAGIC **we still need to deside what to keep in silver**

# COMMAND ----------

w = Window.partitionBy("ticket_id").orderBy(
    col("updated_ts").desc_nulls_last(),
    col("_ingest_bronze_ts").desc_nulls_last(),
    col("ingestion_date").desc_nulls_last(),
)

# COMMAND ----------

df_ticket_s_latest = (
    df_ticket_s
    .withColumn("_rn", row_number().over(w))
    .filter(col("_rn") == 1)
    .drop("_rn")
)

# COMMAND ----------

(df_ticket_s_latest.write
  .format("delta")
  .mode("overwrite")
  .option("overwriteSchema", "true")
  .save(silver_path)
)

# COMMAND ----------

df_ticket_new = spark.read.format("delta").load(f'{silver_path}')

# COMMAND ----------

print(df_ticket_new.count())

# COMMAND ----------

print(df_ticket_new.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC # Tickets Analysis

# COMMAND ----------

df_created_daily = (
    df_ticket_s
    .where(col("created_ts").isNotNull())
    .groupBy(to_date(col("created_ts")).alias("day"))
    .agg(count("*").alias("tickets_created"))
    .orderBy(col("day"))
)

display(df_created_daily)

# COMMAND ----------

df_by_status = (
    df_ticket_s
    .groupBy(col("status_code"))
    .agg(count("*").alias("tickets"))
    .orderBy(col("tickets").desc())
)
display(df_by_status)

# COMMAND ----------

df_by_priority = (
    df_ticket_s
    .groupBy(col("priority_code"))
    .agg(count("*").alias("tickets"))
    .orderBy(col("tickets").desc())
)
display(df_by_priority)

# COMMAND ----------

df_by_category = (
    df_ticket_s
    .groupBy(col("category_id"))
    .agg(count("*").alias("tickets"))
    .orderBy(col("tickets").desc())
)
display(df_by_category)
