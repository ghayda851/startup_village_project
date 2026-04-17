# Databricks notebook source
from pyspark.sql.functions import *

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
# MAGIC ## WIDGETS (job parameters)

# COMMAND ----------

dbutils.widgets.text("pg_host", "")
dbutils.widgets.text("pg_port", "5432")
dbutils.widgets.text("pg_db", "startupvillage_serving")
dbutils.widgets.text("pg_user", "startupvillage")
dbutils.widgets.text("pg_password", "")

dbutils.widgets.text("repartition_reservations", "8")
dbutils.widgets.text("repartition_kpis", "2")

pg_host = dbutils.widgets.get("pg_host").strip()
pg_port = dbutils.widgets.get("pg_port").strip()
pg_db = dbutils.widgets.get("pg_db").strip()
pg_user = dbutils.widgets.get("pg_user").strip()
pg_password = dbutils.widgets.get("pg_password")

repartition_reservations = int(dbutils.widgets.get("repartition_reservations").strip())
repartition_kpis = int(dbutils.widgets.get("repartition_kpis").strip())

if not pg_host or not pg_db or not pg_user or not pg_password:
    raise Exception("Missing widget values. Please set pg_host, pg_db, pg_user, pg_password (and optionally pg_port).")

jdbc_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}?sslmode=require"
jdbc_props = {
    "user": pg_user,
    "password": pg_password,
    "driver": "org.postgresql.Driver",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold paths (reservations)

# COMMAND ----------

gold_root = "abfss://gold@startupvillagedatalake.dfs.core.windows.net/reservations"

gold_reservations_enriched_current_path = f"{gold_root}/reservations_enriched_current"

gold_kpi_cards_current_path = f"{gold_root}/reservations_kpis/cards_current"
gold_kpi_peak_period_current_path = f"{gold_root}/reservations_kpis/peak_period_current"
gold_kpi_trends_by_month_path = f"{gold_root}/reservations_kpis/trends_by_month"
gold_kpi_by_item_current_path = f"{gold_root}/reservations_kpis/by_item_current"
gold_kpi_by_user_current_path = f"{gold_root}/reservations_kpis/by_user_current"
gold_kpi_duration_distribution_current_path = f"{gold_root}/reservations_kpis/duration_distribution_current"
gold_kpi_invalid_current_path = f"{gold_root}/reservations_kpis/invalid_current"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Gold

# COMMAND ----------

df_res = spark.read.format("delta").load(gold_reservations_enriched_current_path)

df_cards = spark.read.format("delta").load(gold_kpi_cards_current_path)
df_peak = spark.read.format("delta").load(gold_kpi_peak_period_current_path)
df_trends = spark.read.format("delta").load(gold_kpi_trends_by_month_path)
df_by_item = spark.read.format("delta").load(gold_kpi_by_item_current_path)
df_by_user = spark.read.format("delta").load(gold_kpi_by_user_current_path)
df_duration = spark.read.format("delta").load(gold_kpi_duration_distribution_current_path)
df_invalid = spark.read.format("delta").load(gold_kpi_invalid_current_path)

print(df_res.columns)
print(df_cards.columns)
print(df_peak.columns)
print(df_trends.columns)
print(df_by_item.columns)
print(df_by_user.columns)
print(df_duration.columns)
print(df_invalid.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Keep all columns (1:1 schema)

# COMMAND ----------

df_res_out = df_res.select(*df_res.columns)

df_cards_out = df_cards.select(*df_cards.columns)
df_peak_out = df_peak.select(*df_peak.columns)
df_trends_out = df_trends.select(*df_trends.columns)
df_by_item_out = df_by_item.select(*df_by_item.columns)
df_by_user_out = df_by_user.select(*df_by_user.columns)
df_duration_out = df_duration.select(*df_duration.columns)
df_invalid_out = df_invalid.select(*df_invalid.columns)

print("Schemas (Delta -> will be same in Postgres):")
print("reservations_enriched_current:", df_res_out.columns)
print("cards_current:", df_cards_out.columns)
print("peak_period_current:", df_peak_out.columns)
print("trends_by_month:", df_trends_out.columns)
print("by_item_current:", df_by_item_out.columns)
print("by_user_current:", df_by_user_out.columns)
print("duration_distribution_current:", df_duration_out.columns)
print("invalid_current:", df_invalid_out.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## TRUNCATE target tables

# COMMAND ----------

schema = "srv"
tables = [
    "reservations_enriched_current",
    "reservations_kpi_cards_current",
    "reservations_kpi_peak_period_current",
    "reservations_kpi_trends_by_month",
    "reservations_kpi_by_item_current",
    "reservations_kpi_by_user_current",
    "reservations_kpi_duration_distribution_current",
    "reservations_kpi_invalid_current",
]

conn = None
try:
    jvm = spark._sc._gateway.jvm
    DriverManager = jvm.java.sql.DriverManager
    conn = DriverManager.getConnection(jdbc_url, pg_user, pg_password)
    stmt = conn.createStatement()

    stmt.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
    for t in tables:
        stmt.execute(f"TRUNCATE TABLE {schema}.{t};")

    stmt.close()
finally:
    if conn is not None:
        conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write (append) into Postgres (with repartition)

# COMMAND ----------

df_res_out = df_res_out.repartition(repartition_reservations)
df_cards_out = df_cards_out.repartition(repartition_kpis)
df_peak_out = df_peak_out.repartition(repartition_kpis)
df_trends_out = df_trends_out.repartition(repartition_kpis)
df_by_item_out = df_by_item_out.repartition(repartition_kpis)
df_by_user_out = df_by_user_out.repartition(repartition_kpis)
df_duration_out = df_duration_out.repartition(repartition_kpis)
df_invalid_out = df_invalid_out.repartition(repartition_kpis)

(df_res_out.write.mode("append").jdbc(jdbc_url, f"{schema}.reservations_enriched_current", properties=jdbc_props))

(df_cards_out.write.mode("append").jdbc(jdbc_url, f"{schema}.reservations_kpi_cards_current", properties=jdbc_props))
(df_peak_out.write.mode("append").jdbc(jdbc_url, f"{schema}.reservations_kpi_peak_period_current", properties=jdbc_props))
(df_trends_out.write.mode("append").jdbc(jdbc_url, f"{schema}.reservations_kpi_trends_by_month", properties=jdbc_props))
(df_by_item_out.write.mode("append").jdbc(jdbc_url, f"{schema}.reservations_kpi_by_item_current", properties=jdbc_props))
(df_by_user_out.write.mode("append").jdbc(jdbc_url, f"{schema}.reservations_kpi_by_user_current", properties=jdbc_props))
(df_duration_out.write.mode("append").jdbc(jdbc_url, f"{schema}.reservations_kpi_duration_distribution_current", properties=jdbc_props))
(df_invalid_out.write.mode("append").jdbc(jdbc_url, f"{schema}.reservations_kpi_invalid_current", properties=jdbc_props))

print("Published ALL Reservations Gold tables to PostgreSQL (1:1 columns) using TRUNCATE + APPEND.")