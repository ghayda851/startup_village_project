# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

spark.conf.set(f"fs.azure.account.auth.type.startupvillagedatalake.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.startupvillagedatalake.dfs.core.windows.net",
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.startupvillagedatalake.dfs.core.windows.net", 'ac803d5b-6251-4f6b-b1f9-2407fb651dd9')
spark.conf.set(f"fs.azure.account.oauth2.client.secret.startupvillagedatalake.dfs.core.windows.net", 'sIp8Q~4XBFI_epy7gnaf9~o_nruwEOt~1n1yzbRo')
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.startupvillagedatalake.dfs.core.windows.net",
               f"https://login.microsoftonline.com/05a451ff-b8ea-44f1-a99f-7704a79c7f37/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC ## WIDGETS (job parameters)

# COMMAND ----------

dbutils.widgets.text("pg_host", "")
dbutils.widgets.text("pg_port", "5432")
dbutils.widgets.text("pg_db", "startupvillage_serving")
dbutils.widgets.text("pg_user", "startupvillage")
dbutils.widgets.text("pg_password", "")

pg_host = dbutils.widgets.get("pg_host").strip()
pg_port = dbutils.widgets.get("pg_port").strip()
pg_db = dbutils.widgets.get("pg_db").strip()
pg_user = dbutils.widgets.get("pg_user").strip()
pg_password = dbutils.widgets.get("pg_password")

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
# MAGIC # Gold path

# COMMAND ----------

gold_root = "abfss://gold@startupvillagedatalake.dfs.core.windows.net/tickets"

gold_tickets_enriched_path = f"{gold_root}/tickets_enriched"
gold_kpi_load_by_technician_priority_period_path = f"{gold_root}/tickets_kpis/kpi_load_by_technician_priority_period"
gold_kpi_priority_distribution_period_path = f"{gold_root}/tickets_kpis/kpi_priority_distribution_period"
gold_kpi_tickets_by_category_period_path = f"{gold_root}/tickets_kpis/kpi_tickets_by_category_period"
gold_kpi_tickets_by_month_path = f"{gold_root}/tickets_kpis/kpi_tickets_by_month"
gold_kpi_tickets_by_requester_period_path = f"{gold_root}/tickets_kpis/kpi_tickets_by_requester_period"
gold_kpi_tickets_by_technician_period_path = f"{gold_root}/tickets_kpis/kpi_tickets_by_technician_period"
gold_kpi_tickets_by_year_path = f"{gold_root}/tickets_kpis/kpi_tickets_by_year"
gold_tickets_cards_path = f"{gold_root}/tickets_kpis/tickets_cards"

gold_kpi_tickets_by_location_period_path = f"{gold_root}/tickets_kpis/kpi_tickets_by_location_period" 

# COMMAND ----------

# MAGIC %md
# MAGIC # Read Gold

# COMMAND ----------

df_tickets = spark.read.format("delta").load(gold_tickets_enriched_path)
df_kpi_load_by_technician_priority_period = spark.read.format("delta").load(gold_kpi_load_by_technician_priority_period_path)
df_kpi_priority_distribution_period = spark.read.format("delta").load(gold_kpi_priority_distribution_period_path)
df_kpi_tickets_by_category_period = spark.read.format("delta").load(gold_kpi_tickets_by_category_period_path)
df_kpi_tickets_by_month = spark.read.format("delta").load(gold_kpi_tickets_by_month_path)
df_kpi_tickets_by_requester_period = spark.read.format("delta").load(gold_kpi_tickets_by_requester_period_path)
df_kpi_tickets_by_technician_period = spark.read.format("delta").load(gold_kpi_tickets_by_technician_period_path)
df_kpi_tickets_by_year = spark.read.format("delta").load(gold_kpi_tickets_by_year_path)
df_kpi_tickets_by_location_period = spark.read.format("delta").load(gold_kpi_tickets_by_location_period_path)
df_tickets_cards = spark.read.format("delta").load(gold_tickets_cards_path)

# COMMAND ----------

print(df_tickets.columns)
print(df_kpi_load_by_technician_priority_period.columns)
print(df_kpi_priority_distribution_period.columns)
print(df_kpi_tickets_by_category_period.columns)
print(df_kpi_tickets_by_month.columns)
print(df_kpi_tickets_by_requester_period.columns)
print(df_kpi_tickets_by_technician_period.columns)
print(df_kpi_tickets_by_year.columns)
print(df_kpi_tickets_by_location_period.columns)
print(df_tickets_cards.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC # Keep all columns

# COMMAND ----------

df_tickets_out = df_tickets.select(*df_tickets.columns)

df_load_out = df_kpi_load_by_technician_priority_period.select(*df_kpi_load_by_technician_priority_period.columns)
df_prio_dist_out = df_kpi_priority_distribution_period.select(*df_kpi_priority_distribution_period.columns)
df_by_cat_out = df_kpi_tickets_by_category_period.select(*df_kpi_tickets_by_category_period.columns)
df_by_month_out = df_kpi_tickets_by_month.select(*df_kpi_tickets_by_month.columns)
df_by_req_out = df_kpi_tickets_by_requester_period.select(*df_kpi_tickets_by_requester_period.columns)
df_by_tech_out = df_kpi_tickets_by_technician_period.select(*df_kpi_tickets_by_technician_period.columns)
df_by_year_out = df_kpi_tickets_by_year.select(*df_kpi_tickets_by_year.columns)
df_by_loc_out = df_kpi_tickets_by_location_period.select(*df_kpi_tickets_by_location_period.columns)
df_cards_out = df_tickets_cards.select(*df_tickets_cards.columns)

print("Schemas (Delta -> will be same in Postgres):")
print("tickets_enriched:", df_tickets_out.columns)
print("kpi_load_by_technician_priority_period:", df_load_out.columns)
print("kpi_priority_distribution_period:", df_prio_dist_out.columns)
print("kpi_tickets_by_category_period:", df_by_cat_out.columns)
print("kpi_tickets_by_month:", df_by_month_out.columns)
print("kpi_tickets_by_requester_period:", df_by_req_out.columns)
print("kpi_tickets_by_technician_period:", df_by_tech_out.columns)
print("kpi_tickets_by_year:", df_by_year_out.columns)
print("kpi_tickets_by_location_period:", df_by_loc_out.columns)
print("tickets_cards:", df_cards_out.columns)


# COMMAND ----------

schema = "srv"
tables = [
    "tickets_enriched",
    "kpi_load_by_technician_priority_period",
    "kpi_priority_distribution_period",
    "kpi_tickets_by_category_period",
    "kpi_tickets_by_month",
    "kpi_tickets_by_requester_period",
    "kpi_tickets_by_technician_period",
    "kpi_tickets_by_year",
    "kpi_tickets_by_location_period",
    "tickets_cards",
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
# MAGIC # write (append) into postgres

# COMMAND ----------

df_tickets.printSchema()

# COMMAND ----------

df_tickets_out = (
    df_tickets
      .withColumn("is_open", (col("is_open") == lit(1)).cast("boolean"))
      .withColumn("is_deleted", (col("is_deleted") == lit(1)).cast("boolean"))
      .select(*df_tickets.columns)  # keep same column order/names
)

# COMMAND ----------

(df_tickets_out.write.mode("append").jdbc(jdbc_url, f"{schema}.tickets_enriched", properties=jdbc_props))

(df_load_out.write.mode("append").jdbc(jdbc_url, f"{schema}.kpi_load_by_technician_priority_period", properties=jdbc_props))
(df_prio_dist_out.write.mode("append").jdbc(jdbc_url, f"{schema}.kpi_priority_distribution_period", properties=jdbc_props))
(df_by_cat_out.write.mode("append").jdbc(jdbc_url, f"{schema}.kpi_tickets_by_category_period", properties=jdbc_props))
(df_by_month_out.write.mode("append").jdbc(jdbc_url, f"{schema}.kpi_tickets_by_month", properties=jdbc_props))
(df_by_req_out.write.mode("append").jdbc(jdbc_url, f"{schema}.kpi_tickets_by_requester_period", properties=jdbc_props))
(df_by_tech_out.write.mode("append").jdbc(jdbc_url, f"{schema}.kpi_tickets_by_technician_period", properties=jdbc_props))
(df_by_year_out.write.mode("append").jdbc(jdbc_url, f"{schema}.kpi_tickets_by_year", properties=jdbc_props))
(df_by_loc_out.write.mode("append").jdbc(jdbc_url, f"{schema}.kpi_tickets_by_location_period", properties=jdbc_props))
(df_cards_out.write.mode("append").jdbc(jdbc_url, f"{schema}.tickets_cards", properties=jdbc_props))

print("✅ Published ALL Tickets Gold tables to PostgreSQL (1:1 columns) using TRUNCATE + APPEND.")