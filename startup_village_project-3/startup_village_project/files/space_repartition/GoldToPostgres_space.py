# Databricks notebook source
# MAGIC %md
# MAGIC # Data Access And Configuration

# COMMAND ----------

from pyspark.sql import functions as F

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
# MAGIC ## GOLD PATH

# COMMAND ----------

gold_root = "abfss://gold@startupvillagedatalake.dfs.core.windows.net/space"

gold_space_room_current_path = f"{gold_root}/space_room_current"
gold_kpi_global_path = f"{gold_root}/kpi_global_current"
gold_kpi_by_site_path = f"{gold_root}/kpi_by_site_current"
gold_kpi_by_site_space_type_path = f"{gold_root}/kpi_by_site_space_type_current"
gold_kpi_by_site_org_type_path = f"{gold_root}/kpi_by_site_org_type_current"

# COMMAND ----------

# MAGIC %md
# MAGIC ## READ GOLD

# COMMAND ----------

df_room = spark.read.format("delta").load(gold_space_room_current_path)
df_global = spark.read.format("delta").load(gold_kpi_global_path)
df_site = spark.read.format("delta").load(gold_kpi_by_site_path)
df_site_space_type = spark.read.format("delta").load(gold_kpi_by_site_space_type_path)
df_site_org_type = spark.read.format("delta").load(gold_kpi_by_site_org_type_path)

# COMMAND ----------

print(df_room.columns)
print(df_global.columns)
print(df_site.columns)
print(df_site_space_type.columns)
print(df_site_org_type.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## SELECT ONLY COLUMNS THAT EXIST IN POSTGRES

# COMMAND ----------

room_cols = [
    "room_nk",
    "site", "floor", "room",
    "occupancy_status",
    "organization_name", "organization_type",
    "space_type", "activity",
    "employee_count", "total_capacity", "area_m2",
    "occupancy_rate_pct", "density_emp_per_100m2",
    "is_over_capacity",
    "_ingestion_date",
    "last_updated_ts",
]
df_room_out = df_room.select(*room_cols)

global_cols = [
    "villages_count",
    "total_spaces",
    "total_area_m2",
    "total_capacity",
    "total_employees",
    "leased_spaces",
    "non_leased_spaces",
    "reservation_spaces",
    "per_person_spaces",
    "underutilized_spaces_lt_40pct",
    "overoccupied_spaces_gt_100pct",
    "avg_density_emp_per_100m2",
    "weighted_occupancy_rate_pct",
    "_as_of_ts",
]
df_global_out = df_global.select(*global_cols)

site_cols = [
    "site",
    "total_spaces",
    "total_area_m2",
    "total_capacity",
    "total_employees",
    "leased_spaces",
    "non_leased_spaces",
    "underutilized_spaces_lt_40pct",
    "overoccupied_spaces_gt_100pct",
    "avg_density_emp_per_100m2",
    "weighted_occupancy_rate_pct",
    "_as_of_ts",
]
df_site_out = df_site.select(*site_cols)

site_space_type_cols = [
    "site",
    "space_type",
    "spaces_count",
    "sum_area_m2",
    "sum_capacity",
    "sum_employees",
    "_as_of_ts",
]
df_site_space_type_out = df_site_space_type.select(*site_space_type_cols)

site_org_type_cols = [
    "site",
    "organization_type",
    "spaces_count",
    "sum_employees",
    "sum_area_m2",
    "sum_capacity",
    "_as_of_ts",
]
df_site_org_type_out = df_site_org_type.select(*site_org_type_cols)

# COMMAND ----------

print("Rows to publish:")
print("room:", df_room_out.count())
print("global:", df_global_out.count())
print("site:", df_site_out.count())
print("site_space_type:", df_site_space_type_out.count())
print("site_org_type:", df_site_org_type_out.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## TRUNCATE TABLES (keep indexes)

# COMMAND ----------

schema = "srv"
tables = [
    "space_room_current",
    "space_kpi_global_current",
    "space_kpi_by_site_current",
    "space_kpi_by_site_space_type_current",
    "space_kpi_by_site_org_type_current",
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
# MAGIC ## WRITE (APPEND) INTO POSTGRES

# COMMAND ----------

(df_room_out.write
 .mode("append")
 .jdbc(jdbc_url, f"{schema}.space_room_current", properties=jdbc_props)
)

(df_global_out.write
 .mode("append")
 .jdbc(jdbc_url, f"{schema}.space_kpi_global_current", properties=jdbc_props)
)

(df_site_out.write
 .mode("append")
 .jdbc(jdbc_url, f"{schema}.space_kpi_by_site_current", properties=jdbc_props)
)

(df_site_space_type_out.write
 .mode("append")
 .jdbc(jdbc_url, f"{schema}.space_kpi_by_site_space_type_current", properties=jdbc_props)
)

(df_site_org_type_out.write
 .mode("append")
 .jdbc(jdbc_url, f"{schema}.space_kpi_by_site_org_type_current", properties=jdbc_props)
)

print("✅ Published Gold Space tables to PostgreSQL using TRUNCATE + APPEND.")