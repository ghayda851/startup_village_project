# Databricks notebook source
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
# MAGIC ### Base path

# COMMAND ----------

base_gold_path = "abfss://gold@startupvillagedatalake.dfs.core.windows.net"


# COMMAND ----------

# MAGIC %md
# MAGIC ### -----------------------
# MAGIC ### SPACE (gold/space/*)
# MAGIC ### -----------------------

# COMMAND ----------

gold_space_path = f"{base_gold_path}/space"

space_room_current_path = f"{gold_space_path}/space_room_current"
kpi_global_current_path = f"{gold_space_path}/kpi_global_current"
kpi_by_site_current_path = f"{gold_space_path}/kpi_by_site_current"
kpi_by_site_space_type_current_path = f"{gold_space_path}/kpi_by_site_space_type_current"
kpi_by_site_org_type_current_path = f"{gold_space_path}/kpi_by_site_org_type_current"

df_space_room_current = spark.read.format("delta").load(space_room_current_path)
df_kpi_global_current = spark.read.format("delta").load(kpi_global_current_path)
df_kpi_by_site_current = spark.read.format("delta").load(kpi_by_site_current_path)
df_kpi_by_site_space_type_current = spark.read.format("delta").load(kpi_by_site_space_type_current_path)
df_kpi_by_site_org_type_current = spark.read.format("delta").load(kpi_by_site_org_type_current_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ----------------------------------------------------------------
# MAGIC ### RESERVATIONS KPIs (gold/reservations/reservations_kpis/*)
# MAGIC ### ----------------------------------------------------------------

# COMMAND ----------

gold_reservations_kpis_path = f"{base_gold_path}/reservations/reservations_kpis"

by_item_current_path = f"{gold_reservations_kpis_path}/by_item_current"
by_user_current_path = f"{gold_reservations_kpis_path}/by_user_current"
cards_current_path = f"{gold_reservations_kpis_path}/cards_current"
duration_distribution_current_path = f"{gold_reservations_kpis_path}/duration_distribution_current"
invalid_current_path = f"{gold_reservations_kpis_path}/invalid_current"
peak_period_current_path = f"{gold_reservations_kpis_path}/peak_period_current"
trends_by_month_path = f"{gold_reservations_kpis_path}/trends_by_month"

df_res_by_item_current = spark.read.format("delta").load(by_item_current_path)
df_res_by_user_current = spark.read.format("delta").load(by_user_current_path)
df_res_cards_current = spark.read.format("delta").load(cards_current_path)
df_res_duration_distribution_current = spark.read.format("delta").load(duration_distribution_current_path)
df_res_invalid_current = spark.read.format("delta").load(invalid_current_path)
df_res_peak_period_current = spark.read.format("delta").load(peak_period_current_path)
df_res_trends_by_month = spark.read.format("delta").load(trends_by_month_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### -----------------------------------------------------
# MAGIC ### TICKETS ENRICHED (gold/tickets/tickets_enriched)
# MAGIC ### -----------------------------------------------------

# COMMAND ----------

tickets_enriched_path = f"{base_gold_path}/tickets/tickets_enriched"
df_tickets_enriched = spark.read.format("delta").load(tickets_enriched_path)

# COMMAND ----------

gold_tickets_kpis_path = f"{base_gold_path}/tickets/tickets_kpis"

df_kpi_load_by_technician_priority_period = spark.read.format("delta").load(
    f"{gold_tickets_kpis_path}/kpi_load_by_technician_priority_period"
)
df_kpi_priority_distribution_period = spark.read.format("delta").load(
    f"{gold_tickets_kpis_path}/kpi_priority_distribution_period"
)
df_kpi_status_distribution_period = spark.read.format("delta").load(
    f"{gold_tickets_kpis_path}/kpi_status_distribution_period"
)
df_kpi_tickets_by_category_period = spark.read.format("delta").load(
    f"{gold_tickets_kpis_path}/kpi_tickets_by_category_period"
)
df_kpi_tickets_by_location_period = spark.read.format("delta").load(
    f"{gold_tickets_kpis_path}/kpi_tickets_by_location_period"
)
df_kpi_tickets_by_month = spark.read.format("delta").load(
    f"{gold_tickets_kpis_path}/kpi_tickets_by_month"
)
df_kpi_tickets_by_requester_period = spark.read.format("delta").load(
    f"{gold_tickets_kpis_path}/kpi_tickets_by_requester_period"
)
df_kpi_tickets_by_technician_period = spark.read.format("delta").load(
    f"{gold_tickets_kpis_path}/kpi_tickets_by_technician_period"
)
df_kpi_tickets_by_year = spark.read.format("delta").load(
    f"{gold_tickets_kpis_path}/kpi_tickets_by_year"
)
df_tickets_cards = spark.read.format("delta").load(
    f"{gold_tickets_kpis_path}/tickets_cards"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### -------------------------------------------------------------------------------------
# MAGIC ### RESERVATIONS ENRICHED CURRENT (gold/reservations/reservations_enriched_current)
# MAGIC ### -------------------------------------------------------------------------------------

# COMMAND ----------

reservations_enriched_current_path = f"{base_gold_path}/reservations/reservations_enriched_current"
df_reservations_enriched_current = spark.read.format("delta").load(reservations_enriched_current_path)

# COMMAND ----------

df_reservations_enriched_current.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # RESERVATIONS KPIs

# COMMAND ----------

display(df_res_by_item_current)
display(df_res_by_user_current)
display(df_res_cards_current)
display(df_res_duration_distribution_current)
display(df_res_invalid_current)
display(df_res_peak_period_current)
display(df_res_trends_by_month)

# COMMAND ----------

# MAGIC %md
# MAGIC # TICKETS KPIs

# COMMAND ----------

display(df_tickets_enriched)

# COMMAND ----------


display(df_kpi_load_by_technician_priority_period)
display(df_kpi_priority_distribution_period)
display(df_kpi_status_distribution_period)
display(df_kpi_tickets_by_category_period)
display(df_kpi_tickets_by_location_period)
display(df_kpi_tickets_by_month)
display(df_kpi_tickets_by_requester_period)
display(df_kpi_tickets_by_technician_period)
display(df_kpi_tickets_by_year)
display(df_tickets_cards)