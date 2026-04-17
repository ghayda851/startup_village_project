# Databricks notebook source
# MAGIC %md
# MAGIC # Space repartition — Silver to Gold (current snapshot + KPIs)
# MAGIC
# MAGIC What this notebook does:
# MAGIC - Reads Silver space repartition snapshot
# MAGIC - Normalizes keys (site/floor/room) and builds a natural key `room_nk`
# MAGIC - Keeps the latest record per room (safety dedup)
# MAGIC - Builds a Gold "room_current" mart (overwrite)
# MAGIC - Builds KPI marts (global + by site + by site/space_type + by site/org_type) (overwrite)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Access And Configuration

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

# Use a stable timezone for any timestamp logic
spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paths

# COMMAND ----------

silver_path = "abfss://silver@startupvillagedatalake.dfs.core.windows.net/space/space_repartition"

gold_root = "abfss://gold@startupvillagedatalake.dfs.core.windows.net/space"

gold_space_room_current_path = f"{gold_root}/space_room_current"
gold_kpi_global_path = f"{gold_root}/kpi_global_current"
gold_kpi_by_site_path = f"{gold_root}/kpi_by_site_current"
gold_kpi_by_site_space_type_path = f"{gold_root}/kpi_by_site_space_type_current"
gold_kpi_by_site_org_type_path = f"{gold_root}/kpi_by_site_org_type_current"

print("Silver:", silver_path)
print("Gold root:", gold_root)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Silver

# COMMAND ----------

df_s = spark.read.format("delta").load(silver_path)
print("Silver rows:", df_s.count())
print("Silver columns:", df_s.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Normalize + keys

# COMMAND ----------

def norm_col(cname: str):
    return lower(trim(col(cname)))

df = (
    df_s
    .withColumn("site_norm", norm_col("site"))
    .withColumn("floor_norm", norm_col("floor"))
    .withColumn("room_norm", norm_col("room"))
    # natural key for upserts + joins
    .withColumn("room_nk", concat_ws("|", col("site_norm"), col("floor_norm"), col("room_norm")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pick latest record per room (safety)

# COMMAND ----------

w = Window.partitionBy("room_nk").orderBy(
    col("_ingestion_date").desc_nulls_last(),
    col("_silver_ts").desc_nulls_last()
)

# COMMAND ----------

df_latest = (
    df
    .withColumn("_rn", row_number().over(w))
    .filter(col("_rn") == 1)
    .drop("_rn")
)

# COMMAND ----------

print("Distinct rooms:", df_latest.select("room_nk").distinct().count())
print("Rows after room dedup:", df_latest.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Derived metrics + Gold room_current mart

# COMMAND ----------

df_gold_room_current = (
    df_latest
    .withColumn("employee_count", col("employee_count").cast("int"))
    .withColumn("total_capacity", col("total_capacity").cast("int"))
    .withColumn("area_m2", col("area_m2").cast("double"))

    # keep nulls for unknown area/capacity to avoid wrong averages
    .withColumn(
        "occupancy_rate_pct",
        when(col("total_capacity").isNull() | (col("total_capacity") == 0), lit(None).cast("double"))
        .otherwise((coalesce(col("employee_count"), lit(0)) / col("total_capacity")) * lit(100.0))
    )
    .withColumn(
        "density_emp_per_100m2",
        when(col("area_m2").isNull() | (col("area_m2") <= 0), lit(None).cast("double"))
        .otherwise((coalesce(col("employee_count"), lit(0)) / col("area_m2")) * lit(100.0))
    )
    .withColumn(
        "is_over_capacity",
        when(col("employee_count").isNull() | col("total_capacity").isNull(), lit(None).cast("boolean"))
        .otherwise(col("employee_count") > col("total_capacity"))
    )
    .withColumn("last_updated_ts", current_timestamp())
    .select(
        "room_nk",
        "site", "floor", "room",
        "occupancy_status",
        "organization_name", "organization_type",
        "space_type", "activity",
        "employee_count", "total_capacity", "area_m2",
        "occupancy_rate_pct", "density_emp_per_100m2",
        "is_over_capacity",
        "_ingestion_date",
        "_source_file", "_source_system", "_entity",
        "_ingest_ts", "_silver_ts",
        "last_updated_ts",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Gold: room current

# COMMAND ----------

(df_gold_room_current.write
 .format("delta")
 .mode("overwrite")   # small dataset; simplest and cheapest
 .option("overwriteSchema", "true")
 .save(gold_space_room_current_path)
)

# COMMAND ----------

print("Wrote:", gold_space_room_current_path, "rows:", df_gold_room_current.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## KPI marts

# COMMAND ----------

df_kpi_global = (
    df_gold_room_current
    .agg(
        F.countDistinct("site").alias("villages_count"),
        F.countDistinct("room_nk").alias("total_spaces"),
        F.sum(F.col("area_m2")).alias("total_area_m2"),
        F.sum(F.col("total_capacity")).alias("total_capacity"),
        F.sum(F.col("employee_count")).alias("total_employees"),
        F.sum(F.when(norm("occupancy_status") == "loué", 1).otherwise(0)).alias("leased_spaces"),
        F.sum(F.when(norm("occupancy_status") == "non loué", 1).otherwise(0)).alias("non_leased_spaces"),
        F.sum(F.when(norm("occupancy_status").like("%réser%"), 1).otherwise(0)).alias("reservation_spaces"),
        F.sum(F.when(norm("occupancy_status").like("%personne%"), 1).otherwise(0)).alias("per_person_spaces"),
        F.sum(F.when(F.col("occupancy_rate_pct") < 40, 1).otherwise(0)).alias("underutilized_spaces_lt_40pct"),
        F.sum(F.when(F.col("occupancy_rate_pct") > 100, 1).otherwise(0)).alias("overoccupied_spaces_gt_100pct"),
        F.avg(F.col("density_emp_per_100m2")).alias("avg_density_emp_per_100m2"),
    )
    .withColumn(
        "weighted_occupancy_rate_pct",
        F.when(F.col("total_capacity").isNull() | (F.col("total_capacity") == 0), F.lit(None).cast("double"))
         .otherwise((F.col("total_employees") / F.col("total_capacity")) * F.lit(100.0))
    )
    .withColumn("_as_of_ts", F.current_timestamp())
)

(df_kpi_global.write
 .format("delta")
 .mode("overwrite")
 .save(gold_kpi_global_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper for consistent comparisons

# COMMAND ----------

def norm_expr(cname: str):
    return lower(trim(col(cname)))

build_ts = current_timestamp()

# COMMAND ----------

# MAGIC %md
# MAGIC ## -------------------
# MAGIC ## KPI: Global current
# MAGIC ## -------------------

# COMMAND ----------

df_kpi_global = (
    df_gold_room_current
    .agg(
        countDistinct("site").alias("villages_count"),
        countDistinct("room_nk").alias("total_spaces"),
        sum(col("area_m2")).alias("total_area_m2"),
        sum(col("total_capacity")).alias("total_capacity"),
        sum(col("employee_count")).alias("total_employees"),

        sum(when(norm_expr("occupancy_status") == "loué", 1).otherwise(0)).alias("leased_spaces"),
        sum(when(norm_expr("occupancy_status") == "non loué", 1).otherwise(0)).alias("non_leased_spaces"),
        sum(when(norm_expr("occupancy_status").like("%réser%"), 1).otherwise(0)).alias("reservation_spaces"),
        sum(when(norm_expr("occupancy_status").like("%personne%"), 1).otherwise(0)).alias("per_person_spaces"),

        sum(when(col("occupancy_rate_pct") < 40, 1).otherwise(0)).alias("underutilized_spaces_lt_40pct"),
        sum(when(col("occupancy_rate_pct") > 100, 1).otherwise(0)).alias("overoccupied_spaces_gt_100pct"),
        avg(col("density_emp_per_100m2")).alias("avg_density_emp_per_100m2"),
    )
    .withColumn(
        "weighted_occupancy_rate_pct",
        when(col("total_capacity").isNull() | (col("total_capacity") == 0), lit(None).cast("double"))
        .otherwise((col("total_employees") / col("total_capacity")) * lit(100.0))
    )
    .withColumn("_as_of_ts", build_ts)
)

# COMMAND ----------

(df_kpi_global.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .save(gold_kpi_global_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## --------------
# MAGIC ## KPI: By site
# MAGIC ## --------------

# COMMAND ----------

df_kpi_by_site = (
    df_gold_room_current
    .groupBy("site")
    .agg(
        countDistinct("room_nk").alias("total_spaces"),
        sum("area_m2").alias("total_area_m2"),
        sum("total_capacity").alias("total_capacity"),
        sum("employee_count").alias("total_employees"),

        sum(when(norm_expr("occupancy_status") == "loué", 1).otherwise(0)).alias("leased_spaces"),
        sum(when(norm_expr("occupancy_status") == "non loué", 1).otherwise(0)).alias("non_leased_spaces"),

        sum(when(col("occupancy_rate_pct") < 40, 1).otherwise(0)).alias("underutilized_spaces_lt_40pct"),
        sum(when(col("occupancy_rate_pct") > 100, 1).otherwise(0)).alias("overoccupied_spaces_gt_100pct"),
        avg(col("density_emp_per_100m2")).alias("avg_density_emp_per_100m2"),
    )
    .withColumn(
        "weighted_occupancy_rate_pct",
        when(col("total_capacity").isNull() | (col("total_capacity") == 0), lit(None).cast("double"))
        .otherwise((col("total_employees") / col("total_capacity")) * lit(100.0))
    )
    .withColumn("_as_of_ts", build_ts)
)

# COMMAND ----------

(df_kpi_by_site.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .save(gold_kpi_by_site_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ---------------------------
# MAGIC ## KPI: By site + space type
# MAGIC ## ---------------------------

# COMMAND ----------


df_kpi_by_site_space_type = (
    df_gold_room_current
    .groupBy("site", "space_type")
    .agg(
        countDistinct("room_nk").alias("spaces_count"),
        sum("area_m2").alias("sum_area_m2"),
        sum("total_capacity").alias("sum_capacity"),
        sum("employee_count").alias("sum_employees"),
    )
    .withColumn("_as_of_ts", build_ts)
)

# COMMAND ----------

(df_kpi_by_site_space_type.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .save(gold_kpi_by_site_space_type_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## -------------------------
# MAGIC ## KPI: By site + org type
# MAGIC ## -------------------------

# COMMAND ----------

df_kpi_by_site_org_type = (
    df_gold_room_current
    .groupBy("site", "organization_type")
    .agg(
        countDistinct("room_nk").alias("spaces_count"),
        sum("employee_count").alias("sum_employees"),
        sum("area_m2").alias("sum_area_m2"),
        sum("total_capacity").alias("sum_capacity"),
    )
    .withColumn("_as_of_ts", build_ts)
)

# COMMAND ----------

(df_kpi_by_site_org_type.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .save(gold_kpi_by_site_org_type_path)
)