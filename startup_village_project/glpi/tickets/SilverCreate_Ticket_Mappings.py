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

SILVER_ROOT = "abfss://silver@startupvillagedatalake.dfs.core.windows.net/glpi"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ticket Status Dimension

# COMMAND ----------

# MAGIC %skip
# MAGIC status_rows = [
# MAGIC     Row(status_code=1, status="Nouveau"),
# MAGIC     Row(status_code=2, status="En cours (Attribué)"),
# MAGIC     Row(status_code=3, status="En cours (Planifié)"),
# MAGIC     Row(status_code=4, status="En attente"),
# MAGIC     Row(status_code=5, status="Résolu"),
# MAGIC     Row(status_code=6, status="Clos"),
# MAGIC
# MAGIC ]
# MAGIC
# MAGIC df_dim_status = spark.createDataFrame(status_rows)

# COMMAND ----------

# DBTITLE 1,english version
status_rows = [
    Row(status_code=1, status="New"),
    Row(status_code=2, status="In progress (Assigned)"),
    Row(status_code=3, status="In progress (Planned)"),
    Row(status_code=4, status="Pending"),
    Row(status_code=5, status="Solved"),
    Row(status_code=6, status="Closed"),
]

df_dim_status = spark.createDataFrame(status_rows)

# COMMAND ----------

status_path = f"{SILVER_ROOT}/ticket_status"
(df_dim_status.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(status_path)
)

# COMMAND ----------

print("Wrote:", status_path, "rows:", df_dim_status.count())
display(df_dim_status.orderBy("status_code"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ticket Priority Dimension

# COMMAND ----------

# MAGIC %skip
# MAGIC priority_rows = [
# MAGIC     Row(priority_code=6, priority="Majeure"),
# MAGIC     Row(priority_code=5, priority="Très haute"),
# MAGIC     Row(priority_code=4, priority="Haute"),
# MAGIC     Row(priority_code=3, priority="Moyenne"),
# MAGIC     Row(priority_code=2, priority="Basse"),
# MAGIC ]
# MAGIC
# MAGIC df_dim_priority = spark.createDataFrame(priority_rows)

# COMMAND ----------

# DBTITLE 1,english version
priority_rows = [
    Row(priority_code=6, priority="Major"),
    Row(priority_code=5, priority="Very high"),
    Row(priority_code=4, priority="High"),
    Row(priority_code=3, priority="Medium"),
    Row(priority_code=2, priority="Low"),
]

df_dim_priority = spark.createDataFrame(priority_rows)

# COMMAND ----------

priority_path = f"{SILVER_ROOT}/ticket_priority"
(df_dim_priority.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(priority_path)
)

# COMMAND ----------

print("Wrote:", priority_path, "rows:", df_dim_priority.count())
display(df_dim_priority.orderBy(col("priority_code").desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ticket Type Dimension

# COMMAND ----------

# MAGIC %skip
# MAGIC type_rows = [
# MAGIC     Row(type_code=1, type="Incident"),
# MAGIC     Row(type_code=2, type="Demande"),
# MAGIC ]
# MAGIC
# MAGIC df_dim_type = spark.createDataFrame(type_rows)

# COMMAND ----------

# DBTITLE 1,english version
type_rows = [
    Row(type_code=1, type="Incident"),
    Row(type_code=2, type="Request"),
]

df_dim_type = spark.createDataFrame(type_rows)

# COMMAND ----------

type_path = f"{SILVER_ROOT}/ticket_type"
(df_dim_type.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(type_path)
)

# COMMAND ----------

print("Wrote:", type_path, "rows:", df_dim_type.count())
display(df_dim_type.orderBy("type_code"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ticket Category Dimension

# COMMAND ----------

# MAGIC %skip
# MAGIC category_rows = [
# MAGIC     Row(category_id=4, category="Achat"),
# MAGIC     Row(category_id=10, category="Check quotidien préventive"),
# MAGIC     Row(category_id=8, category="Electricité"),
# MAGIC     Row(category_id=5, category="Event"),
# MAGIC     Row(category_id=17, category="Event Externe"),
# MAGIC     Row(category_id=6, category="Finition et décoration"),
# MAGIC     Row(category_id=16, category="fluide"),
# MAGIC     Row(category_id=13, category="Hygiène"),
# MAGIC     Row(category_id=19, category="Intervention externe"),
# MAGIC     Row(category_id=9, category="Inventaire"),
# MAGIC     Row(category_id=1, category="IT and Support"),
# MAGIC     Row(category_id=2, category="Maintenance"),
# MAGIC     Row(category_id=3, category="Sécurité"),
# MAGIC     Row(category_id=7, category="Location Salle"),
# MAGIC     Row(category_id=11, category="vente clients"),
# MAGIC     Row(category_id=12, category="Préventif"),
# MAGIC     Row(category_id=14, category="Location Matériels"),
# MAGIC     Row(category_id=15, category="Plomberie"),
# MAGIC     Row(category_id=18, category="Analyse de donée"),
# MAGIC     Row(category_id=0, category=None),
# MAGIC ]
# MAGIC
# MAGIC df_dim_category = spark.createDataFrame(category_rows)

# COMMAND ----------

# DBTITLE 1,english version
category_rows = [
    Row(category_id=4,  category="Purchasing"),
    Row(category_id=10, category="Daily preventive check"),
    Row(category_id=8,  category="Electricity"),
    Row(category_id=5,  category="Event"),
    Row(category_id=17, category="External event"),
    Row(category_id=6,  category="Finishing & decoration"),
    Row(category_id=16, category="Fluids"),
    Row(category_id=13, category="Hygiene"),
    Row(category_id=19, category="External intervention"),
    Row(category_id=9,  category="Inventory"),
    Row(category_id=1,  category="IT and Support"),
    Row(category_id=2,  category="Maintenance"),
    Row(category_id=3,  category="Security"),
    Row(category_id=7,  category="Room rental"),
    Row(category_id=11, category="Client sales"),
    Row(category_id=12, category="Preventive"),
    Row(category_id=14, category="Equipment rental"),
    Row(category_id=15, category="Plumbing"),
    Row(category_id=18, category="Data analysis"),
    Row(category_id=0,  category=None),  # unknown / null
]

df_dim_category = spark.createDataFrame(category_rows)

# COMMAND ----------

category_path = f"{SILVER_ROOT}/ticket_category"
(df_dim_category.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(category_path)
)

# COMMAND ----------

print("Wrote:", category_path, "rows:", df_dim_category.count())
display(df_dim_category.orderBy("category_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ticket Location Dimension

# COMMAND ----------

location_rows = [
    Row(location_id=0, location=None),
    Row(location_id=1, location="Medianet"),
    Row(location_id=2, location="Startup Village"),
    Row(location_id=3, location="Externe de Startup Village"),
    Row(location_id=4, location="Saphir Consult"),
    Row(location_id=6, location="9ème - Coworking"),
    Row(location_id=7, location="Cilié"),
    Row(location_id=8, location="Medianet Ennaser"),
    Row(location_id=9, location="Express FM"),
    Row(location_id=11, location="Cochef"),
    Row(location_id=12, location="1er étage"),
    Row(location_id=13, location="Salle Avant Première"),
    Row(location_id=14, location="Salle COSY"),
    Row(location_id=15, location="Salle de Stock"),
    Row(location_id=16, location="9éme étage"),
    Row(location_id=17, location="Grand Terrasse"),
    Row(location_id=18, location="Hors Startup Village"),
    Row(location_id=19, location="Salle 13"),
    Row(location_id=20, location="Salle Confidentielle"),
    Row(location_id=21, location="Salle 11"),
    Row(location_id=22, location="Salle 12"),
    Row(location_id=23, location="Dépôt-Principale"),
    Row(location_id=25, location="Dépôt-Technique"),
    Row(location_id=26, location="LT"),
    Row(location_id=27, location="Studio"),
]

df_dim_location = spark.createDataFrame(location_rows)


# COMMAND ----------

# DBTITLE 1,english version
# MAGIC %skip
# MAGIC location_rows = [
# MAGIC     Row(location_id=0,  location_name=None),  # unknown / null
# MAGIC     Row(location_id=1,  location_name="Medianet"),
# MAGIC     Row(location_id=2,  location_name="Startup Village"),
# MAGIC     Row(location_id=3,  location_name="Outside Startup Village"),
# MAGIC     Row(location_id=4,  location_name="Saphir Consult"),
# MAGIC     Row(location_id=6,  location_name="9th floor - Coworking"),
# MAGIC     Row(location_id=7,  location_name="Cilié"),
# MAGIC     Row(location_id=8,  location_name="Medianet Ennaser"),
# MAGIC     Row(location_id=9,  location_name="Express FM"),
# MAGIC     Row(location_id=11, location_name="Cochef"),
# MAGIC     Row(location_id=12, location_name="1st floor"),
# MAGIC     Row(location_id=13, location_name="Avant Première room"),
# MAGIC     Row(location_id=14, location_name="COSY room"),
# MAGIC     Row(location_id=15, location_name="Storage room"),
# MAGIC     Row(location_id=16, location_name="9th floor"),
# MAGIC     Row(location_id=17, location_name="Main terrace"),
# MAGIC     Row(location_id=18, location_name="Outside Startup Village"),
# MAGIC     Row(location_id=19, location_name="Room 13"),
# MAGIC     Row(location_id=20, location_name="Confidential room"),
# MAGIC     Row(location_id=21, location_name="Room 11"),
# MAGIC     Row(location_id=22, location_name="Room 12"),
# MAGIC     Row(location_id=23, location_name="Main warehouse"),
# MAGIC ]
# MAGIC
# MAGIC df_dim_location = spark.createDataFrame(location_rows)
# MAGIC

# COMMAND ----------

location_path = f"{SILVER_ROOT}/ticket_location"
(df_dim_location.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(location_path)
)

# COMMAND ----------

print("Wrote:", location_path, "rows:", df_dim_location.count())
display(df_dim_location.orderBy("location_id"))