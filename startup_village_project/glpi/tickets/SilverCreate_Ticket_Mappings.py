# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

spark.conf.set(f"fs.azure.account.auth.type.startupvillagedatalake.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.startupvillagedatalake.dfs.core.windows.net",
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.startupvillagedatalake.dfs.core.windows.net", 'ac803d5b-6251-4f6b-b1f9-2407fb651dd9')
spark.conf.set(f"fs.azure.account.oauth2.client.secret.startupvillagedatalake.dfs.core.windows.net", 'sIp8Q~4XBFI_epy7gnaf9~o_nruwEOt~1n1yzbRo')
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.startupvillagedatalake.dfs.core.windows.net",
               f"https://login.microsoftonline.com/05a451ff-b8ea-44f1-a99f-7704a79c7f37/oauth2/token")

# COMMAND ----------

SILVER_ROOT = "abfss://silver@startupvillagedatalake.dfs.core.windows.net/glpi"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ticket Status Dimension

# COMMAND ----------

status_rows = [
    Row(status_code=1, status="Nouveau"),
    Row(status_code=2, status="En cours (Attribué)"),
    Row(status_code=3, status="En cours (Planifié)"),
    Row(status_code=4, status="En attente"),
    Row(status_code=5, status="Résolu"),
    Row(status_code=6, status="Clos"),

]

df_dim_status = spark.createDataFrame(status_rows)

# COMMAND ----------

# DBTITLE 1,english version
# MAGIC %skip
# MAGIC status_rows = [
# MAGIC     Row(status_code=1, status_name="New"),
# MAGIC     Row(status_code=2, status_name="In progress (Assigned)"),
# MAGIC     Row(status_code=3, status_name="In progress (Planned)"),
# MAGIC     Row(status_code=4, status_name="Pending"),
# MAGIC     Row(status_code=5, status_name="Solved"),
# MAGIC     Row(status_code=6, status_name="Closed"),
# MAGIC ]
# MAGIC
# MAGIC df_dim_status = spark.createDataFrame(status_rows)

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

priority_rows = [
    Row(priority_code=6, priority="Majeure"),
    Row(priority_code=5, priority="Très haute"),
    Row(priority_code=4, priority="Haute"),
    Row(priority_code=3, priority="Moyenne"),
    Row(priority_code=2, priority="Basse"),
]

df_dim_priority = spark.createDataFrame(priority_rows)

# COMMAND ----------

# DBTITLE 1,english version
# MAGIC %skip
# MAGIC priority_rows = [
# MAGIC     Row(priority_code=6, priority_name="Major"),
# MAGIC     Row(priority_code=5, priority_name="Very high"),
# MAGIC     Row(priority_code=4, priority_name="High"),
# MAGIC     Row(priority_code=3, priority_name="Medium"),
# MAGIC     Row(priority_code=2, priority_name="Low"),
# MAGIC ]
# MAGIC
# MAGIC df_dim_priority = spark.createDataFrame(priority_rows)

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

type_rows = [
    Row(type_code=1, type="Incident"),
    Row(type_code=2, type="Demande"),
]

df_dim_type = spark.createDataFrame(type_rows)

# COMMAND ----------

# DBTITLE 1,english version
# MAGIC %skip
# MAGIC type_rows = [
# MAGIC     Row(type_code=1, type_name="Incident"),
# MAGIC     Row(type_code=2, type_name="Request"),
# MAGIC ]
# MAGIC
# MAGIC df_dim_type = spark.createDataFrame(type_rows)

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

category_rows = [
    Row(category_id=4, category="Achat"),
    Row(category_id=10, category="Check quotidien préventive"),
    Row(category_id=8, category="Electricité"),
    Row(category_id=5, category="Event"),
    Row(category_id=17, category="Event Externe"),
    Row(category_id=6, category="Finition et décoration"),
    Row(category_id=16, category="fluide"),
    Row(category_id=13, category="Hygiène"),
    Row(category_id=19, category="Intervention externe"),
    Row(category_id=9, category="Inventaire"),
    Row(category_id=1, category="IT and Support"),
    Row(category_id=2, category="Maintenance"),
    Row(category_id=3, category="Sécurité"),
    Row(category_id=7, category="Location Salle"),
    Row(category_id=11, category="vente clients"),
    Row(category_id=12, category="Préventif"),
    Row(category_id=14, category="Location Matériels"),
    Row(category_id=15, category="Plomberie"),
    Row(category_id=18, category="Analyse de donée"),
    Row(category_id=0, category=None),
]

df_dim_category = spark.createDataFrame(category_rows)

# COMMAND ----------

# DBTITLE 1,english version
# MAGIC %skip
# MAGIC category_rows = [
# MAGIC     Row(category_id=4,  category_name="Purchasing"),
# MAGIC     Row(category_id=10, category_name="Daily preventive check"),
# MAGIC     Row(category_id=8,  category_name="Electricity"),
# MAGIC     Row(category_id=5,  category_name="Event"),
# MAGIC     Row(category_id=17, category_name="External event"),
# MAGIC     Row(category_id=6,  category_name="Finishing & decoration"),
# MAGIC     Row(category_id=16, category_name="Fluids"),
# MAGIC     Row(category_id=13, category_name="Hygiene"),
# MAGIC     Row(category_id=19, category_name="External intervention"),
# MAGIC     Row(category_id=9,  category_name="Inventory"),
# MAGIC     Row(category_id=1,  category_name="IT and Support"),
# MAGIC     Row(category_id=2,  category_name="Maintenance"),
# MAGIC     Row(category_id=3,  category_name="Security"),
# MAGIC     Row(category_id=7,  category_name="Room rental"),
# MAGIC     Row(category_id=11, category_name="Client sales"),
# MAGIC     Row(category_id=12, category_name="Preventive"),
# MAGIC     Row(category_id=14, category_name="Equipment rental"),
# MAGIC     Row(category_id=15, category_name="Plumbing"),
# MAGIC     Row(category_id=18, category_name="Data analysis"),
# MAGIC     Row(category_id=0,  category_name=None),  # unknown / null
# MAGIC ]
# MAGIC
# MAGIC df_dim_category = spark.createDataFrame(category_rows)

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