# Databricks notebook source
# MAGIC %md
# MAGIC # Data Access And Configuration

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

spark.conf.set(f"fs.azure.account.auth.type.startupvillagedatalake.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.startupvillagedatalake.dfs.core.windows.net",
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.startupvillagedatalake.dfs.core.windows.net", 'ac803d5b-6251-4f6b-b1f9-2407fb651dd9')
spark.conf.set(f"fs.azure.account.oauth2.client.secret.startupvillagedatalake.dfs.core.windows.net", 'sIp8Q~4XBFI_epy7gnaf9~o_nruwEOt~1n1yzbRo')
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.startupvillagedatalake.dfs.core.windows.net",
               f"https://login.microsoftonline.com/05a451ff-b8ea-44f1-a99f-7704a79c7f37/oauth2/token")

# COMMAND ----------

dbutils.fs.ls("abfss://silver@startupvillagedatalake.dfs.core.windows.net/glpi/")

# COMMAND ----------

silver_path = "abfss://silver@startupvillagedatalake.dfs.core.windows.net/glpi/tickets"

# COMMAND ----------

# MAGIC %md
# MAGIC # Read Silver Tickets

# COMMAND ----------

df_ticket_s = spark.read.format("delta").load(f'{silver_path}')

# COMMAND ----------

df_ticket_s.limit(10).display()

# COMMAND ----------

df_by_category = (
    df_ticket_s
    .groupBy(col("location_id"))
    .agg(count("*").alias("count"))
    .orderBy(col("count").desc())
)
display(df_by_category)

# COMMAND ----------

df_by_type_code = (
    df_ticket_s
    .groupBy(col("type_code"))
    .agg(count("*").alias("count"))
    .orderBy(col("count").desc())
)
display(df_by_type_code)

# COMMAND ----------

df_p1 = (
        df_ticket_s
        .filter(col("type_code") == 1)
        .select("ticket_id","request_type_id","location_id","type_code" ,"category_id", "priority_code", "status_code") 
    )
df_p1.limit(10).display()

# COMMAND ----------

df_by_request_type = (
    df_ticket_s
    .groupBy(col("request_type_id"))
    .agg(count("*").alias("count"))
    .orderBy(col("count").desc())
)
display(df_by_request_type)

# COMMAND ----------

for i in range(24):
    df_p3 = (
        df_ticket_s
        .filter(col("location_id") == i)
        .select("ticket_id","request_type_id","location_id","type_code" ,"category_id", "priority_code", "status_code") 
    )
    df_p3.limit(1).display()

# COMMAND ----------

df_p3 = (
    df_ticket_s
    .filter(col("location_id") == 25)
    .select("ticket_id","request_type_id","location_id","type_code" ,"category_id", "priority_code", "status_code") 
)
df_p3.limit(1).display()

# COMMAND ----------

df_p3 = (
    df_ticket_s
    .filter(col("location_id") == 27)
    .select("ticket_id","request_type_id","location_id","type_code" ,"category_id", "priority_code", "status_code") 
)
df_p3.limit(1).display()


# COMMAND ----------

df_p3.limit(5).display()