# Databricks notebook source
df = spark.read.table('databricks_cat.bronze.regions')

# COMMAND ----------

df = df.drop('_rescued_data')

# COMMAND ----------

df.write.format('delta').mode('overwrite').save('abfss://silver@chaitanyadatabricksete.dfs.core.windows.net/regions')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS databricks_cat.silver.regions_silver
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@chaitanyadatabricksete.dfs.core.windows.net/regions'

# COMMAND ----------

