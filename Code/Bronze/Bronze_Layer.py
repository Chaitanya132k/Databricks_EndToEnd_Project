# Databricks notebook source
# MAGIC %md
# MAGIC ### Parameters

# COMMAND ----------

dbutils.widgets.text('file_name','')
file_name = dbutils.widgets.get('file_name')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Reading

# COMMAND ----------

df = spark.readStream.format('cloudFiles')\
    .option('cloudFiles.format','parquet')\
    .option('cloudFiles.schemaLocation',f'abfss://bronze@chaitanyadatabricksete.dfs.core.windows.net/checkpoint_{file_name}')\
    .load(f'abfss://source@chaitanyadatabricksete.dfs.core.windows.net/{file_name}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Writing

# COMMAND ----------

df.writeStream.format('parquet')\
    .outputMode('append')\
    .option('checkpointLocation',f'abfss://bronze@chaitanyadatabricksete.dfs.core.windows.net/checkpoint_{file_name}')\
    .option('path',f'abfss://bronze@chaitanyadatabricksete.dfs.core.windows.net/{file_name}')\
    .trigger(once=True)\
    .start()

# COMMAND ----------

