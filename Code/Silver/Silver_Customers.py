# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import time

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Reading

# COMMAND ----------

df = spark.read.format('parquet').load('abfss://bronze@chaitanyadatabricksete.dfs.core.windows.net/customers')

# COMMAND ----------

df = df.drop('_rescued_data')

# COMMAND ----------

df = df.withColumn('Domains',split(col('email'),'@').getItem(1))

# COMMAND ----------

df_gmail = df.filter(col('Domains')=='gmail.com')
df_gmail.display()
time.sleep(5)
df_yahoo = df.filter(col('Domains')=='yahoo.com')
df_yahoo.display()
time.sleep(5)
df_hotmail = df.filter(col('Domains')=='hotmail.com')
df_hotmail.display()
time.sleep(5)

# COMMAND ----------

df = df.withColumn('full_name',concat(col('first_name'),lit(' '),col('last_name'))).drop('first_name','last_name')

# COMMAND ----------

df.write.mode('overwrite').format('delta').save('abfss://silver@chaitanyadatabricksete.dfs.core.windows.net/customers')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS databricks_cat.silver.customers_silver
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@chaitanyadatabricksete.dfs.core.windows.net/customers'

# COMMAND ----------

