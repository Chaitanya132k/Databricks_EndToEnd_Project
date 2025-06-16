# Databricks notebook source
# MAGIC %md
# MAGIC ### Data Reading

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

df = spark.read.format('parquet').load('abfss://bronze@chaitanyadatabricksete.dfs.core.windows.net/orders')

# COMMAND ----------

df = df.withColumnRenamed('_rescued_data','rescued_data').drop('rescued_data')

# COMMAND ----------

df = df.withColumn('order_date',to_timestamp(col('order_date')))

# COMMAND ----------

df = df.withColumn('Year',year(col('order_date')))

# COMMAND ----------

df1 = df.withColumn('flag',dense_rank().over(Window.partitionBy('Year').orderBy(desc('total_amount'))))

# COMMAND ----------

df1 = df1.withColumn('rank_flag',rank().over(Window.partitionBy('Year').orderBy(desc('total_amount'))))

# COMMAND ----------

df1 = df1.withColumn('row_flag',row_number().over(Window.partitionBy('Year').orderBy(desc('total_amount'))))

# COMMAND ----------

class windows:
    def dense_rank(self,df):
        df_dense_rank = df.withColumn('flag',dense_rank().over(Window.partitionBy('Year').orderBy(desc('total_amount'))))
        return df_dense_rank
    def rank(self,df):
        df_rank = df.withColumn('rank_flag',rank().over(Window.partitionBy('Year').orderBy(desc('total_amount'))))
        return df_rank
    def row_number(self,df):
        df_row_number = df.withColumn('row_flag',row_number().over(Window.partitionBy('Year').orderBy(desc('total_amount'))))
        return df_row_number

# COMMAND ----------

df_new = df

# COMMAND ----------

obj = windows()
df_result = obj.dense_rank(df_new)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data writing

# COMMAND ----------

df.write.format('delta').mode('overwrite').save('abfss://silver@chaitanyadatabricksete.dfs.core.windows.net/orders')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS databricks_cat.silver.orders_silver
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@chaitanyadatabricksete.dfs.core.windows.net/orders'

# COMMAND ----------

