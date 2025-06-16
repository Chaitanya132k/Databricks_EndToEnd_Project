# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable

# COMMAND ----------

init_load_flag = int(dbutils.widgets.get("init_load_flag"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Reading

# COMMAND ----------

df = spark.sql('SELECT * FROM databricks_cat.silver.customers_silver')

# COMMAND ----------

#Removing duplicates
df = df.dropDuplicates(subset=['customer_id'])

# COMMAND ----------

#Dividing new vs old records
if init_load_flag == 0:
    df_old = spark.sql('SELECT Dim_Cust_Key, customer_id, create_date, update_date FROM databricks_cat.gold.Dim_Customers')
else:
    df_old = spark.sql('SELECT 0 Dim_Cust_Key, 0 customer_id, 0 create_date, 0 update_date FROM databricks_cat.silver.customers_silver where 1=0')

# COMMAND ----------

#Renaming columns of df_old
for column in df_old.columns:
    df_old = df_old.withColumnRenamed(column, column+'_old')

# COMMAND ----------

#applying join with old records
df_join = df.join(df_old,df.customer_id == df_old.customer_id_old,'left')

# COMMAND ----------

#Separating old vs new records
df_new = df_join.filter(col('Dim_Cust_Key_old').isNull())
df_old = df_join.filter(col('Dim_Cust_Key_old').isNotNull())

# COMMAND ----------

#preparing df_old
df_old = df_old.drop('customer_id_old','update_date_old')

#Renaming old_create_date and Dim_Cust_Key
df_old = df_old.withColumnRenamed('create_date_old','create_date')
df_old = df_old.withColumn('create_date',to_timestamp(col('create_date')))
df_old = df_old.withColumnRenamed('Dim_Cust_Key_old','Dim_Cust_Key')

#Adding update_date_column
df_old = df_old.withColumn('update_date',current_timestamp())

# COMMAND ----------

#preparing df_new
df_new = df_new.drop('Dim_Cust_Key_old','customer_id_old','update_date_old','create_date_old')

#Adding update_date_column
df_new = df_new.withColumn('update_date',current_timestamp())
df_new = df_new.withColumn('create_date',current_timestamp())

# COMMAND ----------

#Apply surrogate key from 1
df_new = df_new.withColumn('Dim_Cust_Key',monotonically_increasing_id()+lit(1))

# COMMAND ----------

#Adding Max surrogate key
if init_load_flag == 1:
    max_surrogate_key = 0
else:
    df_max_sur = spark.sql('SELECT max(Dim_Cust_Key) as max_surrogate_key FROM databricks_cat.gold.Dim_Customers')
    #converting df_max_sur to variable
    max_surrogate_key = df_max_sur.collect()[0]['max_surrogate_key']

# COMMAND ----------

df_new = df_new.withColumn('Dim_Cust_Key',col('Dim_Cust_Key')+lit(max_surrogate_key))

# COMMAND ----------

df_final = df_old.unionByName(df_new)

# COMMAND ----------

# MAGIC %md
# MAGIC ### SCD Type-1

# COMMAND ----------

if spark.catalog.tableExists('databricks_cat.gold.Dim_Customers'):
    dlt_obj = DeltaTable.forPath(spark,'abfss://gold@chaitanyadatabricksete.dfs.core.windows.net/Dim_Customers')
    dlt_obj.alias('trg').merge(df_final.alias('src'),'trg.Dim_Cust_Key = src.Dim_Cust_Key')\
                        .whenMatchedUpdateAll()\
                        .whenNotMatchedInsertAll()\
                        .execute()
else:
    df_final.write.mode('overwrite')\
        .format('delta')\
        .option('path','abfss://gold@chaitanyadatabricksete.dfs.core.windows.net/Dim_Customers')\
        .saveAsTable('databricks_cat.gold.Dim_Customers')