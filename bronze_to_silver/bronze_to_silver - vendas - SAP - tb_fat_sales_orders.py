# Databricks notebook source
from pyspark.sql.types import DecimalType
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql import functions as F

# COMMAND ----------

tmp_delta_path = "/tmp/bronze/tb_fat_sales_orders"
df = spark.read.format("delta").load(tmp_delta_path)

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC |Prefixo|Utilização|
# MAGIC |-------|-------|
# MAGIC |cd|código|
# MAGIC |nm|nome|
# MAGIC |ds|descrição|
# MAGIC |dt|data|
# MAGIC |hr|hora|
# MAGIC |nr|número|
# MAGIC |vl|valor|
# MAGIC |bool|booleano|
# MAGIC |txt|Texto extenso|
# MAGIC |id|Chave composta|
# MAGIC |qt|quantidade|
# MAGIC

# COMMAND ----------

df = (df
.withColumnRenamed('BILLINGSTATUS', 'ds_billing_status')
.withColumnRenamed('CHANGEDAT', 'dt_change')
.withColumnRenamed('CHANGEDBY', 'cd_change_by')
.withColumnRenamed('CREATEDAT', 'dt_create')
.withColumnRenamed('CREATEDBY', 'cd_create_by')
.withColumnRenamed('CURRENCY', 'ds_currency')
.withColumnRenamed('DELIVERYSTATUS', 'ds_delivery_status')
.withColumnRenamed('FISCALYEARPERIOD', 'dt_fiscal_period')
.withColumnRenamed('FISCVARIANT', 'ds_fisc_variant')
.withColumnRenamed('GROSSAMOUNT', 'qt_gross_amount')
.withColumnRenamed('LIFECYCLESTATUS', 'ds_life_cycle_status')
.withColumnRenamed('NETAMOUNT', 'vl_net_amount')
.withColumnRenamed('NOTEID', 'ds_note_id')
.withColumnRenamed('PARTNERID', 'cd_partner')
.withColumnRenamed('SALESORDERID','cd_sales_order')
.withColumnRenamed('SALESORG', 'ds_sales_org')
.withColumnRenamed('TAXAMOUNT', 'vl_taxa_amount')
.withColumnRenamed('dt_ingestion', 'dt_ingestion'))

display(df)

# COMMAND ----------

#Tratamento das colunas de data
df = df.withColumn(
    "dt_change",
    expr("substring(dt_change, 7, 2) || '-' || substring(dt_change, 5, 2) || '-' || substring(dt_change, 1, 4)")
)
df = df.withColumn(
    "dt_create",
    expr("substring(dt_create, 7, 2) || '-' || substring(dt_create, 5, 2) || '-' || substring(dt_create, 1, 4)")
)
df = df.withColumn(
    "dt_fiscal_period",
    F.to_date(F.substring(F.col("dt_fiscal_period"), 1, 4).cast("string"), "yyyy")
)
display(df)

# COMMAND ----------

#Formatar as colunas
df = (df
.withColumn('ds_billing_status', df['ds_billing_status'].cast('string'))
.withColumn("dt_change", F.to_date(F.col("dt_change"), "dd-MM-yyyy"))
.withColumn('cd_change_by', df['cd_change_by'].cast('string'))
.withColumn("dt_create", F.to_date(F.col("dt_create"), "dd-MM-yyyy"))
.withColumn('cd_create_by', df['cd_create_by'].cast('string'))
.withColumn('ds_currency', df['ds_currency'].cast('string'))
.withColumn('ds_delivery_status', df['ds_delivery_status'].cast('string'))
.withColumn("dt_fiscal_period", F.to_date(F.col("dt_fiscal_period"), "dd-MM-yyyy"))
.withColumn('ds_fisc_variant', df['ds_fisc_variant'].cast('string'))
.withColumn('qt_gross_amount', df['qt_gross_amount'].cast('int'))
.withColumn('ds_life_cycle_status', df['ds_life_cycle_status'].cast('string'))
.withColumn('vl_net_amount', df['vl_net_amount'].cast(DecimalType(10,2)))
.withColumn('ds_note_id', df['ds_note_id'].cast('string'))
.withColumn('cd_partner', df['cd_partner'].cast('string'))
.withColumn('cd_sales_order', df['cd_sales_order'].cast('string'))
.withColumn('ds_sales_org', df['ds_sales_org'].cast('string'))
.withColumn('vl_taxa_amount', df['vl_taxa_amount'].cast(DecimalType(10,2))))


display(df)



# COMMAND ----------

delta = "/tmp/silver/tb_fat_sales_orders"
df.write.partitionBy("dt_ingestion").format("delta").mode("overwrite").save(delta)

# COMMAND ----------

# MAGIC %fs ls /tmp/silver/tb_fat_sales_orders
