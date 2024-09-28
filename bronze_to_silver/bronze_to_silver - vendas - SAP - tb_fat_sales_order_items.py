# Databricks notebook source
from pyspark.sql.types import DecimalType
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql import functions as F

# COMMAND ----------

tmp_delta_path = "/tmp/bronze/tb_fat_sales_order_items"
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
.withColumnRenamed('CURRENCY', 'ds_currency')
.withColumnRenamed('DELIVERYDATE', 'dt_delivery')
.withColumnRenamed('GROSSAMOUNT', 'qt_gross_amount')
.withColumnRenamed('ITEMATPSTATUS', 'cd_item_atp_status')
.withColumnRenamed('NETAMOUNT', 'vl_net_amount')
.withColumnRenamed('NOTEID', 'ds_note_id')
.withColumnRenamed('OPITEMPOS', 'ds_op_item_pos')
.withColumnRenamed('PRODUCTID', 'ds_product_id')
.withColumnRenamed('QUANTITY', 'qt_quantity')
.withColumnRenamed('QUANTITYUNIT', 'ds_quantity_unit')
.withColumnRenamed('SALESORDERID','cd_sales_order_id')
.withColumnRenamed('SALESORDERITEM','qt_sales_order_item')
.withColumnRenamed('TAXAMOUNT', 'vl_taxa_amount')
.withColumnRenamed('dt_ingestion', 'dt_ingestion'))

display(df)

# COMMAND ----------

#Tratamento das colunas de data
df = df.withColumn(
    "dt_delivery",
    expr("substring(dt_delivery, 7, 2) || '-' || substring(dt_delivery, 5, 2) || '-' || substring(dt_delivery, 1, 4)")
)

display(df)

# COMMAND ----------

#Formatar as colunas
df = (df
.withColumn('ds_currency', df['ds_currency'].cast('string'))
.withColumn("dt_delivery", F.to_date(F.col("dt_delivery"), "dd-MM-yyyy"))
.withColumn('qt_gross_amount', df['qt_gross_amount'].cast('int'))
.withColumn('cd_item_atp_status', df['cd_item_atp_status'].cast('string'))
.withColumn('vl_net_amount', df['vl_net_amount'].cast(DecimalType(10,2)))
.withColumn('ds_note_id', df['ds_note_id'].cast('string'))
.withColumn('ds_op_item_pos', df['ds_op_item_pos'].cast('string'))
.withColumn('ds_product_id', df['ds_product_id'].cast('string'))
.withColumn('qt_quantity', df['qt_quantity'].cast('int'))
.withColumn('ds_quantity_unit', df['ds_quantity_unit'].cast('string'))
.withColumn('cd_sales_order_id', df['cd_sales_order_id'].cast('string'))
.withColumn('qt_sales_order_item', df['qt_sales_order_item'].cast('int'))
.withColumn('vl_taxa_amount', df['vl_taxa_amount'].cast(DecimalType(10,2))))

display(df)

# COMMAND ----------

delta = "/tmp/silver/tb_fat_sales_order_items"
df.write.partitionBy("dt_ingestion").format("delta").mode("overwrite").save(delta)

# COMMAND ----------

# MAGIC %fs ls /tmp/silver/tb_fat_sales_order_items
