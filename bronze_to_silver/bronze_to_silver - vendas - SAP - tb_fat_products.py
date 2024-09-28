# Databricks notebook source
from pyspark.sql.types import DecimalType
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql import functions as F

# COMMAND ----------

tmp_delta_path = "/tmp/bronze/tb_fat_products"
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
.withColumnRenamed('CHANGEDAT', 'dt_change')
.withColumnRenamed('CHANGEDBY', 'cd_change_by')
.withColumnRenamed('CREATEDAT', 'dt_create')
.withColumnRenamed('CREATEDBY', 'cd_create_by')
.withColumnRenamed('CURRENCY', 'ds_currency')
.withColumnRenamed('DEPTH', 'ds_depth')
.withColumnRenamed('DIMENSIONUNIT', 'ds_dimension_unit')
.withColumnRenamed('HEIGHT', 'ds_height')
.withColumnRenamed('PRICE', 'vl_price')
.withColumnRenamed('PRODCATEGORYID', 'cd_prod_category_id')
.withColumnRenamed('PRODUCTID', 'cd_product_id')
.withColumnRenamed('PRODUCTPICURL', 'ds_product_picurl')
.withColumnRenamed('QUANTITYUNIT', 'ds_quantity_unit')
.withColumnRenamed('SUPPLIER_PARTNERID', 'cd_supplier_partner_id')
.withColumnRenamed('TAXTARIFFCODE', 'cd_tax_tariff_code')
.withColumnRenamed('TYPECODE', 'ds_type_code')
.withColumnRenamed('WEIGHTMEASURE', 'vl_weight_measure')
.withColumnRenamed('WEIGHTUNIT', 'ds_weight_unit')
.withColumnRenamed('WIDTH', 'ds_width')
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
display(df)

# COMMAND ----------

#Formatar as colunas
df = (df
.withColumn("dt_change", F.to_date(F.col("dt_change"), "dd-MM-yyyy"))
.withColumn('cd_change_by', df['cd_change_by'].cast('string'))
.withColumn("dt_create", F.to_date(F.col("dt_create"), "dd-MM-yyyy"))
.withColumn('cd_create_by', df['cd_create_by'].cast('string'))
.withColumn('ds_currency', df['ds_currency'].cast('string'))
.withColumn('ds_depth', df['ds_depth'].cast('string'))
.withColumn('ds_dimension_unit', df['ds_dimension_unit'].cast('string'))
.withColumn('ds_height', df['ds_height'].cast('string'))
.withColumn('vl_price', df['vl_price'].cast(DecimalType(10,2)))
.withColumn('cd_prod_category_id', df['cd_prod_category_id'].cast('string'))
.withColumn('cd_product_id', df['cd_product_id'].cast('string'))
.withColumn('ds_product_picurl', df['ds_product_picurl'].cast('string'))
.withColumn('ds_quantity_unit', df['ds_quantity_unit'].cast('string'))
.withColumn('cd_supplier_partner_id', df['cd_supplier_partner_id'].cast('string'))
.withColumn('ds_type_code', df['ds_type_code'].cast('string'))
.withColumn('vl_weight_measure', df['vl_weight_measure'].cast(DecimalType(10,2)))
.withColumn('ds_weight_unit', df['ds_weight_unit'].cast('string'))
.withColumn('ds_width', df['ds_width'].cast('string')))


display(df)

# COMMAND ----------

delta = "/tmp/silver/tb_fat_products"
df.write.partitionBy("dt_ingestion").format("delta").mode("overwrite").save(delta)

# COMMAND ----------

# MAGIC %fs ls /tmp/silver/tb_fat_sales_orders
