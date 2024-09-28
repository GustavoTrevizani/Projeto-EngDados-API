# Databricks notebook source
import great_expectations as ge
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC Carregamento

# COMMAND ----------

tmp_parquet_path = "/tmp/transient/tb_fat_products"
df = spark.read.format("parquet").load(tmp_parquet_path)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Verificações

# COMMAND ----------

df_ge = ge.dataset.SparkDFDataset(df)

#Verificar se a coluna 'cd_order' não possui valores nulos
expectativa_nulos = df_ge.expect_column_values_to_not_be_null(column='CHANGEDBY')

#Outra coluna para verificação
expectativa_nulos_outra = df_ge.expect_column_values_to_not_be_null(column='CREATEDBY')

#Validar as expectativas
resultado_validacao = df_ge.validate()

if resultado_validacao['success']:
    print('Arquivo Ok!')
else:
    print('Erro na validação!')

# COMMAND ----------

df = df.withColumn("dt_ingestion", F.date_format(F.current_date(), "dd-MM-yyyy"))

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Salvar no formato Delta

# COMMAND ----------

delta = "/tmp/bronze/tb_fat_products"
df.write.partitionBy("dt_ingestion").format("delta").mode("overwrite").save(delta)

# COMMAND ----------

# MAGIC %fs ls dbfs:/tmp/bronze/tb_fat_products
