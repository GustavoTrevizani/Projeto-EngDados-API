# Databricks notebook source
import requests
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan
import great_expectations as ge

# COMMAND ----------

spark = SparkSession.builder.appName("EXT_SAP_API").getOrCreate()

# COMMAND ----------

class Ext_SAP_API:

    def __init__(self, area, arquivo, chave,spark):
        self.area = area 
        self.arquivo = arquivo
        self.chave = chave
        self.spark = spark

    
    def get_dados_archive(self):
        url_base = "https://datasap-293251100165.herokuapp.com/dados"
        url = f"{url_base}/{self.area}/{self.arquivo}"
        auth = ("", f"{self.chave}")

        response = requests.get(url, auth=auth)
        if response.status_code == 200:
            data = response.json()
            return data 
        else:
            print("Erro: Não foi possível obter dados da API.")
            return None
        
    def create_dataframe(self):
        dados = self.get_dados_archive()
        if dados is not None:
            df = self.spark.createDataFrame(dados)
            return df
        else:
            print("Não foi possível obter dados da API.")
            return None

# COMMAND ----------

area = 'SAP'
arquivo = 'Products'
chave = 'meizterdevs2024'

# COMMAND ----------

extract = Ext_SAP_API(area, arquivo, chave, spark)

#Criar o dataframe do spark e imprimir
df = extract.create_dataframe()

if df is not None:
    print('Dataframe criado com sucesso!')
    display(df)
else:
    print("Não foi possível criar o Dataframe.")

# COMMAND ----------

# MAGIC %md
# MAGIC Verificação de qualidade

# COMMAND ----------

df_ge = ge.dataset.SparkDFDataset(df)

#Expectativa: Verifica a presença das colunas esperadas
colunas_esperadas = ['CHANGEDAT',
 'CHANGEDBY',
 'CREATEDAT',
 'CREATEDBY',
 'CURRENCY',
 'DEPTH',
 'DIMENSIONUNIT',
 'HEIGHT',
 'PRICE',
 'PRODCATEGORYID',
 'PRODUCTID',
 'PRODUCTPICURL',
 'QUANTITYUNIT',
 'SUPPLIER_PARTNERID',
 'TAXTARIFFCODE',
 'TYPECODE',
 'WEIGHTMEASURE',
 'WEIGHTUNIT',
 'WIDTH']

expectativa_colunas = df_ge.expect_table_columns_to_match_ordered_list(column_list=colunas_esperadas)

#validar as expectativas
resultado_validacao = df_ge.validate()

#verifica o resultado
if resultado_validacao['success']:
    print('Arquivo Ok!')
else:
    print('Erro na validação!')

# COMMAND ----------

tmp_delta_path = "/tmp/transient/tb_fat_products"
df.write.format("parquet").mode("overwrite").save(tmp_delta_path)
