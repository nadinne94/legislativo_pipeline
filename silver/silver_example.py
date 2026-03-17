# Databricks notebook source
# MAGIC %md
# MAGIC # PROCESSAMENTO DA CAMADA SILVER (EXEMPLO)
# MAGIC Este notebook demonstra como realizar a limpeza, padronização e enriquecimento de dados da camada Bronze para a camada Silver.

# COMMAND ----------

# DBTITLE 1,Carregar Configurações Globais
# MAGIC %run ../config/project_config

# COMMAND ----------

# DBTITLE 1,Importação de Funções Spark SQL
# Importação das funções necessárias para transformações de data, colunas e auditoria
from pyspark.sql.functions import col, to_date, lit, current_timestamp

# COMMAND ----------

# DBTITLE 1,Leitura de Dados da Camada Bronze
# Carrega a tabela bruta de deputados da camada Bronze
try:
    bronze_path = f"{ProjectConfig.BRONZE_PATH}/deputados"
    bronze_df = spark.read.format("delta").load(bronze_path)
    print("Dados da Bronze carregados com sucesso.")
    bronze_df.printSchema()
except Exception as e:
    print(f"Erro ao carregar dados da Bronze em {bronze_path}: {e}")
    dbutils.notebook.exit("Falha na leitura da Bronze.")

# COMMAND ----------

# DBTITLE 1,Transformações e Padronização (Silver)
"""
Na camada Silver, realizamos:
1. Renomeação de colunas para snake_case padrão.
2. Conversão de tipos (ex: String para Date).
3. Seleção de colunas relevantes para o negócio.
"""
silver_df = bronze_df.select(
    col("id").alias("id_deputado"),
    col("nomeCivil").alias("nome_completo"),
    col("siglaPartido").alias("sigla_partido"),
    col("uf").alias("uf_origem"),
    # Converte data de nascimento para o tipo Date nativo do Spark
    to_date(col("dataNascimento"), "yyyy-MM-dd").alias("data_nascimento"),
    col("sexo"),
    col("data_ingestao").alias("data_ingestao_bronze")
).withColumn("data_processamento_silver", current_timestamp())

print("Schema da camada Silver definido:")
silver_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Gravação na Camada Silver (S3)
# Define o nome da tabela e o caminho de destino no Data Lake
table_name = "deputados"
path = f"{ProjectConfig.SILVER_PATH}/{table_name}"

# Grava os dados limpos em formato Delta, permitindo evolução de schema se necessário
(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(path)
)

print(f"Tabela Silver '{table_name}' gravada com sucesso em: {path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## PRÓXIMOS PASSOS SUGERIDOS:
# MAGIC *   Adicionar validações de qualidade de dados (ex: campos nulos ou formatos inválidos).
# MAGIC *   Realizar JOINs com outras tabelas Silver (ex: partidos) para enriquecer o dataset.
# MAGIC *   Implementar testes unitários para as transformações.
