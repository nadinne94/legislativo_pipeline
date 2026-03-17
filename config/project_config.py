# Databricks notebook source
# MAGIC %md
# MAGIC # CONFIGURAÇÃO GLOBAL DO PROJETO
# MAGIC Este notebook centraliza todas as constantes e configurações utilizadas no pipeline de dados legislativos.

# COMMAND ----------

# DBTITLE 1,Importações de Dependências
# Importação das classes necessárias para inicialização do Spark e manipulação de dados
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# DBTITLE 1,Classe de Configuração do Projeto
class ProjectConfig:
    """
    Classe que armazena as configurações de API, períodos de análise e caminhos de armazenamento.
    Centralizar essas informações facilita a manutenção e evolução do projeto.
    """

    # Configurações da API da Câmara
    API_BASE_URL = "https://dadosabertos.camara.leg.br/api/v2"
    PAGE_SIZE = 100
    MAX_PAGES_PER_EXECUTION = 10
    REQUEST_TIMEOUT = 30

    # Período de análise definido para o mandato atual (2023 - 2027)
    START_YEAR = 2023
    END_YEAR = 2027

    # Definição dos caminhos no Data Lake (S3) seguindo a arquitetura Medallion
    BASE_PATH = "s3://api-legislativo-databricks"
    BRONZE_PATH = f"{BASE_PATH}/bronze"
    SILVER_PATH = f"{BASE_PATH}/silver"
    GOLD_PATH = f"{BASE_PATH}/gold"
    METADATA_PATH = f"{BASE_PATH}/metadata"

    # Nomes dos Schemas/Bancos de dados
    BRONZE_SCHEMA = "bronze"
    SILVER_SCHEMA = "silver"
    GOLD_SCHEMA = "gold"

    # Configurações de performance para ingestão paralela
    MAX_PARENT_IDS = 1000  # Limite de registros pai para buscar dados dependentes
    MAX_WORKERS = 8       # Número de threads para chamadas simultâneas à API

# COMMAND ----------

# DBTITLE 1,Inicialização da Sessão Spark
# No Databricks, a sessão Spark já costuma estar disponível, 
# mas garantimos sua inicialização para compatibilidade.
spark = SparkSession.builder.appName("LegislativoPipeline").getOrCreate()

# COMMAND ----------

# DBTITLE 1,Criação da Estrutura de Pastas
# Garante que os diretórios base existam no S3 antes de iniciar a ingestão
dbutils.fs.mkdirs(ProjectConfig.BRONZE_PATH)
dbutils.fs.mkdirs(ProjectConfig.SILVER_PATH)
dbutils.fs.mkdirs(ProjectConfig.GOLD_PATH)

# COMMAND ----------

# DBTITLE 1,Funções Utilitárias de Configuração
def add_metadata_columns(df):
    """Adiciona colunas de auditoria aos DataFrames."""
    return df.withColumn("data_ingestao", current_timestamp())

# COMMAND ----------

# DBTITLE 1,Log de Inicialização
print("Configuração do projeto carregada com sucesso.")
print(f"Bronze Path: {ProjectConfig.BRONZE_PATH}")
print(f"Silver Path: {ProjectConfig.SILVER_PATH}")
print(f"Gold Path: {ProjectConfig.GOLD_PATH}")
