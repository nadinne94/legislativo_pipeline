# Databricks notebook source
# MAGIC %md
# MAGIC # UTILITÁRIOS DE LOGGING DO PIPELINE
# MAGIC Este notebook gerencia a gravação de logs de execução para monitorar o status de cada dataset processado.

# COMMAND ----------

# DBTITLE 1,Carregar Configurações do Projeto
# MAGIC %run ../config/project_config

# COMMAND ----------

# DBTITLE 1,Importação de Dependências Spark
# Bibliotecas para manipulação de timestamps e definição de schemas
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# COMMAND ----------

# DBTITLE 1,Configuração da Tabela de Logs
# Caminho Delta no S3 para armazenar o histórico de execuções
LOG_TABLE = f"{ProjectConfig.METADATA_PATH}/pipeline_logs"

# Definição do schema fixo para garantir a integridade dos logs
LOG_SCHEMA = StructType([
    StructField("dataset", StringType(), True),
    StructField("status", StringType(), True),
    StructField("message", StringType(), True),
    StructField("records", IntegerType(), True)
])

# COMMAND ----------

# DBTITLE 1,Função para Gravar Eventos no Log
def log_pipeline_event(dataset, status, message=None, records=None):
    """
    Registra o início, sucesso ou erro de uma etapa do pipeline.
    Adiciona automaticamente o timestamp atual de execução.
    """
    # Prepara os dados do log em formato de dicionário
    data = [{
        "dataset": dataset,
        "status": status,
        "message": message,
        "records": records
    }]

    # Cria o DataFrame com schema explícito para evitar erros de inferência
    df = spark.createDataFrame(data, schema=LOG_SCHEMA)

    # Adiciona a coluna de timestamp no momento da gravação
    df = df.withColumn("timestamp", current_timestamp())

    # Grava o log no modo 'append' para manter o histórico completo
    (
        df.write
        .format("delta")
        .mode("append")
        .save(LOG_TABLE)
    )

    # Feedback visual no console do Databricks
    print(f"Log gravado: {dataset} | Status: {status}")
