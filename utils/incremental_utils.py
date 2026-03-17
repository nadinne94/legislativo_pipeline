# Databricks notebook source
# MAGIC %md
# MAGIC # UTILITÁRIOS DE CONTROLE INCREMENTAL (WATERMARK)
# MAGIC Este notebook gerencia o estado da última execução de cada dataset para garantir que o pipeline colete apenas dados novos em cada rodada.

# COMMAND ----------

# DBTITLE 1,Carregar Configurações do Projeto
# MAGIC %run ../config/project_config

# COMMAND ----------

# DBTITLE 1,Importação de Dependências Spark e Delta
# Bibliotecas para manipulação de tipos de dados e comando MERGE (Upsert) do Delta Lake
from pyspark.sql.types import StructType, StructField, StringType
from delta.tables import DeltaTable

# COMMAND ----------

# DBTITLE 1,Configuração da Tabela de Watermark
# Localização da tabela que armazena a última data processada para cada dataset
WATERMARK_TABLE = f"{ProjectConfig.METADATA_PATH}/pipeline_watermark"

# Schema fixo para garantir a consistência dos metadados de controle
WATERMARK_SCHEMA = StructType([
    StructField("dataset", StringType(), True),
    StructField("last_value", StringType(), True)
])

# COMMAND ----------

# DBTITLE 1,Função para Recuperar o Watermark Atual
def get_watermark(dataset):
    """
    Busca o valor da última execução bem-sucedida para um dataset específico.
    Retorna None se for a primeira execução ou se a tabela ainda não existir.
    """
    try:
        # Carrega a tabela de controle e filtra pelo dataset solicitado
        df = spark.read.format("delta").load(WATERMARK_TABLE)
        result = df.filter(df.dataset == dataset).collect()

        # Se houver resultado, retorna o valor da coluna 'last_value'
        if len(result) == 0:
            return None
        return result[0]["last_value"]

    except Exception as e:
        # Retorna None em caso de erro (ex: tabela inexistente na primeira rodada)
        print(f"Aviso: Não foi possível recuperar o watermark para {dataset}: {e}")
        return None

# COMMAND ----------

# DBTITLE 1,Função para Atualizar o Watermark (Upsert)
def update_watermark(dataset, value):
    """
    Atualiza o valor de controle de um dataset após uma execução bem-sucedida.
    Utiliza a lógica de MERGE para evitar duplicidade de registros de controle.
    """
    # Prepara o dado de controle em formato de dicionário
    data = [{
        "dataset": dataset,
        "last_value": value
    }]

    # Cria o DataFrame com schema explícito para evitar erros de tipo
    df = spark.createDataFrame(data, schema=WATERMARK_SCHEMA)

    # Verifica se a tabela de controle já existe no S3
    if not DeltaTable.isDeltaTable(spark, WATERMARK_TABLE):
        # Se não existe, cria a tabela pela primeira vez com overwrite
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .save(WATERMARK_TABLE)
        )
        print(f"Tabela de watermark criada pela primeira vez para: {dataset}")
    else:
        # Se existe, utiliza MERGE (Upsert) para atualizar o valor do dataset
        # Isso garante que cada dataset tenha apenas uma linha na tabela de controle
        delta_table = DeltaTable.forPath(spark, WATERMARK_TABLE)
        
        # Mapeamento explícito para evitar erros de colunas extras
        update_map = {col_name: f"source.{col_name}" for col_name in df.columns}

        (
            delta_table.alias("target")
            .merge(
                df.alias("source"),
                "target.dataset = source.dataset"
            )
            .whenMatchedUpdate(set=update_map)
            .whenNotMatchedInsert(values=update_map)
            .execute()
        )
        print(f"Watermark atualizado com sucesso: {dataset} -> {value}")
