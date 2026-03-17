# Databricks notebook source
# MAGIC %md
# MAGIC # CONFIGURAÇÃO DOS DATASETS DA BRONZE LAYER
# MAGIC Configurações de schemas e parâmetros para ingestão da API da Câmara.

# COMMAND ----------

# DBTITLE 1,Carregar configuração do projeto
# MAGIC %run ./project_config

# COMMAND ----------

# MAGIC %run ./bronze_sources

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, LongType

# Esquemas para as tabelas Bronze
deputados_schema = StructType([
    StructField("id", LongType(), True),
    StructField("uri", StringType(), True),
    StructField("nomeCivil", StringType(), True),
    StructField("siglaPartido", StringType(), True),
    StructField("uf", StringType(), True),
    StructField("urlFoto", StringType(), True),
    StructField("email", StringType(), True),
    StructField("dataNascimento", StringType(), True),
    StructField("sexo", StringType(), True),
    StructField("data_ingestao", TimestampType(), True)
])

partidos_schema = StructType([
    StructField("id", LongType(), True),
    StructField("uri", StringType(), True),
    StructField("sigla", StringType(), True),
    StructField("nome", StringType(), True),
    StructField("data_ingestao", TimestampType(), True)
])

proposicoes_schema = StructType([
    StructField("id", LongType(), True),
    StructField("uri", StringType(), True),
    StructField("siglaTipo", StringType(), True),
    StructField("numero", IntegerType(), True),
    StructField("ano", IntegerType(), True),
    StructField("ementa", StringType(), True),
    StructField("dataApresentacaoInicio", StringType(), True),
    StructField("data_ingestao", TimestampType(), True)
])

proposicoes_autores_schema = StructType([
    StructField("idProposicao", LongType(), True),
    StructField("idAutor", LongType(), True),
    StructField("parent_id", LongType(), True),
    StructField("nomeAutor", StringType(), True),
    StructField("tipoAutor", StringType(), True),
    StructField("data_ingestao", TimestampType(), True)
])

votacoes_schema = StructType([
    StructField("id", StringType(), True),
    StructField("idProposicao", StringType(), True),
    StructField("uri", StringType(), True),
    StructField("data", StringType(), True),
    StructField("descricao", StringType(), True),
    StructField("data_ingestao", TimestampType(), True)
])

votos_schema = StructType([
    StructField("idVotacao", LongType(), True),
    StructField("idDeputado", LongType(), True),
    StructField("parent_id", LongType(), True),
    StructField("nomeDeputado", StringType(), True),
    StructField("voto", StringType(), True),
    StructField("data_ingestao", TimestampType(), True)
])

eventos_schema = StructType([
    StructField("id", LongType(), True),
    StructField("uri", StringType(), True),
    StructField("dataHoraInicio", StringType(), True),
    StructField("descricao", StringType(), True),
    StructField("data_ingestao", TimestampType(), True)
])

presencas_eventos_schema = StructType([
    StructField("idEvento", LongType(), True),
    StructField("idDeputado", LongType(), True),
    StructField("parent_id", LongType(), True),
    StructField("nomeDeputado", StringType(), True),
    StructField("tipoPresenca", StringType(), True),
    StructField("data_ingestao", TimestampType(), True)
])

BRONZE_SCHEMAS = {
    "deputados": deputados_schema,
    "partidos": partidos_schema,
    "proposicoes": proposicoes_schema,
    "proposicoes_autores": proposicoes_autores_schema,
    "votacoes": votacoes_schema,
    "votos": votos_schema,
    "eventos": eventos_schema,
    "presencas_eventos": presencas_eventos_schema
}

# COMMAND ----------

# DBTITLE 1,Definição dos datasets Bronze
CAMARA_DATASETS = {
    "deputados": {
        "source": "camara",
        "endpoint": "/deputados",
        "table": "deputados",
        "description": "Lista de deputados federais",
        "incremental": False,
        "pagination": True,
        "merge_keys": ["id"],
        "schema": BRONZE_SCHEMAS["deputados"]
    },
    "partidos": {
        "source": "camara",
        "endpoint": "/partidos",
        "table": "partidos",
        "description": "Lista de partidos",
        "incremental": False,
        "pagination": True,
        "merge_keys": ["id"],
        "schema": BRONZE_SCHEMAS["partidos"]
    },
    "proposicoes": {
        "source": "camara",
        "endpoint": "/proposicoes",
        "table": "proposicoes",
        "description": "Projetos de lei",
        "incremental": True,
        "incremental_param": "dataApresentacaoInicio",
        "incremental_field": "dataApresentacao",
        "pagination": True,
        "params": {
            "dataApresentacaoInicio": "2023-01-01", # OBRIGATÓRIO para evitar 0 registros
            "itens": ProjectConfig.PAGE_SIZE
        },
        "merge_keys": ["id"],
        "schema": BRONZE_SCHEMAS["proposicoes"]
    },
    "proposicoes_autores": {
        "source": "camara",
        "endpoint": "/proposicoes/{id}/autores",
        "table": "proposicoes_autores",
        "description": "Autores das proposições",
        "incremental": False,
        "pagination": False,
        "requires_parent": "proposicoes",
        "merge_keys": ["idProposicao", "idAutor"],
        "schema": BRONZE_SCHEMAS["proposicoes_autores"]
    },
    "votacoes": {
        "source": "camara",
        "endpoint": "/votacoes",
        "table": "votacoes",
        "description": "Votações da Câmara",
        "incremental": True,
        "incremental_param": "dataInicio",
        "incremental_field": "data",
        "pagination": True,
        "params": {
            "dataInicio": "2023-01-01", # OBRIGATÓRIO para evitar 0 registros
            "itens": ProjectConfig.PAGE_SIZE
        },
        "merge_keys": ["id"],
        "schema": BRONZE_SCHEMAS["votacoes"]
    },
    "votos": {
        "source": "camara",
        "endpoint": "/votacoes/{id}/votos",
        "table": "votos",
        "description": "Votos dos deputados",
        "incremental": False,
        "pagination": False,
        "requires_parent": "votacoes",
        "merge_keys": ["idVotacao", "idDeputado"],
        "schema": BRONZE_SCHEMAS["votos"]
    },
    "eventos": {
        "source": "camara",
        "endpoint": "/eventos",
        "table": "eventos",
        "description": "Eventos e sessões",
        "incremental": True,
        "incremental_param": "dataInicio",
        "incremental_field": "dataHoraInicio",
        "pagination": True,
        "params": {
            "dataInicio": "2023-01-01", # OBRIGATÓRIO para evitar 0 registros
            "dataFim": "2027-12-31",
            "itens": ProjectConfig.PAGE_SIZE
        },
        "merge_keys": ["id"],
        "schema": BRONZE_SCHEMAS["eventos"]
    },
    "presencas_eventos": {
        "source": "camara",
        "endpoint": "/eventos/{id}/deputados",
        "table": "presencas_eventos",
        "description": "Presença dos deputados",
        "incremental": False,
        "pagination": False,
        "requires_parent": "eventos",
        "merge_keys": ["idEvento", "idDeputado"],
        "schema": BRONZE_SCHEMAS["presencas_eventos"]
    }
}
