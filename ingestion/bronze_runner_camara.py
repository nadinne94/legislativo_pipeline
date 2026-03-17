# Databricks notebook source
# MAGIC %md
# MAGIC # INGESTÃO DE DADOS DA CÂMARA (LAYER BRONZE)
# MAGIC Este notebook orquestra a ingestão de dados brutos da API da Câmara dos Deputados para a camada Bronze, utilizando lógica de MERGE (Upsert) para evitar duplicidade.

# COMMAND ----------

# DBTITLE 1,Parâmetros de Execução do Pipeline
dbutils.widgets.removeAll()
dbutils.widgets.text("dataset", "all", "Dataset para ingestão (ex: deputados, proposicoes ou 'all')")
dataset_param = dbutils.widgets.get("dataset")

# COMMAND ----------

# DBTITLE 1,Carregamento de Dependências
# MAGIC %run ../config/project_config

# COMMAND ----------

# MAGIC %run ../config/camara_datasets

# COMMAND ----------

# MAGIC %run ../utils/api_utils

# COMMAND ----------

# MAGIC %run ../utils/logging_utils

# COMMAND ----------

# MAGIC %run ../utils/incremental_utils

# COMMAND ----------

# DBTITLE 1,Importações Adicionais
from pyspark.sql.functions import current_timestamp, lit
from concurrent.futures import ThreadPoolExecutor
from delta.tables import DeltaTable
import time
import os
from datetime import datetime

# COMMAND ----------

# DBTITLE 1,Função de Escrita com Lógica de MERGE (Upsert)
def write_bronze(df, dataset_name, config):
    """
    Grava os dados na camada Bronze utilizando MERGE (Upsert).
    Remove duplicatas antes de gravar para evitar erros de condição não determinística.
    """
    table_name = config["table"]
    path = f"{ProjectConfig.BRONZE_PATH}/camara/{table_name}"
    
    # GARANTIA DE UNICIDADE: Remove duplicatas do DataFrame de origem baseado nas chaves de merge
    merge_keys = config.get("merge_keys", ["id"])
    df = df.dropDuplicates(merge_keys)
    
    if not DeltaTable.isDeltaTable(spark, path):
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
        print(f"Tabela {table_name} criada com sucesso.")
    else:
        merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in merge_keys])
        delta_table = DeltaTable.forPath(spark, path)
        
        # Mapeamento explícito de colunas para evitar conflitos de schema
        update_map = {col_name: f"source.{col_name}" for col_name in df.columns}
        
        (delta_table.alias("target")
            .merge(df.alias("source"), merge_condition)
            .whenMatchedUpdate(set=update_map)
            .whenNotMatchedInsert(values=update_map)
            .execute())
        print(f"Tabela {table_name} atualizada via MERGE.")

# COMMAND ----------

# DBTITLE 1,Coleta Paralela para Datasets Dependentes
def fetch_nested_data(endpoint_template, parent_ids):
    results = []
    def fetch_single(resource_id):
        try:
            endpoint = format_endpoint(endpoint_template, resource_id)
            data = get_all_pages(endpoint)
            for row in data:
                row["parent_id"] = resource_id
            return data
        except Exception as e:
            print(f"Erro no ID {resource_id}: {e}")
            return []

    with ThreadPoolExecutor(max_workers=ProjectConfig.MAX_WORKERS) as executor:
        responses = executor.map(fetch_single, parent_ids)
        for response in responses:
            results.extend(response)
    return results

# COMMAND ----------

# DBTITLE 1,Ingestão de Datasets Simples
def ingest_simple_dataset(dataset_name, config):

    log_pipeline_event(dataset_name, "START")

    endpoint = config["endpoint"]

    # parâmetros base do dataset
    params = dict(config.get("params", {}))

    watermark = get_watermark(dataset_name)
    incremental_param = config.get("incremental_param")

    # ----------------------------
    # LÓGICA INCREMENTAL
    # ----------------------------

    if config.get("incremental") and incremental_param:

        if watermark:

            params[incremental_param] = watermark

            # fecha intervalo até hoje
            if incremental_param == "dataApresentacaoInicio":
                params["dataApresentacaoFim"] = datetime.now().strftime("%Y-%m-%d")

            elif incremental_param == "dataInicio":
                params["dataFim"] = datetime.now().strftime("%Y-%m-%d")

            print(f"Execução Incremental: {incremental_param} >= {watermark}")

        else:

            print(f"Primeira Execução: Usando filtros do config: {params}")

    # ----------------------------
    # CHAMADA DA API
    # ----------------------------

    data = get_all_pages(endpoint, params)
    if not data:
        print(f"Nenhum registro retornado para {dataset_name}")
        return

    # Cria DataFrame com schema flexível para evitar erros de campos faltantes na API
    df_raw = spark.createDataFrame(data)
    
    # Seleciona apenas as colunas que existem tanto no dado quanto no schema esperado
    # Preenche com nulo as colunas que estão no schema mas não vieram no dado
    expected_cols = config["schema"].fieldNames()
    for col_name in expected_cols:
        if col_name not in df_raw.columns and col_name != "data_ingestao":
            df_raw = df_raw.withColumn(col_name, lit(None))
            
    df = df_raw.select(*[c for c in expected_cols if c != "data_ingestao"])
    df = df.withColumn("data_ingestao", current_timestamp())

    # ----------------------------
    # ATUALIZA WATERMARK
    # ----------------------------

    incremental_field = config.get("incremental_field")

    if incremental_field and incremental_field in df.columns:

        max_val = df.agg({incremental_field: "max"}).collect()[0][0]

        if max_val:
            update_watermark(dataset_name, str(max_val))

    # ----------------------------
    # WRITE BRONZE
    # ----------------------------

    write_bronze(df, dataset_name, config)

    log_pipeline_event(dataset_name, "SUCCESS", records=len(data))

# COMMAND ----------

# DBTITLE 1,Ingestão de Datasets Dependentes
def ingest_nested_dataset(dataset_name, config):
    log_pipeline_event(dataset_name, "START")
    parent_dataset = config["requires_parent"]
    parent_table = CAMARA_DATASETS[parent_dataset]["table"]
    parent_path = f"{ProjectConfig.BRONZE_PATH}/camara/{parent_table}"

    try:
        parent_df = spark.read.format("delta").load(parent_path)
        # Limita para não estourar a API em testes iniciais
        parent_ids = [r["id"] for r in parent_df.select("id").distinct().limit(ProjectConfig.MAX_PARENT_IDS).collect()]
    except Exception as e:
        print(f"Erro ao ler pai {parent_dataset}: {e}")
        return

    try:
        data = fetch_nested_data(config["endpoint"], parent_ids)
    except Exception as e:
        print(f"Erro ao coletar dados dependentes para {dataset_name}: {e}")
        raise e

    if not data:
        print(f"Nenhum registro retornado para {dataset_name}")
        return

    # Cria DataFrame com schema flexível
    df_raw = spark.createDataFrame(data)
    
    expected_cols = config["schema"].fieldNames()
    for col_name in expected_cols:
        if col_name not in df_raw.columns and col_name != "data_ingestao":
            df_raw = df_raw.withColumn(col_name, lit(None))
            
    df = df_raw.select(*[c for c in expected_cols if c != "data_ingestao"])
    df = df.withColumn("data_ingestao", current_timestamp())


    write_bronze(df, dataset_name, config)
    log_pipeline_event(dataset_name, "SUCCESS", records=len(data))

# COMMAND ----------

# DBTITLE 1,Runner
def run_pipeline(dataset):
    datasets = {dataset: CAMARA_DATASETS[dataset]} if dataset != "all" else CAMARA_DATASETS
    for name, cfg in datasets.items():
        try:
            if "requires_parent" in cfg: ingest_nested_dataset(name, cfg)
            else: ingest_simple_dataset(name, cfg)
        except Exception as e:
            log_pipeline_event(name, "ERROR", message=str(e))
            print(f"Erro em {name}: {e}")


# COMMAND ----------

run_pipeline(dataset_param)
