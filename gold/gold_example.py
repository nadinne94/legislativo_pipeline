# Databricks notebook source
# MAGIC %md
# MAGIC # PROCESSAMENTO DA CAMADA GOLD (EXEMPLO)
# MAGIC Este notebook demonstra como realizar agregações de negócio a partir da camada Silver para a camada Gold, otimizando os dados para dashboards e relatórios finais.

# COMMAND ----------

# DBTITLE 1,Carregar Configurações Globais
# MAGIC %run ../config/project_config

# COMMAND ----------

# DBTITLE 1,Importação de Funções de Agregação Spark SQL
# Importação de funções necessárias para agrupamento e contagem de registros
from pyspark.sql.functions import col, count, countDistinct, current_timestamp

# COMMAND ----------

# DBTITLE 1,Leitura de Dados da Camada Silver
# Carrega a tabela limpa de deputados da camada Silver
try:
    silver_path = f"{ProjectConfig.SILVER_PATH}/deputados"
    silver_df = spark.read.format("delta").load(silver_path)
    print("Dados da Silver carregados com sucesso.")
    silver_df.printSchema()
except Exception as e:
    print(f"Erro ao carregar dados da Silver em {silver_path}: {e}")
    dbutils.notebook.exit("Falha na leitura da Silver.")

# COMMAND ----------

# DBTITLE 1,Transformações e Agregações de Negócio (Gold)
"""
Na camada Gold, realizamos:
1. Agrupamento por dimensões de interesse (ex: sigla_partido, uf_origem).
2. Cálculo de métricas (ex: total de deputados distintos).
3. Adição de metadados de processamento final.
"""
gold_df = (
    silver_df.groupBy("sigla_partido", "uf_origem")
    .agg(
        countDistinct("id_deputado").alias("total_deputados"),
        count("id_deputado").alias("total_registros_historicos")
    )
    .withColumn("data_processamento_gold", current_timestamp())
)

print("Schema da camada Gold (Agregação):")
gold_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Gravação na Camada Gold (S3)
# Define o nome da tabela de resumo para consumo de BI
table_name = "deputados_resumo_partido_uf"
path = f"{ProjectConfig.GOLD_PATH}/{table_name}"

# Grava os dados agregados em formato Delta, prontos para dashboards
(
    gold_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(path)
)

print(f"Tabela Gold '{table_name}' gravada com sucesso em: {path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## PRÓXIMOS PASSOS SUGERIDOS:
# MAGIC *   Criar tabelas Gold para responder às perguntas de negócio específicas (ex: ranking de faltas, projetos aprovados).
# MAGIC *   Conectar as tabelas Gold a ferramentas de visualização como Databricks SQL Dashboards, Power BI ou Tableau.
# MAGIC *   Automatizar a execução periódica deste notebook via Databricks Workflows.
