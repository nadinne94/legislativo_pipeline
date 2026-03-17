# Databricks notebook source
# MAGIC %md
# MAGIC # PROCESSAMENTO DA CAMADA GOLD - INDICADORES DE ATUAÇÃO PARLAMENTAR
# MAGIC Este notebook consolida os indicadores finais (KPIs) de atuação dos deputados, utilizando os dados limpos e classificados da camada Silver.

# COMMAND ----------

# DBTITLE 1,Carregar Configurações Globais
# MAGIC %run ../config/project_config

# COMMAND ----------

# DBTITLE 1,Importação de Dependências Spark SQL
from pyspark.sql.functions import col, count, desc, current_timestamp, sum, lit, round
from delta.tables import DeltaTable

# COMMAND ----------

# DBTITLE 1,Função de Escrita na Gold (Overwrite para Dashboards)
def write_gold(df, table_name):
    """
    Grava os dados na camada Gold utilizando Overwrite.
    Como a camada Gold é destinada a consumo por BI/Dashboards, o overwrite garante 
    que os indicadores reflitam sempre o estado atualizado da Silver.
    """
    path = f"{ProjectConfig.GOLD_PATH}/{table_name}"
    (df.write.format("delta")
     .mode("overwrite")
     .option("overwriteSchema", "true")
     .save(path))
    print(f"Indicador Gold {table_name} gerado com sucesso.")

# COMMAND ----------

# DBTITLE 1,Indicador: Ranking de Faltas (Absenteísmo)
try:
    print("Gerando Indicador: Ranking de Faltas...")
    df_eventos = spark.read.format("delta").load(f"{ProjectConfig.SILVER_PATH}/eventos")
    df_presencas = spark.read.format("delta").load(f"{ProjectConfig.SILVER_PATH}/presencas_eventos")
    df_deputados = spark.read.format("delta").load(f"{ProjectConfig.SILVER_PATH}/deputados")

    # Total de eventos registrados no período
    total_eventos = df_eventos.count()

    ranking_faltas = (df_presencas
                      .groupBy("id_deputado", "nome_deputado")
                      .agg(count("id_evento").alias("presencas_confirmadas"))
                      .withColumn("total_eventos_periodo", lit(total_eventos))
                      .withColumn("faltas_estimadas", col("total_eventos_periodo") - col("presencas_confirmadas"))
                      .withColumn("percentual_presenca", round((col("presencas_confirmadas") / col("total_eventos_periodo")) * 100, 2))
                      .join(df_deputados.select("id_deputado", "sigla_partido", "uf_origem"), "id_deputado", "left")
                      .orderBy(desc("faltas_estimadas")))

    write_gold(ranking_faltas, "indicador_faltas_deputados")
except Exception as e:
    print(f"Aviso: Falha ao gerar indicador de faltas: {e}")

# COMMAND ----------

# DBTITLE 1,Indicador: Produção Legislativa por Tema (Taxonomia)
try:
    print("Gerando Indicador: Produção Legislativa por Tema...")
    df_proposicoes = spark.read.format("delta").load(f"{ProjectConfig.SILVER_PATH}/proposicoes")
    
    # Agregação por Tema (definido na Silver) e Ano
    indicador_temas = (df_proposicoes
                       .groupBy("tema_principal", "ano")
                       .agg(count("id_proposicao").alias("total_proposicoes"))
                       .orderBy(desc("total_proposicoes")))

    write_gold(indicador_temas, "indicador_temas_legislativos")
except Exception as e:
    print(f"Aviso: Falha ao gerar indicador de temas: {e}")

# COMMAND ----------

# DBTITLE 1,Indicador: Autoria de Projetos por Deputado
try:
    print("Gerando Indicador: Autoria por Deputado...")
    df_autores = spark.read.format("delta").load(f"{ProjectConfig.SILVER_PATH}/proposicoes_autores")
    df_proposicoes = spark.read.format("delta").load(f"{ProjectConfig.SILVER_PATH}/proposicoes")
    df_deputados = spark.read.format("delta").load(f"{ProjectConfig.SILVER_PATH}/deputados")

    # Cruzamento para obter temas por autor
    producao_autor = (df_autores
                      .join(df_proposicoes.select("id_proposicao", "tema_principal"), "id_proposicao")
                      .groupBy("id_autor", "nome_autor", "tema_principal")
                      .agg(count("id_proposicao").alias("total_projetos"))
                      .join(df_deputados.select(col("id_deputado").alias("id_autor"), "sigla_partido", "uf_origem"), "id_autor", "left")
                      .orderBy(desc("total_projetos")))

    write_gold(producao_autor, "indicador_producao_parlamentar")
except Exception as e:
    print(f"Aviso: Falha ao gerar indicador de autoria: {e}")
