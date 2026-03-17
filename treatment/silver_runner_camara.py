# Databricks notebook source
# MAGIC %md
# MAGIC # PROCESSAMENTO DA CAMADA SILVER - CÂMARA DOS DEPUTADOS
# MAGIC Este notebook realiza a limpeza, padronização, tipagem e classificação temática dos dados brutos da camada Bronze.

# COMMAND ----------

# DBTITLE 1,Carregar Configurações Globais
# MAGIC %run ../config/project_config

# COMMAND ----------

# DBTITLE 1,Importação de Dependências Spark SQL
from pyspark.sql.functions import col, to_date, to_timestamp, current_timestamp, when, lit, udf, element_at, split
from pyspark.sql.types import StringType
from delta.tables import DeltaTable

# COMMAND ----------

# DBTITLE 1,Função Genérica de Escrita na Silver (MERGE)
def write_silver(df, table_name, merge_keys):
    """
    Grava os dados na camada Silver utilizando MERGE para evitar duplicidade.
    Garante que os dados limpos sejam integrados corretamente à camada intermediária.
    """
    path = f"{ProjectConfig.SILVER_PATH}/camara/{table_name}"
    
    if not DeltaTable.isDeltaTable(spark, path):
        df.write.format("delta").mode("overwrite").save(path)
        print(f"Tabela Silver {table_name} criada com sucesso.")
    else:
        dt = DeltaTable.forPath(spark, path)
        condition = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])
        
        # Mapeamento de colunas para o merge (Update/Insert)
        update_map = {c: f"source.{c}" for c in df.columns}
        
        (dt.alias("target")
         .merge(df.alias("source"), condition)
         .whenMatchedUpdate(set=update_map)
         .whenNotMatchedInsert(values=update_map)
         .execute())
        print(f"Tabela Silver {table_name} atualizada via MERGE.")

# COMMAND ----------

# DBTITLE 1,Definição da Taxonomia Legislativa
# Dicionário que mapeia palavras-chave para temas principais da atuação parlamentar.
# Esta taxonomia permite agrupar milhares de projetos em categorias de fácil análise.
LEGISLATIVE_TAXONOMY = {
    "SAÚDE": ["saúde", "hospital", "doença", "médico", "sus", "farmácia", "medicamento", "vacina", "sanitário"],
    "EDUCAÇÃO": ["educação", "escola", "universidade", "ensino", "aluno", "professor", "bolsa", "pesquisa", "alfabetização"],
    "ECONOMIA E TRIBUTOS": ["economia", "imposto", "tributo", "mercado", "comércio", "inflação", "emprego", "salário", "orçamento", "fiscal"],
    "SEGURANÇA E JUSTIÇA": ["segurança", "polícia", "crime", "violência", "prisão", "armas", "justiça", "penal", "judicial"],
    "MEIO AMBIENTE": ["ambiente", "floresta", "água", "poluição", "clima", "sustentabilidade", "desmatamento", "ecossistema"],
    "DIREITOS E CIDADANIA": ["direitos", "minorias", "discriminação", "igualdade", "liberdade", "cidadania", "mulher", "idoso", "criança"],
    "INFRAESTRUTURA": ["infraestrutura", "transporte", "rodovia", "ferrovia", "porto", "aeroporto", "energia", "saneamento", "obras"],
    "AGRO E RURAL": ["agricultura", "rural", "agronegócio", "safra", "pecuária", "terra", "produtor", "fazenda"],
    "CULTURA E ESPORTE": ["cultura", "arte", "esporte", "lazer", "patrimônio", "turismo", "cinema"],
    "TECNOLOGIA E CIÊNCIA": ["tecnologia", "inovação", "digital", "internet", "dados", "ciência", "inteligência artificial"]
}

# COMMAND ----------

# DBTITLE 1,Função UDF para Classificação Temática
def classify_theme(ementa):
    """
    Analisa o texto da ementa e classifica no primeiro tema da taxonomia que possuir uma palavra-chave correspondente.
    Retorna 'OUTROS' caso não identifique palavras-chave conhecidas.
    """
    if not ementa:
        return "OUTROS"
    
    ementa_lower = ementa.lower()
    for theme, keywords in LEGISLATIVE_TAXONOMY.items():
        for keyword in keywords:
            if keyword in ementa_lower:
                return theme
    return "OUTROS"

# Registro da UDF no Spark
classify_theme_udf = udf(classify_theme, StringType())

# COMMAND ----------

# DBTITLE 1,Processamento: Deputados
print("Limpando dados de Deputados...")
df_deputados = spark.read.format("delta").load(f"{ProjectConfig.BRONZE_PATH}/camara/deputados")

df_deputados_silver = df_deputados.select(
    col("id").cast("long").alias("id_deputado"),
    col("nome").alias("nome_completo"),
    col("siglaPartido").alias("sigla_partido"),
    col("siglaUf").alias("uf_origem"),
    col("email"),
    col("urlFoto").alias("url_foto"),
    current_timestamp().alias("data_processamento")
)


write_silver(df_deputados_silver, "deputados", ["id_deputado"])

# COMMAND ----------

df_deputados.limit(10).display()

# COMMAND ----------

df_deputados_silver.limit(10).display()

# COMMAND ----------

# DBTITLE 1,Processamento: Proposições (Com Classificação Temática)
try:
    print("Processando e Classificando Proposições...")
    df_prop = spark.read.format("delta").load(f"{ProjectConfig.BRONZE_PATH}/camara/proposicoes")

    df_prop_silver = df_prop.select(
        col("id").cast("long").alias("id_proposicao"),
        col("siglaTipo").alias("tipo_proposicao"),
        col("numero").cast("int"),
        col("ano").cast("int"),
        col("ementa"),
        to_date(col("dataApresentacaoInicio")).alias("data_apresentacao"),
        classify_theme_udf(col("ementa")).alias("tema_principal"), # Aplicação da Taxonomia
        current_timestamp().alias("data_processamento")
    )

    write_silver(df_prop_silver, "proposicoes", ["id_proposicao"])
except Exception as e:
    print(f"Aviso: Falha ao processar Proposições: {e}")

# COMMAND ----------

df_prop.limit(10).display()

# COMMAND ----------

df_prop_silver.limit(10).display()

# COMMAND ----------

# DBTITLE 1,Processamento: Autores de Proposições
try:
    print("Processando Autores de Proposições...")
    df_autores = spark.read.format("delta").load(f"{ProjectConfig.BRONZE_PATH}/camara/proposicoes_autores")

    from pyspark.sql.functions import element_at, split
    
    df_autores_silver = df_autores.select(
        col("parent_id").cast("long").alias("id_proposicao"),
        element_at(split(col("uri"), "/"), -1).cast("long").alias("id_autor"),
        col("nome").alias("nome_autor"),
        col("tipo").alias("tipo_autor"),
        current_timestamp().alias("data_processamento")
    )
    write_silver(df_autores_silver, "proposicoes_autores", ["id_proposicao", "nome_autor"])

<<<<<<< Updated upstream
    write_silver(df_autores_silver, "proposicoes_autores", ["id_proposicao", "nome_autor"])
=======
>>>>>>> Stashed changes
except Exception as e:
    print(f"Aviso: Falha ao processar Autores: {e}")

# COMMAND ----------

df_autores.limit(10).display()

# COMMAND ----------

df_autores_silver.limit(10).display()

# COMMAND ----------

# DBTITLE 1,Processamento: Eventos e Presenças
try:
    print("Processando Eventos e Presenças...")
    df_eventos = spark.read.format("delta").load(f"{ProjectConfig.BRONZE_PATH}/camara/eventos")
    df_presencas = spark.read.format("delta").load(f"{ProjectConfig.BRONZE_PATH}/camara/presencas_eventos")

    # Silver Eventos
    df_eventos_silver = df_eventos.select(
        col("id").cast("long").alias("id_evento"),
        to_timestamp(col("dataHoraInicio")).alias("data_hora_inicio"),
        col("descricao").alias("descricao_evento"),
        current_timestamp().alias("data_processamento")
    )
    write_silver(df_eventos_silver, "eventos", ["id_evento"])

    # Silver Presenças
    df_presencas_silver = df_presencas.select(
        col("idEvento").alias("id_evento").cast("long"),
        col("idDeputado").alias("id_deputado").cast("long"),
        col("nomeDeputado").alias("nome_deputado"),
        current_timestamp().alias("data_processamento")
    )
    write_silver(df_presencas_silver, "presencas_eventos", ["id_evento", "id_deputado"])
except Exception as e:
    print(f"Aviso: Falha ao processar Eventos/Presenças: {e}")

# COMMAND ----------

df_eventos.limit(10).display()

# COMMAND ----------

df_eventos_silver.limit(10).display()

# COMMAND ----------

df_presencas.limit(10).display()

# COMMAND ----------

df_presencas_silver.limit(10).display()

# COMMAND ----------

# DBTITLE 1,Processamento: Votações e Votos
try:
    print("Processando Votações e Votos...")
    df_votacoes = spark.read.format("delta").load(f"{ProjectConfig.BRONZE_PATH}/camara/votacoes")
    df_votos = spark.read.format("delta").load(f"{ProjectConfig.BRONZE_PATH}/camara/votos")

    # Silver Votações
    df_votacoes_silver = df_votacoes.select(
        col("id").cast("string").alias("id_votacao"),
<<<<<<< Updated upstream
        to_date(col("data"), "yyyy-MM-dd").alias("data_votacao"),
=======
        to_date(col("data")).alias("data_votacao"),
>>>>>>> Stashed changes
        col("descricao").alias("descricao_votacao"),
        current_timestamp().alias("data_processamento")
    )
    write_silver(df_votacoes_silver, "votacoes", ["id_votacao"])

    # Silver Votos
    df_votos_silver = df_votos.select(
        col("idVotacao").cast("string").alias("id_votacao"),
        col("idDeputado").cast("long").alias("id_deputado"),
        col("nomeDeputado").alias("nome_deputado"),
        col("voto"),
        current_timestamp().alias("data_processamento")
    )
    write_silver(df_votos_silver, "votos", ["id_votacao", "id_deputado"])
except Exception as e:
    print(f"Aviso: Falha ao processar Votações/Votos: {e}")

# COMMAND ----------

df_votacoes.limit(10).display()

# COMMAND ----------

df_votacoes_silver.limit(10).display()

# COMMAND ----------

df_votos.limit(10).display()

# COMMAND ----------

df_votos_silver.limit(10).display()
