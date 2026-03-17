# Databricks notebook source
# MAGIC %md
# MAGIC # CONFIGURAÇÃO DAS FONTES DE DADOS
# MAGIC Define as fontes externas de dados (Câmara e futuramente Senado) e suas especificidades de API.

# COMMAND ----------

# DBTITLE 1,Dicionário de Fontes de Dados
# Armazena as configurações de cada fonte, facilitando a expansão do projeto para o Senado.
BRONZE_SOURCES = {

    "camara": {
        "name": "Câmara dos Deputados",
        "api_base_url": "https://dadosabertos.camara.leg.br/api/v2",
        "format": "json",
        "pagination": "links"
    },

    "senado": {
        "name": "Senado Federal",
        "api_base_url": "https://legis.senado.leg.br/dadosabertos",
        "format": "xml",
        "pagination": None
    }
}

# COMMAND ----------

# DBTITLE 1,Funções de Recuperação de Configuração
def get_source_config(source_name):
    """Retorna as configurações de uma fonte específica."""
    if source_name not in BRONZE_SOURCES:
        raise ValueError(f"Fonte '{source_name}' não encontrada no catálogo.")
    return BRONZE_SOURCES[source_name]

# COMMAND ----------

# DBTITLE 1,Listagem de Fontes Disponíveis
def list_sources():
    """Exibe no console as fontes de dados configuradas no sistema."""
    print("\nFontes de dados configuradas para o pipeline:\n")
    for source, config in BRONZE_SOURCES.items():
        print(f"Fonte: {source.upper()}")
        print(f"Nome: {config['name']}")
        print(f"API Base: {config['api_base_url']}")
        print(f"Formato: {config['format']}")
        print("-" * 40)

# COMMAND ----------

# DBTITLE 1,Execução do Teste de Listagem
# Exibe a lista ao carregar o notebook para validação visual
list_sources()
