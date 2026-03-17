# Databricks notebook source
# MAGIC %md
# MAGIC # UTILITÁRIOS DE COMUNICAÇÃO COM API
# MAGIC Este notebook contém funções especializadas para interagir com as APIs legislativas, tratando retries e paginação de forma automática.

# COMMAND ----------

# DBTITLE 1,Carregar Configurações do Projeto
# MAGIC %run ../config/project_config

# COMMAND ----------

# DBTITLE 1,Importação de Bibliotecas Python
# Bibliotecas padrão para requisições HTTP e controle de tempo
import requests
import time

# COMMAND ----------

# DBTITLE 1,Construção de URL Completa
def build_endpoint_url(endpoint):
    """Constrói a URL completa a partir do endpoint fornecido."""
    return f"{ProjectConfig.API_BASE_URL}{endpoint}"

# COMMAND ----------

# DBTITLE 1,Requisição API com Lógica de Retry
def request_api(url, params=None, max_retries=3):
    """
    Realiza uma requisição GET à API, tratando erros temporários com tentativas automáticas (retries).
    Evita que o pipeline pare por oscilações momentâneas do servidor da Câmara.
    """
    retries = 0
    while retries < max_retries:
        try:
            response = requests.get(
                url,
                params=params,
                timeout=ProjectConfig.REQUEST_TIMEOUT
            )
            
            # Sucesso na requisição
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Erro HTTP {response.status_code} na URL: {url}")
                
        except Exception as e:
            print(f"Erro inesperado na requisição: {e}")

        retries += 1
        if retries < max_retries:
            print(f"Tentativa {retries}/{max_retries} falhou. Tentando novamente em 2s...")
            time.sleep(2)

    raise Exception(f"Falha na API após {max_retries} tentativas: {url}")

# COMMAND ----------

# DBTITLE 1,Navegação entre Páginas (Links de Paginação)
def get_next_page_link(response_json):
    """Extrai o link para a próxima página de resultados conforme o padrão da API da Câmara."""
    if "links" not in response_json:
        return None

    for link in response_json["links"]:
        if link["rel"] == "next":
            return link["href"]
    return None

# COMMAND ----------

# DBTITLE 1,Coleta de Todas as Páginas de um Endpoint
def get_all_pages(endpoint, params=None):
    """
    Função principal que percorre todas as páginas de um endpoint até coletar todos os dados disponíveis.
    Útil para tabelas como Deputados, Proposições e Votações.
    """
    url = build_endpoint_url(endpoint)
    results = []
    page_count = 0

    # usa valor padrão do projeto caso não seja informado
    max_pages = ProjectConfig.MAX_PAGES_PER_EXECUTION

    while url:
        page_count += 1

        print(f"Requisitando página: {url}")
        response_json = request_api(url, params)
        
        # Extrai os dados da resposta (campo 'dados')
        dados = response_json.get("dados", [])
        results.extend(dados)

        # verifica limite máximo de páginas
        if page_count >= max_pages:

            print(
                f"Limite de páginas atingido ({max_pages}). "
                f"Interrompendo coleta para evitar sobrecarga da API."
            )

            break

        # Verifica se existe próxima página
        next_page = get_next_page_link(response_json)
        url = next_page
        
        # Parâmetros iniciais (como dataInicio) são necessários apenas na primeira chamada
        params = None
        
        # Pequeno delay para respeitar limites de taxa da API
        time.sleep(0.2)

    print(f"Coleta concluída. Total de registros: {len(results)}")
    return results

# COMMAND ----------

# DBTITLE 1,Formatação de Endpoints com Parâmetros Dinâmicos
def format_endpoint(endpoint, resource_id):
    """Substitui marcadores de ID na URL (ex: {id}) pelo valor real do recurso."""
    return endpoint.replace("{id}", str(resource_id))

# COMMAND ----------

# DBTITLE 1,Coleta de Dados de Endpoints Dependentes (Nested)
def get_nested_endpoint_data(endpoint_template, parent_ids):
    """
    Função para buscar dados que dependem de um ID pai (ex: autores de uma proposição).
    Adiciona o 'parent_id' aos resultados para manter a relação entre as tabelas.
    """
    all_results = []
    for resource_id in parent_ids:
        endpoint = format_endpoint(endpoint_template, resource_id)
        try:
            data = get_all_pages(endpoint)
            for row in data:
                row["parent_id"] = resource_id
            all_results.extend(data)
        except Exception as e:
            print(f"Aviso: Erro ao coletar dados para o recurso {resource_id}: {e}")
    return all_results
