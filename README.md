# Pipeline de Dados Legislativos — Câmara dos Deputados

## Visão Geral

Este projeto implementa um **pipeline de engenharia de dados em arquitetura Lakehouse** para ingestão e organização de dados legislativos disponibilizados pela API pública da Câmara dos Deputados.

O pipeline realiza:

* ingestão automatizada da API
* tratamento de paginação e retries
* controle de ingestão incremental
* persistência em **Delta Lake**
* organização em camadas **Bronze → Silver → Gold**

O objetivo é disponibilizar dados legislativos estruturados para **análise política, ciência de dados e construção de dashboards analíticos**.

## Arquitetura

Arquitetura baseada no padrão **Medallion Architecture (Lakehouse)**.

```
API Câmara
     │
     ▼
Bronze (Raw API ingestion)
     │
     ▼
Silver (Data cleansing)
     │
     ▼
Gold (Analytics / BI)
```

Camadas:

| Camada | Objetivo               |
| ------ | ---------------------- |
| Bronze | Ingestão bruta da API  |
| Silver | Limpeza e normalização |
| Gold   | Modelos analíticos     |

## Estrutura do Projeto

```
legislativo_pipeline_v3_DEFINITIVE_FIXED

config/
    project_config.py
    bronze_sources.py
    camara_datasets.py

utils/
    api_utils.py
    logging_utils.py
    incremental_utils.py

ingestion/
    bronze_runner_camara.py

silver/
    silver_runner_camara.py

gold/
    gold_runner_camara.py
```

## 1. Configuração do Projeto

Arquivo:

```
config/project_config.py
```

Centraliza configurações globais do pipeline.

Principais parâmetros:

| Parâmetro               | Função                           |
| ----------------------- | -------------------------------- |
| API_BASE_URL            | Endpoint base da API legislativa |
| REQUEST_TIMEOUT         | Timeout das requisições          |
| API_REQUEST_DELAY       | Delay entre chamadas             |
| MAX_PAGES_PER_EXECUTION | Controle de paginação            |

Exemplo conceitual:

```python
class ProjectConfig:

    API_BASE_URL = "https://dadosabertos.camara.leg.br/api/v2"

    REQUEST_TIMEOUT = 30

    API_REQUEST_DELAY = 0.2

    MAX_PAGES_PER_EXECUTION = 120
```

Esse design permite **configuração centralizada e reutilizável**.

## 2. Definição das Fontes Bronze

Arquivo:

```
config/bronze_sources.py
```

Define **quais recursos da API serão ingeridos**.

Exemplos:

* deputados
* partidos
* proposições
* votações

Cada fonte possui:

| Campo              | Descrição                              |
| ------------------ | -------------------------------------- |
| endpoint           | endpoint da API                        |
| table_name         | tabela Delta                           |
| incremental_column | coluna usada para ingestão incremental |

Exemplo conceitual:

```python
{
    "name": "proposicoes",
    "endpoint": "/proposicoes",
    "incremental_column": "dataApresentacao"
}
```

Isso permite adicionar novas fontes **sem alterar o pipeline principal**.

## 3. Catálogo de Datasets

Arquivo:

```
config/camara_datasets.py
```

Responsável por estruturar metadados dos datasets.

Define:

* nomes das tabelas
* endpoints relacionados
* dependências entre datasets

Isso permite gerenciar **datasets complexos com relações hierárquicas**.

Exemplo:

```
proposicoes
   ├── autores
   ├── temas
   └── votacoes
```

## 4. Utilitários de API

Arquivo:

```
utils/api_utils.py
```

Responsável pela comunicação com a API.

Principais funções:

| Função                   | Objetivo                     |
| ------------------------ | ---------------------------- |
| build_endpoint_url       | montar URL                   |
| request_api              | requisição com retry         |
| get_next_page_link       | paginação automática         |
| get_all_pages            | coleta paginada              |
| get_nested_endpoint_data | coleta endpoints dependentes |

### Retry Automático

A função `request_api` implementa **tratamento de falhas temporárias**.

Problemas tratados:

* HTTP 500
* HTTP 504
* timeout

Estratégia:

```
try request
   ↓
fail
   ↓
retry (até 3 tentativas)
```

Isso evita falhas causadas por instabilidade da API.

### Paginação Automática

A API da Câmara usa o padrão:

```
links:
   rel = next
```

A função `get_next_page_link` detecta automaticamente a próxima página.

Fluxo:

```
pagina 1
  ↓
pagina 2
  ↓
pagina 3
  ↓
...
```

### Controle de Paginação

Foi implementado controle para evitar execuções gigantes:

```
MAX_PAGES_PER_EXECUTION
```

Isso evita casos como:

```
/proposicoes
500+ páginas
```

## 5. Logging do Pipeline

Arquivo:

```
utils/logging_utils.py
```

Responsável por registrar execução do pipeline.

Eventos registrados:

* START
* SUCCESS
* ERROR

Exemplo de log:

```
Log gravado: deputados | Status: START
Log gravado: deputados | Status: SUCCESS
```

Esse registro permite:

* auditoria de execução
* rastreabilidade
* monitoramento

## 6. Ingestão Incremental

Arquivo:

```
utils/incremental_utils.py
```

Implementa ingestão incremental baseada em data.

Fluxo:

```
ler última data do Delta
      ↓
usar como dataInicio
      ↓
buscar novos registros
```

Exemplo:

```
dataInicio=2024-01-01
```

Isso evita reprocessamento completo da API.

## 7. Runner de Ingestão Bronze

Arquivo:

```
ingestion/bronze_runner_camara.py
```

É o **orquestrador da ingestão**.

Fluxo principal:

```
para cada fonte configurada:

    registrar START

    coletar dados da API

    converter para dataframe

    salvar no Delta Lake

    registrar SUCCESS
```

### Pipeline Bronze

Fluxo completo:

```
config → sources
      ↓
api_utils → API
      ↓
incremental_utils → controle incremental
      ↓
Spark DataFrame
      ↓
Delta Lake
```

### Persistência em Delta Lake

Os dados são salvos em formato **Delta**.

Vantagens:

| Recurso          | Benefício                |
| ---------------- | ------------------------ |
| ACID             | integridade transacional |
| Time Travel      | histórico de versões     |
| Schema evolution | evolução de schema       |
| Performance      | leitura otimizada        |

---

### Tabelas Bronze

Exemplos de tabelas geradas:

```
bronze.deputados
bronze.partidos
bronze.proposicoes
bronze.votacoes
```

Essas tabelas contêm os **dados brutos da API**.

### Estratégia de Escalabilidade

O pipeline foi projetado para lidar com:

* grandes volumes de páginas
* endpoints com milhares de registros
* instabilidade da API pública

Soluções aplicadas:

| Problema           | Solução                 |
| ------------------ | ----------------------- |
| timeout da API     | retry automático        |
| paginação infinita | limite de páginas       |
| reprocessamento    | ingestão incremental    |
| instabilidade      | delay entre requisições |

---

### Segurança e Boas Práticas

Boas práticas aplicadas:

* separação de camadas
* código modular
* configuração centralizada
* logs de execução
* retry resiliente
* controle de paginação

## Casos de Uso Analítico

Após processamento nas camadas Silver e Gold, os dados permitem análises como:

* produtividade legislativa
* comportamento de votação
* rede de autores de proposições
* análise temporal de leis
* análise partidária


## Tecnologias Utilizadas

| Tecnologia | Uso                  |
| ---------- | -------------------- |
| Python     | ingestão             |
| Spark      | processamento        |
| Delta Lake | armazenamento        |
| REST API   | fonte de dados       |
| Databricks | execução do pipeline |

