# Pipeline de Dados Legislativos da Câmara dos Deputados

## Visão Geral do Projeto

Este projeto implementa um pipeline de dados robusto e escalável para coletar, processar e analisar informações da Câmara dos Deputados do Brasil. Utilizando a arquitetura Medallion, o pipeline transforma dados brutos da API oficial em conjuntos de dados limpos, estruturados e prontos para análise, permitindo insights sobre a atividade legislativa, o desempenho dos parlamentares e as tendências políticas.

## Objetivo

O principal objetivo é democratizar o acesso aos dados legislativos, fornecendo uma base sólida para pesquisadores, jornalistas, cidadãos e analistas políticos. Através da ingestão contínua e do processamento inteligente, o projeto visa facilitar a compreensão do complexo cenário político brasileiro.

## Estrutura do Projeto

Disposição dos diretórios:

```
legislativo_pipeline_v3_DEFINITIVE_FIXED
│
├── config
│   ├── project_config.py
│   ├── bronze_sources.py
│   └── camara_datasets.py
│
├── utils
│   ├── api_utils.py
│   ├── logging_utils.py
│   └── incremental_utils.py
│
├── ingestion
│   └── bronze_runner_camara.py
│
├── silver
│   └── silver_runner_camara.py
└── gold
    └── gold_runner_camara.py
```

## Arquitetura do Pipeline: Medallion Architecture

O pipeline segue a **Arquitetura Medallion**, um padrão de design de dados que organiza os dados em três camadas principais: Bronze, Silver e Gold. Essa abordagem garante a qualidade, a confiabilidade e a usabilidade dos dados em cada estágio do processamento.

```
API Câmara dos Deputados
          │
          ▼
      Bronze Layer
   (Dados brutos da API)
          │
          ▼
      Silver Layer
 (Dados limpos e estruturados)
          │
          ▼
      Gold Layer
(Modelos analíticos prontos)
```
## Desenvolvimento do Projeto

### 1. Configurações do Projeto
### 2. Utilitários
### 3. Camada Bronze (Raw Data)

*   **Propósito:** Armazenar os dados brutos, exatamente como são recebidos da fonte, sem qualquer transformação. Serve como um histórico imutável dos dados originais.
*   **Conteúdo:** Dados diretamente da API da Câmara dos Deputados (deputados, proposições, votações, eventos, etc.).
*   **Formato:** Delta Lake.

### 4. Camada Silver (Cleaned & Conformed Data)

*   **Propósito:** Limpar, padronizar e enriquecer os dados da camada Bronze. Aplica transformações básicas, como correção de tipos de dados, tratamento de valores nulos e desduplicação.
*   **Conteúdo:** Dados limpos e estruturados, prontos para análises mais aprofundadas. Inclui a classificação temática de proposições.
*   **Formato:** Delta Lake.

### 5. Camada Gold (Curated & Business-Ready Data)

*   **Propósito:** Fornecer dados agregados e otimizados para casos de uso específicos de negócios e análises. Esta camada é projetada para consumo direto por ferramentas de BI, modelos de Machine Learning e aplicações.
*   **Conteúdo:** Tabelas sumarizadas, dimensões e fatos que respondem a perguntas de negócios específicas (ex: ranking de deputados por presença, proposições por tema).
*   **Formato:** Delta Lake.

## Fontes de Dados

Os dados são coletados diretamente da **API de Dados Abertos da Câmara dos Deputados** [1], que oferece acesso programático a uma vasta gama de informações legislativas.

## Tecnologias Utilizadas

*   **Databricks:** Plataforma unificada para engenharia de dados, Machine Learning e BI.
*   **Apache Spark:** Motor de processamento de dados distribuído para lidar com grandes volumes de dados.
*   **Delta Lake:** Camada de armazenamento de dados que oferece transações ACID, versionamento e metadados para o data lake.
*   **Python:** Linguagem de programação principal para o desenvolvimento do pipeline.
*   **Requests:** Biblioteca HTTP para interagir com a API da Câmara.
*   **ThreadPoolExecutor:** Para paralelização de chamadas à API.

## Estrutura do Repositório

O repositório está organizado nas seguintes pastas:

*   `config/`: Contém arquivos de configuração global do projeto, como URLs de API, caminhos de armazenamento e esquemas de dados.
*   `utils/`: Módulos utilitários com funções auxiliares para interação com a API, logging e lógica incremental.
*   `ingestion/`: Notebooks responsáveis pela ingestão de dados brutos da API para a camada Bronze (`bronze_runner_camara.py`).
*   `treatment/`: Notebooks para o processamento e limpeza dos dados da camada Bronze para a camada Silver (`silver_runner_camara.py`).
*   `analytics/`: Notebooks para a criação de tabelas agregadas e análises na camada Gold.

## Correções Implementadas

Durante a revisão do pipeline, foram identificados e corrigidos os seguintes pontos:

### `ingestion/bronze_runner_camara.py`

*   **Problema:** A delta table `votos` não estava sendo criada corretamente devido a uma lógica falha na extração de `parent_ids` para datasets aninhados e uma verificação duplicada de `if not data: return` que poderia impedir a criação da tabela em caso de dados vazios na primeira execução.
*   **Correção:** Ajustada a lógica para garantir que `parent_ids` sejam corretamente extraídos e que a verificação de dados vazios seja feita de forma única e eficaz, permitindo a criação da tabela `votos` mesmo que a primeira chamada retorne vazio, mas garantindo que o processo não continue sem dados válidos.

### Camadas Bronze e Silver (Mapeamento e Integridade)

*   **Problema de Campos Nulos (Deputados/Autores):** Diversos campos importantes como `nome_completo`, `uf_origem`, `id_proposicao` e `id_autor` estavam resultando em `null`.
    *   **Causa:** Inconsistência entre os nomes dos campos nos schemas (ex: `nomeCivil`, `uf`) e o que a API realmente retorna no endpoint de listagem (`nome`, `siglaUf`). Além disso, o endpoint de autores não fornece o `idAutor` diretamente, apenas na URI.
*   **Problema de Mapeamento (Proposições):** A coluna `dataApresentacao` estava sendo referenciada, mas a API retorna `dataApresentacaoInicio`.
*   **Problema de Tipagem (Votações/Votos):** O campo `id_votacao` causava erro de cast (`CAST_INVALID_INPUT`) ao ser tratado como `long`, sendo que a API utiliza formato alfanumérico.
*   **Correções Implementadas:**
    *   **Resiliência no Bronze:** O `bronze_runner` agora cria DataFrames de forma flexível, garantindo que colunas ausentes na API sejam preenchidas com nulo sem quebrar o pipeline, e mapeando corretamente os campos existentes.
    *   **Correção de Schemas:** Atualizados os nomes dos campos em `camara_datasets.py` para coincidir com o retorno real da API (v2).
    *   **Extração Inteligente:** No `silver_runner`, o `id_autor` agora é extraído dinamicamente da URI do autor via regex/split.
    *   **Tipagem e Datas:** Alterado `id_votacao` para `string` e removidos formatos fixos de data para permitir o parsing automático e resiliente do Spark.

## Como Executar

Para executar este pipeline, é necessário um ambiente Databricks configurado com acesso a um bucket S3. Os notebooks devem ser importados para o workspace do Databricks e executados na seguinte ordem:

1.  `config/project_config.py` (para inicializar as configurações)
2.  `ingestion/bronze_runner_camara.py` (para ingestão dos dados brutos)
3.  `treatment/silver_runner_camara.py` (para limpeza e transformação dos dados)
4.  `analytics/gold_runner_camara.py` (para agregação e análise)

Certifique-se de configurar as credenciais do S3 no ambiente Databricks para que o pipeline possa ler e escrever nos caminhos definidos em `project_config.py`.

## Contato

Para dúvidas ou sugestões, entre em contato com [Seu Nome/Organização].

## Referências

[1] [API de Dados Abertos da Câmara dos Deputados](https://dadosabertos.camara.leg.br/api/v2/)
