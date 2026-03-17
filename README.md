# Pipeline de Dados Legislativos da Câmara dos Deputados

## Visão Geral do Projeto

Este projeto implementa um pipeline de dados robusto e escalável para coletar, processar e analisar informações da Câmara dos Deputados do Brasil. Utilizando a arquitetura Medallion, o pipeline transforma dados brutos da API oficial em conjuntos de dados limpos, estruturados e prontos para análise, permitindo insights sobre a atividade legislativa, o desempenho dos parlamentares e as tendências políticas.

## Objetivo

O principal objetivo é democratizar o acesso aos dados legislativos, fornecendo uma base sólida para pesquisadores, jornalistas, cidadãos e analistas políticos. Através da ingestão contínua e do processamento inteligente, o projeto visa facilitar a compreensão do complexo cenário político brasileiro.

## Arquitetura do Pipeline: Medallion Architecture

O pipeline segue a **Arquitetura Medallion**, um padrão de design de dados que organiza os dados em três camadas principais: Bronze, Silver e Gold. Essa abordagem garante a qualidade, a confiabilidade e a usabilidade dos dados em cada estágio do processamento.

### 1. Camada Bronze (Raw Data)

*   **Propósito:** Armazenar os dados brutos, exatamente como são recebidos da fonte, sem qualquer transformação. Serve como um histórico imutável dos dados originais.
*   **Conteúdo:** Dados diretamente da API da Câmara dos Deputados (deputados, proposições, votações, eventos, etc.).
*   **Formato:** Delta Lake.

### 2. Camada Silver (Cleaned & Conformed Data)

*   **Propósito:** Limpar, padronizar e enriquecer os dados da camada Bronze. Aplica transformações básicas, como correção de tipos de dados, tratamento de valores nulos e desduplicação.
*   **Conteúdo:** Dados limpos e estruturados, prontos para análises mais aprofundadas. Inclui a classificação temática de proposições.
*   **Formato:** Delta Lake.

### 3. Camada Gold (Curated & Business-Ready Data)

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

### `treatment/silver_runner_camara.py`

*   **Problema de Mapeamento:** Erro na coluna utilizada para a data de apresentação das proposições. A coluna `dataApresentacao` estava sendo referenciada, mas a API da Câmara retorna `dataApresentacaoInicio`.
*   **Problema de Tipagem (Votações/Votos):** O campo `id_votacao` estava sendo convertido para `long`, porém a API da Câmara utiliza um formato alfanumérico (ex: `2351239-4`), causando erro de cast (`CAST_INVALID_INPUT`).
*   **Problema de Formatação de Data:** Formatos fixos em `to_date` e `to_timestamp` causavam falhas ao encontrar variações nos dados da API (presença ou ausência de segundos/milissegundos).
*   **Correções:**
    *   Alterada a referência da coluna de `dataApresentacao` para `dataApresentacaoInicio`.
    *   Alterado o tipo de `id_votacao` de `long` para `string` em todas as tabelas relacionadas (`votacoes`, `votos` e esquemas Bronze).
    *   Removidos formatos fixos de data/timestamp para permitir o parsing automático e resiliente do Spark.

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
