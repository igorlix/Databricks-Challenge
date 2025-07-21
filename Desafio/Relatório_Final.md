
# Relatório Final - Desafio Databricks: Engenharia de Dados

Este documento compila as informações essenciais, decisões técnicas, desafios e o funcionamento geral da solução de Engenharia de Dados desenvolvida na plataforma Databricks para o desafio. O objetivo principal foi ingerir um dataset público de incidentes de incêndio de San Francisco, tratá-lo e agregá-lo de forma otimizada, seguindo uma arquitetura de camadas (Bronze, Silver, Gold).

---

## 1. Arquitetura da Solução: Modelo de Camadas

A solução adota uma arquitetura de Medallion Lakehouse, dividida em três camadas principais, otimizando o processamento e a qualidade dos dados para consumo final:

* **Camada Bronze (Raw/Bruta)**: Ingestão inicial dos dados.
* **Camada Silver (Refined/Refinada)**: Limpeza, transformação e padronização dos dados.
* **Camada Gold (Curated/Curada)**: Agregação e otimização dos dados para consumo analítico.

---

## 2. Implementação por Camada

### 2.1. Camada Bronze (Notebook: `1-Camada_Bronze.ipynb`)

Esta etapa foca na ingestão robusta dos dados brutos.

* **Objetivos**:
    * Ingerir dados do CSV em uma tabela Delta, atualizando-a incrementalmente.
    * Estruturar a tabela no formato Delta e registrá-la no Unity Catalog.
* **Decisões Técnicas**:
    * Utilização extensiva do **Apache Spark** para paralelismo e processamento distribuído.
    * Emprego do comando `COPY INTO` para ingestão **incremental**, lendo novos arquivos CSV de um volume (ex: `/Volumes/workspace/default/arquivos_brutos/`) e adicionando apenas os registros ainda não carregados.
    * A tabela Bronze foi criada com todas as colunas como tipo `String`. Essa decisão foi tomada devido a **conflitos de tipo de dado** inferidos automaticamente pelo Databricks ao usar `inferSchema = 'true'` com o CSV e a tabela SQL, optando por tratar os tipos de dados de forma explícita na próxima camada.
* **Dúvidas/Desafios**:
    * A **ingestão diária** do dataset gerou uma dúvida. Como o site do dataset atualiza o CSV diariamente, a solução ideal seria web scraping para baixar os novos dados. No entanto, a **versão gratuita do Databricks restringe o acesso externo à Internet**. Por isso, a ingestão diária foi simulada com a **adição manual** de arquivos CSV no volume, replicando o cenário de novos dados chegando para processamento.

---

### 2.2. Camada Silver (Notebook: `2-Camada_Silver.ipynb`)

Nesta camada, os dados brutos são limpos e transformados.

* **Objetivos**:
    * Ler dados da tabela Delta da Camada Bronze.
    * Converter tipos de dados de `String` para tipos apropriados (`Bigint`, `Double`, `Date`, etc.).
    * Padronizar valores ausentes (ex: "NA", "None", "N None", "null") para `NULL`.
    * Deduplicar registros.
    * Ingerir dados limpos na tabela Delta da Camada Silver de forma incremental, usando `MERGE INTO`.
    * Registrar a tabela Silver no Unity Catalog.
* **Decisões Técnicas**:
    * Aproveitamento do **Spark** para operações em paralelo.
    * A coluna `ID` foi definida como **chave primária** para a deduplicação.
    * Para o tratamento de tipos de dados, foi utilizado **PySpark** com um dicionário de mapeamento de coluna para tipo, além do `TRY_CAST` para lidar com valores que não podem ser convertidos (transformando-os em `NULL` para evitar falhas).
    * A padronização de nulos foi feita identificando diversas variações (`NA`, `None`, etc.) no CSV e convertendo-as para `NULL`. Reconheço que pode haver formas mais otimizadas de detectar todos os padrões de nulos.
    * A deduplicação foi realizada filtrando pela chave primária `ID` e utilizando o comando `MERGE INTO` para inserir novos registros e atualizar os existentes, garantindo a unicidade e a incrementalidade.
    * A tabela Silver é recriada (apenas o schema) antes do `MERGE` para garantir a compatibilidade com o schema do DataFrame transformado, especialmente com nomes de coluna contendo espaços.

---

### 2.3. Camada Gold (Notebook: `3-Camada_Gold.ipynb`)

Esta é a camada de consumo, com dados agregados e otimizados para BI.

* **Objetivos**:
    * Ler dados da tabela Delta da Camada Silver.
    * Criar tabelas agregadas por período (ano/mês), distrito e batalhão.
    * Garantir que as tabelas Gold estejam no formato Delta e gerenciadas pelo Unity Catalog.
* **Decisões Técnicas**:
    * Mantido o foco no **paralelismo do Spark** para operações como `groupBy` (agrupamento) e `agg` (agregação).
    * As agregações são calculadas e armazenadas em **tabelas separadas** (`fire_incidents_gold_by_time`, `fire_incidents_gold_by_district`, `fire_incidents_gold_by_battalion`).
    * A escrita na Camada Gold utiliza o modo `overwrite`, reescrevendo os dados a cada execução com os valores mais recentes das camadas anteriores, garantindo a atualização das agregações.

---

### 2.4. Relatório Exploratório (Notebook: `4-Relatório_Exploratório.ipynb`)

Este notebook demonstra o consumo dos dados da Camada Gold.

* **Objetivos**:
    * Utilizar os dados processados e agregados na Camada Gold para consultas SQL.
    * Fornecer insights sobre os incidentes de incêndio.
* **Consultas Realizadas**:
    * Tendência de incidentes ao longo do tempo (por ano e mês).
    * Top 10 distritos com maior número de incidentes.
    * Análise de perdas estimadas (propriedade e conteúdo) por batalhão.
    * Identificação dos tipos de incidentes mais comuns (consultando a Camada Silver para detalhes).

---

## 3. Orquestração e Simulação Diária (Jobs)

Para simular o processo de ingestão e processamento diário, um **Job do Databricks** foi configurado. Este Job executa os notebooks das camadas Bronze, Silver e Gold em **sequência**, com dependências que garantem que cada etapa só comece após a conclusão bem-sucedida da anterior. Isso estabelece um pipeline de dados automatizado, simulando o comportamento de um processo ETL diário.

---

## 4. Desafios da Versão Gratuita

A utilização da versão gratuita do Databricks trouxe algumas limitações:

* **Acesso Externo à Internet Restrito**: Impossibilitou a implementação de web scraping direto para a ingestão diária automática do dataset. A solução foi a simulação com carga manual em volumes.
* **Limitações de Permissão de Administrador**: Não foi possível salvar as saídas dos notebooks diretamente no GitHub (como `.ipynb` renderizado), necessitando a exportação para HTML para visualização offline.

Apesar dessas limitações, a versão gratuita mostrou-se robusta e completa para desenvolver e demonstrar a arquitetura de Engenharia de Dados proposta.

---

Espero que este relatório detalhado seja útil!
