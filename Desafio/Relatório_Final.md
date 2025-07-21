
# Relatório Final - Databricks Challenge – Data Engineer

Este relatório compila as informações essenciais, decisões técnicas, duvidas e um vídeo do funcionamento da solução.

## 1. Arquitetura:

Adotei uma arquitetura de Medallion Lakehouse.

* **Camada Bronze**: Ingestão inicial dos dados.
* **Camada Silver**: Limpeza, transformação e padronização dos dados.
* **Camada Gold**: Agregação e otimização dos dados para consumo.


## 2. Implementação por Camada

### 2.1. Camada Bronze 

#### **Decisões Técnicas**:
- Tentei me ater ao paralelismo utilizando o *Spark*.

- Criei a tabela pela interface do Databricks, definindo como Delta Lake.

- O requisito **"Ingerir diariamente o dataset"** me gerou uma dúvida. Identifiquei que o site do dataset atualiza o CSV diariamante, nesse caso, acredito que uma solução de web scraping seria mais eficaz, baixando os novos dados diariamante para a ingestão. Porém, lendo a documentação da versão gratuita do databricks, descobri que o acesso externo à Internet é restrito, nesse caso optei por adicionar manualmente os arquivos CSV no volume "arquivos_brutos", simulando a ingestão diaria ao executar as camadas após a inclusão de um novo CSV.

- Ao criar a tabela SQL para ingerir os dados do CSV, tive problemas ao usar o comando *COPY INTO* para mesclar os dados utulizando a opção **"inferSchema"** como *True*, há algum conflito nos tipos de dados inferidos pela plataforma para o CSV e a tabela SQL. Nesse caso declarei a opção como *False*, e adotei inicialmente o tipo de dado String como padrão para todos os valores, tratando-os posteriormente na camada **Silver**.
 

### 2.2. Camada Silver 

#### **Decisões Técnicas**:
- Novamente tentei me ater ao paralelismo utilizando o *Spark*.

- Adotei ID como chave primária da tabela SQL.

- Para tratar os tipos de dados utilizei PySpark e criei um dicionário com o nome das colunas e seus respectivos tipos de dados. Para identificar os tipos certos analisei o CSV.

- Esse dataset possui muitos valores nulos de formatos diferentes, seja um campo vazio, "NA", "None", "N - None", etc. Nesse caso analisei o CSV, e busquei as ocorrências na tabela, mesmo assim, acredito que há uma forma mais otimizada de filtrar esses nulos. 

- Para a deduplicação, filtrei pela chave primário ID, e em seguida utilizei o comando *MERGE INTO* para mesclar os dados apenas se o ID for único.


### 2.3. Camada Gold (Notebook: `3-Camada_Gold.ipynb`)

#### **Decisões Técnicas:**
- Novamente tentei me ater ao paralelismo, por isso as operações como _GroupBy_ (agrupamento) e _Agg_ (agregação) são executadas de forma paralela pelo Spark.

-  As agregações são calculadas e armazenadas previamente em tabelas separadas.

- Optei por utilizar _"overwrite"_ na escrita para reescrever os dados com os valores mais recentes das camadas anteriores.

### 2.4. Relatório Exploratório (Notebook: `4-Relatório_Exploratório.ipynb`)

Este notebook demonstra o consumo dos dados da Camada Gold.

#### **Objetivos**:
 * Utilizar os dados processados e agregados na Camada Gold para consultas SQL.
#### **Consultas Realizadas**:
 * O número total de incidentes ao longo do tempo (por ano e mês). 
 * Os distritos com maior e menor número de incidentes. 
 * O total de perdas estimadas (propriedade e conteúdo) por batalhão. 
 * Os tipos de incidentes mais comuns.


## 3. Jobs

Para simular o processo de ingestão e processamento diário, um **Job do Databricks** foi configurado. Este Job executa os notebooks das camadas Bronze, Silver e Gold em **sequência** a cada 24 horas.
<img width="1697" height="820" alt="Captura de tela 2025-07-20 214654" src="https://github.com/user-attachments/assets/98df7a8b-c02a-4cb6-b4cf-315ae6ba96a7" />
<img width="1706" height="816" alt="Captura de tela 2025-07-20 214701" src="https://github.com/user-attachments/assets/38d1f8e2-e733-4c02-b2dd-28a3d9d11aff" />

## 4. Desafios da Versão Gratuita


