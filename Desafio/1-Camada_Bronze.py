# Databricks notebook source
# MAGIC %md
# MAGIC # Camada Bronze
# MAGIC Essa etapa é responsável por ingerir os dados do dataset público de incidentes de incêndio de San Francisco
# MAGIC
# MAGIC ## Objetivos:
# MAGIC - Ingerir os dados do CSV armazenado a cada execução, atualizando a tabela.
# MAGIC
# MAGIC - Estruturei a tabela no formato Delta, registrando no Unity Catalog.
# MAGIC
# MAGIC - A ingestão dos dados é incremental, o comando *COPY INTO* lê o volume com os arquivos CSV e adiciona apenas aqueles que ainda não foram carregados na tabela SQL.
# MAGIC
# MAGIC ## Decisões Técnicas:
# MAGIC - Criei a tabela pela interface do Databricks, definindo como Delta Lake.
# MAGIC
# MAGIC - O requisito **"Ingerir diariamente o dataset"** me gerou uma dúvida. Identifiquei que o site do dataset atualiza o CSV diariamante, nesse caso, acredito que uma solução de web scraping seria mais eficaz, baixando os novos dados diariamante para a ingestão. Porém, lendo a documentação da versão gratuita do databricks, descobri que o acesso externo à Internet é restrito, nesse caso optei por adicionar manualmente os arquivos CSV no volume "arquivos_brutos", simulando a ingestão diaria ao executar as camadas após a inclusão de um novo CSV.
# MAGIC
# MAGIC - Ao criar a tabela SQL para ingerir os dados do CSV, tive problemas ao usar o comando *COPY INTO* para mesclar os dados utulizando a opção **"inferSchema"** como *True*, há algum conflito nos tipos de dados inferidos pela plataforma para o CSV e a tabela SQL. Nesse caso declarei a opção como *False*, e adotei inicialmente o tipo de dado String como padrão para todos os valores, tratando-os posteriormente na camada **Silver**.
# MAGIC
# MAGIC

# COMMAND ----------

# Configuração Inicial

## Importando bibliotecas necessárias
from pyspark.sql.functions import current_timestamp
import os

csv_input = "/Volumes/workspace/default/arquivos_brutos/" # pasta com os arquivos CSV no Unity Catalog
tabela_bronze_caminho = "`workspace`.`default`.`fire_incidents_bronze`" # Caminho da tabela no Unity Catalog
tabela_bronze_nome = "fire_incidents_bronze" # Nome para referênciar no PySpark

print(f"Diretório de entrada (Volume do Unity Catalog): {csv_input}")
print(f"Nome completo da tabela Bronze existente: {tabela_bronze_caminho}")
print(f"Nome simples da tabela Bronze existente: {tabela_bronze_nome}")

# COMMAND ----------

# Verificando o Volume

print("O comando 'COPY INTO' no próximo passo lerá o diretório do Volume para novos arquivos.")
print(f"\n Verificando o conteúdo do Volume de entrada: {csv_input}")
try:
    # dbutils.fs.ls lista o conteúdo de um caminho no sistema de arquivos do Databricks
    volume_contents = dbutils.fs.ls(csv_input)
    if not volume_contents:
        print(f"AVISO: O Volume '{csv_input}' está vazio.")
    else:
        print("Conteúdo encontrado no Volume:")
        for item in volume_contents:
            print(f"- {item.path}")
        print("\nSUCESSO: Arquivos CSV encontrados no Volume. Pronto para a ingestão incremental.")
except Exception as e:
    print(f"\nERRO: Falha ao acessar o Volume '{csv_input}'.")
    print(f"Detalhes do erro: {e}")
    print("Por favor, verifique se o caminho do Volume está correto e se seus arquivos foram carregados para lá.")
    dbutils.notebook.exit("Falha na Etapa Bronze: Não foi possível acessar o Volume de entrada.")

# COMMAND ----------

# Ingestão de Dados

print(f"Iniciando operação COPY INTO para {tabela_bronze_caminho} a partir do Volume {csv_input}")
spark.sql(f"""
  COPY INTO {tabela_bronze_caminho}
  FROM '{csv_input}'
  FILEFORMAT = CSV
  FORMAT_OPTIONS (
    'header' = 'true',           -- O CSV tem cabeçalho
    'inferSchema' = 'false',     -- Não inferi o schema do CSV
    'enforceSchema' = 'false'    
  )
  COPY_OPTIONS (
    'mergeSchema' = 'false'      -- Não fundi o schema, a tabela já tem o schema String
  )
""")

print(f"COPY INTO concluído para a tabela Delta '{tabela_bronze_caminho}'.")
print("Dados brutos ingeridos de forma robusta na Camada Bronze com todas as colunas como String.")

# COMMAND ----------

# Verificando os dados na tabela bronze usando PySpark DataFrame API
print("\n--- Exemplo dos 5 primeiros registros na camada bronze (via PySpark DataFrame): ---")
bronze_df = spark.table(tabela_bronze_nome) 
display(bronze_df.limit(5))

print("\n--- Esquema REAL da tabela bronze (deve ser todo STRING): ---")
bronze_df.printSchema()

print("\n--- Contagem total de registros na camada bronze: ---")
final_record_count = bronze_df.count()
print(f"Total de registros na camada Bronze: {final_record_count}")

print("\n--- Etapa 1 (Camada Bronze) concluída e verificada com sucesso! ---")
print("A tabela está pronta para a próxima etapa (Camada Silver).")
print("Para simular uma nova carga diária, adicione mais arquivos CSV ao Volume de entrada")
print(f"('{csv_input}') e execute o notebook novamente.")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verificação adicional da tabela bronze via SQL (para confirmação)
# MAGIC SELECT *
# MAGIC FROM `workspace`.`default`.`fire_incidents_bronze`
# MAGIC LIMIT 5;
