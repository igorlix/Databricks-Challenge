# Databricks notebook source
# MAGIC %md
# MAGIC # Camada Silver
# MAGIC Essa etapa é responsável por limpar, transformar e padronizar os dados recebidos pela etapa bronze.
# MAGIC
# MAGIC ## Objetivos:
# MAGIC - Efetuei a leitura dos dados da tabela Delta da Camada Bronze.
# MAGIC
# MAGIC - Converti os tipos de dados de *String* para *Bigint*, *Double*, *Date*, etc.
# MAGIC
# MAGIC - Padronizei os valores ausentes (como "NA", "None", "N None present, etc") para *NULL*.
# MAGIC
# MAGIC - Dedupliquei os registros.
# MAGIC
# MAGIC - Ingeri os dados na tabela Delta da Camada Silver de forma incremental, usando *MERGE INTO*
# MAGIC
# MAGIC - A tabela Silver criada está no formato Delta e é gerenciada pelo Unity Catalog.
# MAGIC
# MAGIC - Ao final comparo a tabela silver e bronze para demonstrar o tratamentro dos valores e deduplicação.
# MAGIC
# MAGIC ## Decisões Técnicas:
# MAGIC - Novamente tentei me ater ao paralelismo utilizando o *Spark*.
# MAGIC
# MAGIC - Adotei ID como chave primária da tabela SQL.
# MAGIC
# MAGIC - Para tratar os tipos de dados utilizei PySpark e criei um dicionário com o nome das colunas e seus respectivos tipos de dados. Para identificar os tipos certos analisei o CSV.
# MAGIC
# MAGIC - Esse dataset possui muitos valores nulos de formatos diferentes, seja um campo vazio, "NA", "None", "N - None", etc. Nesse caso analisei o CSV, e busquei as ocorrências na tabela, mesmo assim, acredito que há uma forma mais otimizada de filtrar esses nulos. 
# MAGIC
# MAGIC - Para a deduplicação, filtrei pela chave primário ID, e em seguida utilizei o comando *MERGE INTO* para mesclar os dados apenas se o ID for único.
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# Importando bibliotecas necessárias
from pyspark.sql.functions import col, to_date, lit, coalesce, expr, current_timestamp
from pyspark.sql.types import LongType, DoubleType, StringType, DateType, TimestampType
from pyspark.sql.window import Window 

# Tabela Bronze fonte
tabela_bronze_caminho = "`workspace`.`default`.`fire_incidents_bronze`" # Caminho no Unity Catalog
tabela_bronze_nome = "fire_incidents_bronze"

# Tabela Silver destino
tabela_silver_caminho = "`workspace`.`default`.`fire_incidents_silver`" # Caminho no Unity Catalog
tabela_silver_nome = "fire_incidents_silver"

print(f"Fonte de dados (Camada Bronze): {tabela_bronze_caminho}")
print(f"Destino de dados (Camada Silver): {tabela_silver_caminho}")

# COMMAND ----------

#Lendo os dados da Camada Bronze
print(f"\nLendo dados da Camada Bronze ({tabela_bronze_caminho})...")

bronze_df = spark.table(tabela_bronze_nome)

print("Schema da Camada Bronze (esperado todo String):")
bronze_df.printSchema()
print(f"Total de registros lidos da Camada Bronze: {bronze_df.count()}")

# COMMAND ----------

# Aplicando transformações e limpando os dados

from pyspark.sql.functions import col, to_date, lit, coalesce, when, trim, expr
from pyspark.sql.types import LongType, DoubleType, StringType, DateType

# Dicionário que mapeia o nome da coluna para o tipo PySpark desejado.
column_type_mapping = {
    "ID": LongType(), 
    "Incident Number": LongType(), 
    "Exposure Number": LongType(), 
    "Call Number": LongType(), 
    "Suppression Units": LongType(), 
    "Suppression Personnel": LongType(), 
    "EMS Units": LongType(), 
    "EMS Personnel": LongType(), 
    "Other Units": LongType(), 
    "Other Personnel": LongType(), 
    "Fire Fatalities": LongType(), 
    "Fire Injuries": LongType(), 
    "Civilian Fatalities": LongType(), 
    "Civilian Injuries": LongType(), 
    "Number of Alarms": LongType(), 
    "Floor of Fire Origin": LongType(), 
    "Number of floors with minimum damage": LongType(), 
    "Number of floors with significant damage": LongType(), 
    "Number of floors with heavy damage": LongType(), 
    "Number of floors with extreme damage": LongType(), 
    "Number of Sprinkler Heads Operating": LongType(), 
    "Supervisor District": LongType(), 
    "Estimated Property Loss": DoubleType(), 
    "Estimated Contents Loss": DoubleType(), 
    "Incident Date": DateType()
}

# Tratando variações de nulo
def clean_and_standardize_string(column_name):
    column_expr = col(column_name) 
    return when(
        (trim(column_expr) == "NA") |
        (trim(column_expr) == "None") |
        (trim(column_expr) == "N None") |
        (trim(column_expr) == "N - None") |
        (trim(column_expr) == "N None present") |  
        (trim(column_expr) == "N -Not present") | 
        (trim(column_expr) == "null") | 
        (trim(column_expr) == "UU - Undetermined") |
        (trim(column_expr) == "") | 
        column_expr.isNull(),      
        lit(None)                  
    ).otherwise(column_expr)       
select_expressions = []

for col_info in bronze_df.schema.fields: # Itera sobre os campos do schema da Bronze
    col_name = col_info.name # Nome da coluna, ex: "Incident Number"
    quoted_col_name = f"`{col_name}`" if " " in col_name else col_name # Adiciona backticks se necessário

    # Aplica a limpeza de strings primeiro a TODAS as colunas
    cleaned_col_expr = clean_and_standardize_string(quoted_col_name)

    # Verifica se a coluna tem um mapeamento de tipo específico
    if col_name in column_type_mapping:
        target_type = column_type_mapping[col_name]
        
        if target_type == DateType():
            cast_expr = to_date(cleaned_col_expr, "yyyy/MM/dd") # Ajuste o formato da data
        elif target_type in [LongType(), DoubleType()]:
            # Para numéricos, usa TRY_CAST.
            cast_expr = expr(f"TRY_CAST({quoted_col_name} AS {target_type.simpleString().upper()})")
            if col_name == "ID":
                cast_expr = coalesce(cast_expr, lit(-1)) # Valor padrão para ID inválido
        else: # Colunas mapeadas como StringType, apenas garantem o cast para String
            cast_expr = cleaned_col_expr.cast(StringType()) # Garante que o tipo seja String
    else: # Se a coluna NÃO está no column_type_mapping, ela é tratada como String
        cast_expr = cleaned_col_expr.cast(StringType()) # Garante que o tipo seja String após a limpeza

    select_expressions.append(cast_expr.alias(col_name)) 
    
# Criando o DataFrame transformado
silver_df = bronze_df.select(*select_expressions)

# VAlidando a chave primária 'ID'
silver_df = silver_df.filter(col("ID").isNotNull()) 

print("\nSchema do DataFrame transformado (silver_df - após o tratamento otimizado):")
silver_df.printSchema()
print(f"Total de registros no 'silver_df': {silver_df.count()}")

# COMMAND ----------

# Ingestão Incremental e Deduplicação

print("\nRealizando deduplicação e ingestão incremental na Camada Silver...")

# 1. Adicionar o timestamp de carregamento para os dados
silver_df_final = silver_df.withColumn("silver_loaded_at", current_timestamp())

# 2. Deduplicação por ID (chave primária) - Antes do MERGE
# Isso remove duplicatas dentro do lote de dados atual (se o mesmo ID aparecer várias vezes no bronze).
print(f"Registros antes da deduplicação (pelo ID no lote atual): {silver_df_final.count()}")
silver_df_final = silver_df_final.dropDuplicates(["ID"])
print(f"Registros após a deduplicação (pelo ID no lote atual): {silver_df_final.count()}")

# 3. Preparar os dados para o MERGE INTO.
silver_df_final.createOrReplaceTempView("fire_incidents_silver_staging")
print(f"Total de registros no staging DataFrame para o MERGE: {silver_df_final.count()}") 

# 4. Executando o MERGE INTO para a Camada Silver
# Excluir a tabela Silver antes de recriar
# Isso garante que a tabela seja sempre recriada com o schema correto do silver_df_final
print(f"\nVerificando e recriando a tabela Silver '{tabela_silver_caminho}' para garantir o schema correto...")
spark.sql(f"DROP TABLE IF EXISTS {tabela_silver_caminho}") 
# Usamos .limit(0) para criar a tabela APENAS com o schema na primeira execução.
# 'delta.columnMapping.mode' é necessário para nomes de colunas com espaços.
silver_df_final.limit(0).write \
                      .format("delta") \
                      .mode("overwrite") \
                      .option("overwriteSchema", "true") \
                      .option("delta.columnMapping.mode", "name") \
                      .saveAsTable(tabela_silver_nome) # saveAsTable registra no Unity Catalog 

print(f"Tabela Silver '{tabela_silver_caminho}' criada/recriada com sucesso com o esquema correto (vazia de dados neste passo).")
# Contagem de registros na Camada Silver ANTES do MERGE
silver_target_count_before_merge = spark.table(tabela_silver_nome).count()
print(f"Total de registros na Camada Silver ANTES do MERGE: {silver_target_count_before_merge}")


# 5. Executar o MERGE INTO para inserir/atualizar dados. 
print(f"\nIniciando operação MERGE INTO para a tabela Delta '{tabela_silver_caminho}'...")
df_column_names_raw = silver_df_final.columns
update_set_clauses = []
for col_name in df_column_names_raw:
    quoted_col_name = f"`{col_name}`" # Adiciona backticks para o SQL
    update_set_clauses.append(f"target.{quoted_col_name} = source.{quoted_col_name}")
update_set_sql_string = ",\n    ".join(update_set_clauses) 
# Strings para INSERT (colunas)
insert_columns_sql_string = ", ".join([f"`{col_name}`" for col_name in df_column_names_raw])
# Strings para INSERT (valores)
insert_values_sql_string = ", ".join([f"source.`{col_name}`" for col_name in df_column_names_raw])
spark.sql(f"""
  MERGE INTO {tabela_silver_caminho} AS target
  USING fire_incidents_silver_staging AS source
  ON target.ID = source.ID 
  WHEN MATCHED THEN UPDATE SET
    {update_set_sql_string}
  WHEN NOT MATCHED THEN INSERT (
    {insert_columns_sql_string}
  )
  VALUES (
    {insert_values_sql_string}
  )
""")
print(f"MERGE INTO concluído para a tabela Delta '{tabela_silver_caminho}'.")
print("A Camada Silver foi atualizada com dados limpos e deduplicados.")

# Contagem de registros na Silver depois do MERGE
silver_target_count_after_merge = spark.table(tabela_silver_nome).count()
print(f"Total de registros na Camada Silver DEPOIS do MERGE: {silver_target_count_after_merge}")

# COMMAND ----------

# Importando bibliotecas necessárias
from pyspark.sql.functions import col, to_date, lit, coalesce, expr, current_timestamp
from pyspark.sql.types import LongType, DoubleType, StringType, DateType, TimestampType
from pyspark.sql.window import Window # Necessário para deduplicação avançada, se aplicável

# Tabela Bronze fonte
tabela_bronze_caminho = "`workspace`.`default`.`fire_incidents_bronze`" # Caminho no Unity Catalog
tabela_bronze_nome = "fire_incidents_bronze"

# Tabela Silver destino
tabela_silver_caminho = "`workspace`.`default`.`fire_incidents_silver`" # Caminho no Unity Catalog
tabela_silver_nome = "fire_incidents_silver"

print(f"Fonte de dados (Camada Bronze): {tabela_bronze_caminho}")
print(f"Destino de dados (Camada Silver): {tabela_silver_caminho}")


# Verificando e Comparação Silver e Bronze
print("\nVerificando 10 registros da Camada Silver e comparando com a Bronze")

final_silver_df = spark.table(tabela_silver_nome)

# 1. Obtendo os IDs dos primeiros 10 registros da Camada Silver
silver_sample_ids_df = final_silver_df.select("ID").limit(10)
silver_sample_ids = [row.ID for row in silver_sample_ids_df.collect()]

print(f"\nIDs de exemplo da Camada Silver para comparação: {silver_sample_ids}")

# 2. Filtrando os mesmos registros na Camada Silver (para mostrar os dados transformados)
print("\n--- Registros selecionados na Camada Silver")
silver_records_for_comparison = final_silver_df.filter(col("ID").isin(silver_sample_ids)).orderBy("ID")
display(silver_records_for_comparison)

# 3. Filtrando os mesmos registros na Camada Bronze (para mostrar os dados originais)
print("\n--- Registros correspondentes na Camada Bronze (dados brutos): ---")
bronze_df_raw = spark.table(tabela_bronze_nome)

# Convertendo a coluna 'ID' da Bronze para BIGINT ANTES de filtrar.
# Isso garante que a comparação seja feita entre tipos numéricos.
bronze_records_for_comparison = bronze_df_raw.filter(
    expr("TRY_CAST(ID AS BIGINT)").isin(silver_sample_ids)
).orderBy("ID")

display(bronze_records_for_comparison)

print("Observe a diferença nos tipos de dados Silver e Bronze, e o tratamento de nulos/formato.")
